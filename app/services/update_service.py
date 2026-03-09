import asyncio
import json
import logging
import os
import platform
import re
import shutil
import stat
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as url_error
from urllib import request as url_request

from app.config import Settings
from app.db import Database
from app.services.app_restart_service import AppRestartService
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class GitHubReleaseAsset:
    name: str
    browser_download_url: str
    size: int


@dataclass(slots=True)
class GitHubReleaseInfo:
    tag_name: str
    html_url: str
    body: str
    published_at: str
    assets: list[GitHubReleaseAsset]


class UpdateService:
    KEY_LAST_RELEASE_TAG = "update.last_release_tag"
    KEY_LAST_CHECK_AT = "update.last_check_at"
    KEY_LAST_ERROR = "update.last_error"
    KEY_LAST_NOTIFIED_TAG = "update.last_notified_tag"
    KEY_LAST_NOTIFIED_AT = "update.last_notified_at"
    KEY_LAST_TRIGGER_AT = "update.last_trigger_at"
    KEY_LAST_APPLIED_TAG = "update.last_applied_tag"

    def __init__(
        self,
        db: Database,
        settings: Settings,
        telegram: TelegramManager,
        restart_service: AppRestartService | None = None,
    ):
        self.db = db
        self.settings = settings
        self.telegram = telegram
        self.restart_service = restart_service or AppRestartService()
        self._status_cache: dict[str, object] | None = None
        self._status_cache_at = 0.0
        self._status_lock = asyncio.Lock()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    @staticmethod
    def _normalize_tag(value: str | None) -> str:
        return str(value or "").strip().lower().lstrip("v")

    @classmethod
    def _parse_version_tuple(cls, value: str | None) -> tuple[int, ...] | None:
        normalized = cls._normalize_tag(value)
        if not normalized or not normalized[:1].isdigit():
            return None
        parts = re.findall(r"\d+", normalized)
        if not parts:
            return None
        return tuple(int(part) for part in parts[:4])

    @classmethod
    def _has_newer_version(cls, current_version: str, latest_tag: str) -> bool:
        current_tuple = cls._parse_version_tuple(current_version)
        latest_tuple = cls._parse_version_tuple(latest_tag)
        if current_tuple is not None and latest_tuple is not None:
            width = max(len(current_tuple), len(latest_tuple))
            current_padded = current_tuple + (0,) * (width - len(current_tuple))
            latest_padded = latest_tuple + (0,) * (width - len(latest_tuple))
            return latest_padded > current_padded
        return cls._normalize_tag(current_version) != cls._normalize_tag(latest_tag)

    @staticmethod
    def _is_running_in_docker() -> bool:
        if os.path.exists("/.dockerenv"):
            return True
        for path in ("/proc/1/cgroup", "/proc/self/cgroup"):
            try:
                text = Path(path).read_text(encoding="utf-8")
            except Exception:
                continue
            lowered = text.lower()
            if any(token in lowered for token in ("docker", "containerd", "kubepods")):
                return True
        return False

    def _status_cache_ttl_seconds(self) -> int:
        return min(max(int(self.settings.update_check_interval_seconds), 30), 300)

    def _build_headers(self, *, accept: str) -> dict[str, str]:
        headers = {
            "Accept": accept,
            "User-Agent": "TelegramAutoClone",
        }
        token = (self.settings.update_github_token or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _fetch_latest_release_sync(self, repository: str, timeout_seconds: int) -> GitHubReleaseInfo | None:
        url = f"https://api.github.com/repos/{repository}/releases/latest"
        req = url_request.Request(
            url,
            headers=self._build_headers(accept="application/vnd.github+json"),
            method="GET",
        )
        try:
            with url_request.urlopen(req, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except url_error.HTTPError as exc:
            if int(exc.code) == 404:
                return None
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"GitHub Release 查询失败(status={exc.code}): {body[:200]}") from exc
        except Exception as exc:
            raise RuntimeError(f"GitHub Release 查询失败: {exc}") from exc

        assets: list[GitHubReleaseAsset] = []
        for item in payload.get("assets", []):
            assets.append(
                GitHubReleaseAsset(
                    name=str(item.get("name") or "").strip(),
                    browser_download_url=str(item.get("browser_download_url") or "").strip(),
                    size=int(item.get("size") or 0),
                )
            )

        return GitHubReleaseInfo(
            tag_name=str(payload.get("tag_name") or "").strip(),
            html_url=str(payload.get("html_url") or "").strip(),
            body=str(payload.get("body") or "").strip(),
            published_at=str(payload.get("published_at") or "").strip(),
            assets=assets,
        )

    def _download_asset_sync(self, asset_url: str, destination: Path, timeout_seconds: int) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        req = url_request.Request(
            asset_url,
            headers=self._build_headers(accept="application/octet-stream"),
            method="GET",
        )
        try:
            with url_request.urlopen(req, timeout=timeout_seconds) as response, destination.open("wb") as file_obj:
                shutil.copyfileobj(response, file_obj)
        except Exception as exc:
            raise RuntimeError(f"下载更新包失败: {exc}") from exc

    def _resolve_asset_arch(self) -> tuple[str | None, str]:
        machine = platform.machine().lower()
        if machine in {"x86_64", "amd64"}:
            return "linux-x64", machine
        if machine in {"aarch64", "arm64"}:
            return "linux-arm64", machine
        return None, machine

    def _pick_asset(self, assets: list[GitHubReleaseAsset]) -> tuple[GitHubReleaseAsset | None, str]:
        arch_label, machine = self._resolve_asset_arch()
        if not arch_label:
            return None, machine
        prefix = (self.settings.self_update_asset_prefix or "telegram-auto-clone").strip().lower()
        exact_name = f"{prefix}-{arch_label}.zip"
        for asset in assets:
            if asset.name.strip().lower() == exact_name:
                return asset, arch_label
        for asset in assets:
            normalized = asset.name.strip().lower()
            if normalized.endswith(".zip") and arch_label in normalized and prefix in normalized:
                return asset, arch_label
        return None, arch_label

    def _resolve_work_paths(self) -> tuple[Path, Path, Path, Path]:
        work_root = Path(self.settings.self_update_work_dir).expanduser()
        if not work_root.is_absolute():
            work_root = Path.cwd() / work_root
        workspace_dir = work_root / "workspace"
        current_dir = work_root / "current"
        backup_dir = work_root / "previous"
        return work_root, workspace_dir, current_dir, backup_dir

    @staticmethod
    def _unwrap_stage_dir(stage_dir: Path) -> Path:
        children = [item for item in stage_dir.iterdir() if item.name != "__MACOSX"]
        if len(children) == 1 and children[0].is_dir():
            return children[0]
        return stage_dir

    @staticmethod
    def _promote_current_directory(source_dir: Path, current_dir: Path, backup_dir: Path) -> None:
        if backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)

        moved_current = False
        try:
            if current_dir.exists():
                current_dir.rename(backup_dir)
                moved_current = True
            source_dir.rename(current_dir)
        except Exception:
            if moved_current and backup_dir.exists() and not current_dir.exists():
                backup_dir.rename(current_dir)
            raise

    def _apply_release_sync(self, *, latest_tag: str, asset_name: str, asset_url: str) -> None:
        _work_root, workspace_dir, current_dir, backup_dir = self._resolve_work_paths()
        workspace_dir.mkdir(parents=True, exist_ok=True)

        package_path = workspace_dir / f"pkg-{int(time.time())}-{asset_name}"
        stage_dir = workspace_dir / f"stage-{int(time.time())}"
        promote_source = stage_dir

        try:
            self._download_asset_sync(asset_url, package_path, int(self.settings.update_http_timeout_seconds))
            stage_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(package_path, "r") as archive:
                archive.extractall(stage_dir)

            promote_source = self._unwrap_stage_dir(stage_dir)
            executable_name = self.settings.self_update_executable_name.strip() or "telegram-auto-clone"
            executable_path = promote_source / executable_name
            if not executable_path.exists():
                raise RuntimeError(f"更新包结构无效：缺少 {executable_name}")

            executable_path.chmod(executable_path.stat().st_mode | stat.S_IEXEC)
            (promote_source / "VERSION").write_text(f"{latest_tag}\n", encoding="utf-8")
            self._promote_current_directory(promote_source, current_dir, backup_dir)
        except zipfile.BadZipFile as exc:
            raise RuntimeError(f"更新包不是有效 zip 文件: {exc}") from exc
        finally:
            if package_path.exists():
                package_path.unlink(missing_ok=True)
            if stage_dir.exists():
                shutil.rmtree(stage_dir, ignore_errors=True)

    async def _check_status_core(self, *, send_notify: bool) -> dict[str, object]:
        now = self._now()
        current_version = (self.settings.app_version or "dev").strip() or "dev"
        is_docker = self._is_running_in_docker()
        repository = (self.settings.update_repository or "").strip()

        base: dict[str, object] = {
            "ok": True,
            "enabled": bool(self.settings.self_update_enabled),
            "current_version": current_version,
            "repository": repository,
            "is_docker": is_docker,
            "docker_only": bool(self.settings.self_update_docker_only),
            "restart_requested": bool(self.restart_service.restart_requested),
            "last_error": await self.db.get_setting(self.KEY_LAST_ERROR),
            "last_check_at": await self.db.get_setting(self.KEY_LAST_CHECK_AT),
            "last_notified_at": await self.db.get_setting(self.KEY_LAST_NOTIFIED_AT),
            "last_trigger_at": await self.db.get_setting(self.KEY_LAST_TRIGGER_AT),
            "last_applied_tag": await self.db.get_setting(self.KEY_LAST_APPLIED_TAG),
        }

        if not self.settings.self_update_enabled:
            await self.db.set_setting(self.KEY_LAST_ERROR, "")
            await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)
            base.update(
                {
                    "has_update": False,
                    "update_available": False,
                    "can_apply": False,
                    "blocked_reason": "自动更新功能未启用",
                    "message": "自动更新功能未启用",
                    "last_check_at": now,
                    "last_error": "",
                }
            )
            return base

        if not repository or "/" not in repository:
            error = "UPDATE_REPOSITORY 配置无效，应为 owner/repo"
            await self.db.set_setting(self.KEY_LAST_ERROR, error)
            await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)
            base.update(
                {
                    "ok": False,
                    "error": error,
                    "has_update": False,
                    "update_available": False,
                    "can_apply": False,
                    "last_check_at": now,
                    "last_error": error,
                }
            )
            return base

        try:
            release = await asyncio.to_thread(
                self._fetch_latest_release_sync,
                repository,
                int(self.settings.update_http_timeout_seconds),
            )
        except Exception as exc:
            error = f"更新检查失败: {exc}"
            await self.db.set_setting(self.KEY_LAST_ERROR, error[:500])
            await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)
            base.update(
                {
                    "ok": False,
                    "error": error,
                    "has_update": False,
                    "update_available": False,
                    "can_apply": False,
                    "last_check_at": now,
                    "last_error": error[:500],
                }
            )
            return base

        if release is None or not release.tag_name:
            error = "仓库未找到可用 Release，无法执行面板内更新"
            await self.db.set_setting(self.KEY_LAST_ERROR, error)
            await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)
            base.update(
                {
                    "ok": False,
                    "error": error,
                    "has_update": False,
                    "update_available": False,
                    "can_apply": False,
                    "last_check_at": now,
                    "last_error": error,
                }
            )
            return base

        asset, arch_label = self._pick_asset(release.assets)
        has_update = self._has_newer_version(current_version, release.tag_name)
        message = ""
        blocked_reason = ""
        if not has_update:
            message = "当前已是最新版本"
            blocked_reason = message
        elif self.settings.self_update_docker_only and not is_docker:
            blocked_reason = "当前仅支持 Docker 容器内执行自更新"
            message = blocked_reason
        elif asset is None:
            blocked_reason = f"未找到匹配当前架构的更新包（arch={arch_label}）"
            message = blocked_reason

        can_apply = has_update and not blocked_reason

        notified = False
        if send_notify and has_update and self.settings.update_notify_enabled:
            last_notified_tag = await self.db.get_setting(self.KEY_LAST_NOTIFIED_TAG)
            if last_notified_tag != release.tag_name:
                await self.telegram.send_notification(
                    "🆕 检测到新版本\n"
                    f"当前版本: {current_version}\n"
                    f"最新版本: {release.tag_name}\n"
                    f"仓库: {repository}\n"
                    "请在面板点击“下载更新并重启”执行升级。"
                )
                await self.db.set_setting(self.KEY_LAST_NOTIFIED_TAG, release.tag_name)
                await self.db.set_setting(self.KEY_LAST_NOTIFIED_AT, now)
                notified = True

        await self.db.set_setting(self.KEY_LAST_RELEASE_TAG, release.tag_name)
        await self.db.set_setting(self.KEY_LAST_ERROR, "")
        await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)

        base.update(
            {
                "has_update": has_update,
                "update_available": has_update,
                "can_apply": can_apply,
                "blocked_reason": blocked_reason or None,
                "message": message or None,
                "latest_tag": release.tag_name,
                "release_url": release.html_url,
                "published_at": release.published_at,
                "notes": release.body,
                "asset_name": asset.name if asset else None,
                "asset_download_url": asset.browser_download_url if asset else None,
                "asset_size_bytes": asset.size if asset else None,
                "arch": arch_label,
                "notified": notified,
                "notify_enabled": bool(self.settings.update_notify_enabled),
                "last_check_at": now,
                "last_error": "",
            }
        )
        return base

    async def _load_status(self, *, force_refresh: bool, send_notify: bool) -> dict[str, object]:
        ttl_seconds = self._status_cache_ttl_seconds()
        if not force_refresh and self._status_cache is not None and (time.monotonic() - self._status_cache_at) < ttl_seconds:
            return dict(self._status_cache)

        async with self._status_lock:
            if not force_refresh and self._status_cache is not None and (time.monotonic() - self._status_cache_at) < ttl_seconds:
                return dict(self._status_cache)
            status = await self._check_status_core(send_notify=send_notify)
            self._status_cache = dict(status)
            self._status_cache_at = time.monotonic()
            return dict(status)

    def _invalidate_status_cache(self) -> None:
        self._status_cache = None
        self._status_cache_at = 0.0

    async def get_status(self) -> dict[str, object]:
        return await self._load_status(force_refresh=False, send_notify=False)

    async def check_and_notify(self) -> dict[str, object]:
        return await self._load_status(force_refresh=True, send_notify=True)

    async def confirm_and_trigger_update(self) -> dict[str, object]:
        running_jobs = await self.db.count_running_recovery_jobs()
        if running_jobs > 0:
            raise RuntimeError(f"当前有 {running_jobs} 个恢复任务正在执行，请等待完成后再更新")

        status = await self._load_status(force_refresh=True, send_notify=False)
        if not bool(status.get("ok")):
            raise RuntimeError(str(status.get("error") or "更新检查失败"))

        if not bool(status.get("has_update")):
            return {
                "ok": True,
                "triggered": False,
                "current_version": status.get("current_version"),
                "latest_tag": status.get("latest_tag") or status.get("current_version"),
                "message": status.get("message") or "当前已是最新版本",
            }

        if not bool(status.get("can_apply")):
            raise RuntimeError(str(status.get("blocked_reason") or "当前环境不允许执行自更新"))

        latest_tag = str(status.get("latest_tag") or "").strip()
        asset_name = str(status.get("asset_name") or "").strip()
        asset_url = str(status.get("asset_download_url") or "").strip()
        if not latest_tag or not asset_name or not asset_url:
            raise RuntimeError("缺少可用更新包信息，无法执行更新")

        await asyncio.to_thread(
            self._apply_release_sync,
            latest_tag=latest_tag,
            asset_name=asset_name,
            asset_url=asset_url,
        )

        now = self._now()
        await self.db.set_setting(self.KEY_LAST_APPLIED_TAG, latest_tag)
        await self.db.set_setting(self.KEY_LAST_TRIGGER_AT, now)
        await self.db.set_setting(self.KEY_LAST_ERROR, "")
        self._invalidate_status_cache()
        self.restart_service.request_restart(
            int(self.settings.self_update_restart_delay_seconds),
            f"self-update {latest_tag}",
        )

        return {
            "ok": True,
            "triggered": True,
            "current_version": status.get("current_version"),
            "latest_tag": latest_tag,
            "asset_name": asset_name,
            "restart_requested": bool(self.restart_service.restart_requested),
            "confirmed_at": now,
        }