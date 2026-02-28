import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib import error as url_error
from urllib import parse as url_parse
from urllib import request as url_request

from app.config import Settings
from app.db import Database
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


@dataclass
class ParsedImageRef:
    registry: str
    repository: str
    tag: str
    full: str


class UpdateService:
    KEY_CONFIRMED_DIGEST = "update.confirmed_digest"
    KEY_LAST_CHECK_AT = "update.last_check_at"
    KEY_LAST_ERROR = "update.last_error"
    KEY_LAST_NOTIFIED_DIGEST = "update.last_notified_digest"
    KEY_LAST_NOTIFIED_AT = "update.last_notified_at"
    KEY_LAST_TRIGGER_AT = "update.last_trigger_at"

    def __init__(self, db: Database, settings: Settings, telegram: TelegramManager):
        self.db = db
        self.settings = settings
        self.telegram = telegram

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    @staticmethod
    def _parse_image_ref(image_ref: str) -> ParsedImageRef:
        text = (image_ref or "").strip()
        if not text:
            raise ValueError("APP_IMAGE 为空")
        if "@" in text:
            raise ValueError("APP_IMAGE 使用 digest 固定版本，无法执行更新检查")

        last_slash = text.rfind("/")
        last_colon = text.rfind(":")
        if last_colon > last_slash:
            image_name = text[:last_colon]
            tag = text[last_colon + 1 :]
        else:
            image_name = text
            tag = "latest"

        parts = image_name.split("/")
        if len(parts) >= 2 and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
            registry = parts[0]
            repository = "/".join(parts[1:])
        else:
            registry = "docker.io"
            repository = image_name if "/" in image_name else f"library/{image_name}"

        if not repository:
            raise ValueError(f"无效镜像名: {image_ref}")
        if not tag:
            tag = "latest"
        return ParsedImageRef(
            registry=registry,
            repository=repository,
            tag=tag,
            full=f"{registry}/{repository}:{tag}",
        )

    @staticmethod
    def _parse_bearer_challenge(header: str) -> tuple[str, dict[str, str]]:
        if not header:
            raise ValueError("缺少 Www-Authenticate 响应头")
        if not header.lower().startswith("bearer "):
            raise ValueError(f"不支持的认证类型: {header}")
        raw = header[len("Bearer ") :]
        params: dict[str, str] = {}
        for part in raw.split(","):
            part = part.strip()
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            params[key.strip()] = value.strip().strip('"')
        realm = params.get("realm", "").strip()
        if not realm:
            raise ValueError(f"Bearer challenge 缺少 realm: {header}")
        return realm, params

    @classmethod
    def _fetch_remote_digest_sync(cls, parsed: ParsedImageRef, timeout: int) -> str:
        accept_header = (
            "application/vnd.oci.image.index.v1+json,"
            "application/vnd.docker.distribution.manifest.list.v2+json,"
            "application/vnd.oci.image.manifest.v1+json,"
            "application/vnd.docker.distribution.manifest.v2+json"
        )
        manifest_url = f"https://{parsed.registry}/v2/{parsed.repository}/manifests/{parsed.tag}"

        def do_request(token: str | None) -> tuple[bytes, url_request.addinfourl]:
            headers = {"Accept": accept_header}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            req = url_request.Request(manifest_url, headers=headers, method="GET")
            resp = url_request.urlopen(req, timeout=timeout)
            body = resp.read()
            return body, resp

        token: str | None = None
        try:
            _, resp = do_request(token=None)
            digest = (resp.headers.get("Docker-Content-Digest") or "").strip()
            if not digest:
                raise RuntimeError("镜像仓库返回内容缺少 Docker-Content-Digest")
            return digest
        except url_error.HTTPError as exc:
            if exc.code != 401:
                raise
            challenge = exc.headers.get("Www-Authenticate", "")
            realm, params = cls._parse_bearer_challenge(challenge)
            query = {}
            service = params.get("service")
            scope = params.get("scope")
            if service:
                query["service"] = service
            if scope:
                query["scope"] = scope
            token_url = realm
            if query:
                token_url = f"{realm}?{url_parse.urlencode(query)}"
            token_req = url_request.Request(token_url, method="GET")
            token_resp = url_request.urlopen(token_req, timeout=timeout)
            token_data = json.loads(token_resp.read().decode("utf-8"))
            token = token_data.get("token") or token_data.get("access_token")
            if not token:
                raise RuntimeError("拉取仓库令牌失败: 响应里没有 token")
            _, resp = do_request(token=token)
            digest = (resp.headers.get("Docker-Content-Digest") or "").strip()
            if not digest:
                raise RuntimeError("镜像仓库返回内容缺少 Docker-Content-Digest")
            return digest

    def _is_watchtower_api_enabled(self) -> bool:
        return bool((self.settings.watchtower_url or "").strip()) and bool(
            (self.settings.watchtower_http_token or "").strip()
        )

    def _trigger_watchtower_update_sync(self) -> tuple[bool, int, str]:
        url = f"{self.settings.watchtower_url.rstrip('/')}/v1/update"
        token = self.settings.watchtower_http_token.strip()
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Api-Token": token,
            "Content-Type": "application/json",
        }
        req = url_request.Request(url, headers=headers, method="POST", data=b"{}")
        try:
            resp = url_request.urlopen(req, timeout=self.settings.update_http_timeout_seconds)
            body = resp.read().decode("utf-8", errors="ignore")
            return True, int(getattr(resp, "status", 200) or 200), body[:500]
        except url_error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            return False, int(exc.code), body[:500]
        except Exception as exc:
            return False, 0, str(exc)

    async def _check_status_core(self, send_notify: bool) -> dict[str, object]:
        now = self._now()
        parsed = self._parse_image_ref(self.settings.app_image)
        try:
            latest_digest = await asyncio.to_thread(
                self._fetch_remote_digest_sync,
                parsed,
                int(self.settings.update_http_timeout_seconds),
            )
            await self.db.set_setting(self.KEY_LAST_ERROR, "")
        except Exception as exc:
            err = f"更新检查失败: {exc}"
            await self.db.set_setting(self.KEY_LAST_ERROR, err[:500])
            await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)
            return {
                "ok": False,
                "image": parsed.full,
                "error": err,
                "has_update": False,
                "last_check_at": now,
            }

        confirmed_digest = await self.db.get_setting(self.KEY_CONFIRMED_DIGEST)
        baseline_initialized = False
        if not confirmed_digest:
            # 首次启用更新检查时，用当前最新 digest 作为基线。
            await self.db.set_setting(self.KEY_CONFIRMED_DIGEST, latest_digest)
            confirmed_digest = latest_digest
            baseline_initialized = True

        has_update = latest_digest != confirmed_digest

        notified = False
        if send_notify and has_update and self.settings.update_notify_enabled:
            last_notified = await self.db.get_setting(self.KEY_LAST_NOTIFIED_DIGEST)
            if last_notified != latest_digest:
                await self.telegram.send_notification(
                    "🆕 检测到镜像更新\n"
                    f"镜像: {parsed.full}\n"
                    f"当前确认: {confirmed_digest}\n"
                    f"最新版本: {latest_digest}\n"
                    "请在面板点击“确认并更新”执行升级。"
                )
                await self.db.set_setting(self.KEY_LAST_NOTIFIED_DIGEST, latest_digest)
                await self.db.set_setting(self.KEY_LAST_NOTIFIED_AT, now)
                notified = True

        await self.db.set_setting(self.KEY_LAST_CHECK_AT, now)
        return {
            "ok": True,
            "image": parsed.full,
            "registry": parsed.registry,
            "repository": parsed.repository,
            "tag": parsed.tag,
            "latest_digest": latest_digest,
            "confirmed_digest": confirmed_digest,
            "has_update": has_update,
            "baseline_initialized": baseline_initialized,
            "notified": notified,
            "notify_enabled": bool(self.settings.update_notify_enabled),
            "watchtower_api_enabled": self._is_watchtower_api_enabled(),
            "last_check_at": now,
            "last_error": await self.db.get_setting(self.KEY_LAST_ERROR),
            "last_notified_at": await self.db.get_setting(self.KEY_LAST_NOTIFIED_AT),
            "last_trigger_at": await self.db.get_setting(self.KEY_LAST_TRIGGER_AT),
        }

    async def get_status(self) -> dict[str, object]:
        return await self._check_status_core(send_notify=False)

    async def check_and_notify(self) -> dict[str, object]:
        return await self._check_status_core(send_notify=True)

    async def confirm_and_trigger_update(self) -> dict[str, object]:
        running_jobs = await self.db.count_running_recovery_jobs()
        if running_jobs > 0:
            raise RuntimeError(f"当前有 {running_jobs} 个恢复任务正在执行，请等待完成后再更新")

        status = await self._check_status_core(send_notify=False)
        if not bool(status.get("ok")):
            raise RuntimeError(str(status.get("error") or "更新检查失败"))

        latest_digest = str(status.get("latest_digest") or "")
        if not latest_digest:
            raise RuntimeError("无法获取最新镜像 digest")

        triggered = False
        trigger_status_code = 0
        trigger_response = ""
        if self._is_watchtower_api_enabled():
            ok, code, body = await asyncio.to_thread(self._trigger_watchtower_update_sync)
            triggered = ok
            trigger_status_code = code
            trigger_response = body
            if not ok:
                raise RuntimeError(f"触发 watchtower 更新失败(status={code}): {body}")

        now = self._now()
        await self.db.set_setting(self.KEY_CONFIRMED_DIGEST, latest_digest)
        await self.db.set_setting(self.KEY_LAST_TRIGGER_AT, now)
        await self.db.set_setting(self.KEY_LAST_ERROR, "")

        return {
            "ok": True,
            "triggered": triggered,
            "watchtower_api_enabled": self._is_watchtower_api_enabled(),
            "latest_digest": latest_digest,
            "status_code": trigger_status_code,
            "trigger_response": trigger_response,
            "confirmed_at": now,
        }
