import shutil
import zipfile
from pathlib import Path

import pytest

from app.config import Settings
from app.db import Database
from app.services.update_service import GitHubReleaseAsset, GitHubReleaseInfo, UpdateService


class DummyTelegram:
    def __init__(self):
        self.messages: list[str] = []

    async def send_notification(self, message: str) -> None:
        self.messages.append(message)


class DummyRestartService:
    def __init__(self):
        self.calls: list[tuple[int, str]] = []

    @property
    def restart_requested(self) -> bool:
        return bool(self.calls)

    def request_restart(self, delay_seconds: int, reason: str) -> None:
        self.calls.append((delay_seconds, reason))


def build_settings(tmp_path: Path, **overrides) -> Settings:
    payload = {
        "API_ID": 1,
        "API_HASH": "hash",
        "BOT_TOKEN": "token",
        "PANEL_PASSWORD": "password",
        "DATABASE_PATH": str(tmp_path / "test.db"),
        "app_version": "v1.0.0",
        "UPDATE_REPOSITORY": "owner/repo",
        "UPDATE_GITHUB_TOKEN": "",
        "UPDATE_CHECK_INTERVAL_SECONDS": 600,
        "UPDATE_HTTP_TIMEOUT_SECONDS": 5,
        "UPDATE_NOTIFY_ENABLED": True,
        "SELF_UPDATE_ENABLED": True,
        "SELF_UPDATE_DOCKER_ONLY": True,
        "SELF_UPDATE_WORK_DIR": str(tmp_path / "self_update"),
        "SELF_UPDATE_EXECUTABLE_NAME": "telegram-auto-clone",
        "SELF_UPDATE_ASSET_PREFIX": "telegram-auto-clone",
        "SELF_UPDATE_RESTART_DELAY_SECONDS": 1,
    }
    payload.update(overrides)
    return Settings.model_validate(payload)


def build_release(tag_name: str = "v1.1.0") -> GitHubReleaseInfo:
    return GitHubReleaseInfo(
        tag_name=tag_name,
        html_url="https://example.com/releases/tag/v1.1.0",
        body="release notes",
        published_at="2026-03-09T00:00:00Z",
        assets=[
            GitHubReleaseAsset(
                name="telegram-auto-clone-linux-x64.zip",
                browser_download_url="https://example.com/releases/download/v1.1.0/telegram-auto-clone-linux-x64.zip",
                size=1024,
            )
        ],
    )


def build_release_zip(zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("telegram-auto-clone/telegram-auto-clone", "#!/usr/bin/env sh\necho running\n")
        archive.writestr("telegram-auto-clone/app/static/.keep", "")
        archive.writestr("telegram-auto-clone/app/templates/.keep", "")


@pytest.mark.asyncio
async def test_check_and_notify_only_notifies_once_for_same_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    settings = build_settings(tmp_path)
    db = Database(str(tmp_path / "test.db"))
    await db.init()
    telegram = DummyTelegram()
    restart_service = DummyRestartService()
    service = UpdateService(db, settings, telegram, restart_service)

    monkeypatch.setattr(service, "_fetch_latest_release_sync", lambda *_args: build_release())
    monkeypatch.setattr(service, "_is_running_in_docker", lambda: True)

    first = await service.check_and_notify()
    second = await service.check_and_notify()

    assert first["ok"] is True
    assert first["has_update"] is True
    assert first["can_apply"] is True
    assert first["latest_tag"] == "v1.1.0"
    assert len(telegram.messages) == 1
    assert second["notified"] is False


@pytest.mark.asyncio
async def test_get_status_blocks_non_docker_when_enabled_for_docker_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    settings = build_settings(tmp_path)
    db = Database(str(tmp_path / "test.db"))
    await db.init()
    service = UpdateService(db, settings, DummyTelegram(), DummyRestartService())

    monkeypatch.setattr(service, "_fetch_latest_release_sync", lambda *_args: build_release())
    monkeypatch.setattr(service, "_is_running_in_docker", lambda: False)

    status = await service.get_status()

    assert status["ok"] is True
    assert status["has_update"] is True
    assert status["can_apply"] is False
    assert "Docker" in str(status["blocked_reason"])


@pytest.mark.asyncio
async def test_confirm_and_trigger_update_downloads_package_and_cleans_temp_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    settings = build_settings(tmp_path)
    db = Database(str(tmp_path / "test.db"))
    await db.init()
    restart_service = DummyRestartService()
    service = UpdateService(db, settings, DummyTelegram(), restart_service)

    asset_zip = tmp_path / "telegram-auto-clone-linux-x64.zip"
    build_release_zip(asset_zip)

    monkeypatch.setattr(service, "_fetch_latest_release_sync", lambda *_args: build_release())
    monkeypatch.setattr(service, "_is_running_in_docker", lambda: True)
    monkeypatch.setattr(
        service,
        "_download_asset_sync",
        lambda _url, destination, _timeout: shutil.copyfile(asset_zip, destination),
    )

    result = await service.confirm_and_trigger_update()
    current_dir = Path(settings.self_update_work_dir) / "current"
    workspace_dir = Path(settings.self_update_work_dir) / "workspace"

    assert result["ok"] is True
    assert result["triggered"] is True
    assert (current_dir / "telegram-auto-clone").exists()
    assert (current_dir / "VERSION").read_text(encoding="utf-8").strip() == "v1.1.0"
    assert restart_service.restart_requested is True
    assert all(not item.name.startswith("pkg-") for item in workspace_dir.glob("*"))
    assert all(not item.name.startswith("stage-") for item in workspace_dir.glob("*"))


@pytest.mark.asyncio
async def test_confirm_and_trigger_update_returns_without_restart_when_latest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    settings = build_settings(tmp_path, app_version="v1.1.0")
    db = Database(str(tmp_path / "test.db"))
    await db.init()
    restart_service = DummyRestartService()
    service = UpdateService(db, settings, DummyTelegram(), restart_service)

    monkeypatch.setattr(service, "_fetch_latest_release_sync", lambda *_args: build_release(tag_name="v1.1.0"))
    monkeypatch.setattr(service, "_is_running_in_docker", lambda: True)

    result = await service.confirm_and_trigger_update()

    assert result["ok"] is True
    assert result["triggered"] is False
    assert restart_service.restart_requested is False