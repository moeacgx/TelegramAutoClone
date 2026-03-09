from pathlib import Path

import pytest

from app.db import Database
from app.services.clone_runtime_settings import CloneSettingsService


@pytest.mark.asyncio
async def test_db_setting_roundtrip(tmp_path: Path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    await db.set_setting("sample.key", "sample-value")

    assert await db.get_setting("sample.key") == "sample-value"
    assert await db.get_setting("missing.key") is None


@pytest.mark.asyncio
async def test_clone_settings_defaults(tmp_path: Path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    service = CloneSettingsService(db)
    settings = await service.get_settings()

    assert settings.md5_mutation_enabled is False
    assert settings.download_group_concurrency == 2


@pytest.mark.asyncio
async def test_clone_settings_invalid_raw_values_fallback_to_defaults(tmp_path: Path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    await db.set_setting(CloneSettingsService.KEY_MD5_MUTATION_ENABLED, "not-bool")
    await db.set_setting(CloneSettingsService.KEY_DOWNLOAD_GROUP_CONCURRENCY, "not-int")

    service = CloneSettingsService(db)
    settings = await service.get_settings()

    assert settings.md5_mutation_enabled is False
    assert settings.download_group_concurrency == 2


@pytest.mark.asyncio
async def test_clone_settings_update_roundtrip(tmp_path: Path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    service = CloneSettingsService(db)
    updated = await service.update_settings(md5_mutation_enabled=True, download_group_concurrency=4)

    assert updated.md5_mutation_enabled is True
    assert updated.download_group_concurrency == 4
    assert await db.get_setting(CloneSettingsService.KEY_MD5_MUTATION_ENABLED) == "1"
    assert await db.get_setting(CloneSettingsService.KEY_DOWNLOAD_GROUP_CONCURRENCY) == "4"
