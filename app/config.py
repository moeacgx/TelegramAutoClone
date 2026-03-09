from functools import lru_cache
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _read_app_version() -> str:
    candidates = [
        Path.cwd() / "VERSION",
        Path(__file__).resolve().parent.parent / "VERSION",
    ]
    for path in candidates:
        try:
            text = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if text:
            return text
    return "dev"


class Settings(BaseSettings):
    api_id: int = Field(default=0, alias="API_ID")
    api_hash: str = Field(default="", alias="API_HASH")
    bot_token: str = Field(default="", alias="BOT_TOKEN")

    database_path: str = Field(default="data/telegram_auto_clone.db", alias="DATABASE_PATH")
    topic_avatar_dir: str = Field(default="data/topic_avatars", alias="TOPIC_AVATAR_DIR")
    sessions_dir: str = Field(default="sessions", alias="SESSIONS_DIR")

    notify_chat_id: int | None = Field(default=None, alias="NOTIFY_CHAT_ID")
    monitor_interval_seconds: int = Field(default=60, alias="MONITOR_INTERVAL_SECONDS")
    standby_refresh_seconds: int = Field(default=120, alias="STANDBY_REFRESH_SECONDS")
    recovery_max_retry: int = Field(default=3, alias="RECOVERY_MAX_RETRY")

    app_version: str = Field(default_factory=lambda: _read_app_version())
    update_repository: str = Field(default="moeacgx/TelegramAutoClone", alias="UPDATE_REPOSITORY")
    update_github_token: str = Field(default="", alias="UPDATE_GITHUB_TOKEN")
    update_check_interval_seconds: int = Field(default=600, alias="UPDATE_CHECK_INTERVAL_SECONDS")
    update_http_timeout_seconds: int = Field(default=15, alias="UPDATE_HTTP_TIMEOUT_SECONDS")
    update_notify_enabled: bool = Field(default=True, alias="UPDATE_NOTIFY_ENABLED")

    self_update_enabled: bool = Field(default=True, alias="SELF_UPDATE_ENABLED")
    self_update_docker_only: bool = Field(default=True, alias="SELF_UPDATE_DOCKER_ONLY")
    self_update_work_dir: str = Field(default="/app/data/self_update", alias="SELF_UPDATE_WORK_DIR")
    self_update_executable_name: str = Field(default="telegram-auto-clone", alias="SELF_UPDATE_EXECUTABLE_NAME")
    self_update_asset_prefix: str = Field(default="telegram-auto-clone", alias="SELF_UPDATE_ASSET_PREFIX")
    self_update_restart_delay_seconds: int = Field(default=2, alias="SELF_UPDATE_RESTART_DELAY_SECONDS")

    panel_password: str = Field(default="", alias="PANEL_PASSWORD")
    panel_session_ttl_seconds: int = Field(default=86400, alias="PANEL_SESSION_TTL_SECONDS")

    app_name: str = "Telegram Auto Clone"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @model_validator(mode="after")
    def validate_panel_auth(self) -> "Settings":
        if not self.panel_password.strip():
            raise ValueError("PANEL_PASSWORD 未配置，拒绝启动")
        if self.panel_session_ttl_seconds <= 0:
            raise ValueError("PANEL_SESSION_TTL_SECONDS 必须大于 0")
        if self.update_check_interval_seconds <= 0:
            raise ValueError("UPDATE_CHECK_INTERVAL_SECONDS 必须大于 0")
        if self.update_http_timeout_seconds <= 0:
            raise ValueError("UPDATE_HTTP_TIMEOUT_SECONDS 必须大于 0")
        if self.self_update_restart_delay_seconds <= 0:
            raise ValueError("SELF_UPDATE_RESTART_DELAY_SECONDS 必须大于 0")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()