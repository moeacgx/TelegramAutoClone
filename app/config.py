from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_id: int = Field(default=0, alias="API_ID")
    api_hash: str = Field(default="", alias="API_HASH")
    bot_token: str = Field(default="", alias="BOT_TOKEN")

    database_path: str = Field(default="data/telegram_auto_clone.db", alias="DATABASE_PATH")
    sessions_dir: str = Field(default="sessions", alias="SESSIONS_DIR")

    notify_chat_id: int | None = Field(default=None, alias="NOTIFY_CHAT_ID")
    monitor_interval_seconds: int = Field(default=60, alias="MONITOR_INTERVAL_SECONDS")
    standby_refresh_seconds: int = Field(default=120, alias="STANDBY_REFRESH_SECONDS")
    recovery_max_retry: int = Field(default=3, alias="RECOVERY_MAX_RETRY")

    app_image: str = Field(default="ghcr.io/moeacgx/telegramautoclone:latest", alias="APP_IMAGE")
    watchtower_url: str = Field(default="http://watchtower:8080", alias="WATCHTOWER_URL")
    watchtower_http_token: str = Field(default="", alias="WATCHTOWER_HTTP_TOKEN")
    update_check_interval_seconds: int = Field(default=600, alias="UPDATE_CHECK_INTERVAL_SECONDS")
    update_http_timeout_seconds: int = Field(default=8, alias="UPDATE_HTTP_TIMEOUT_SECONDS")
    update_notify_enabled: bool = Field(default=True, alias="UPDATE_NOTIFY_ENABLED")

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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
