from functools import lru_cache

from pydantic import Field
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

    app_name: str = "Telegram Auto Clone"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
