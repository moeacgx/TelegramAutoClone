from dataclasses import asdict, dataclass

from app.db import Database


@dataclass(frozen=True, slots=True)
class CloneRuntimeSettings:
    md5_mutation_enabled: bool = False
    download_group_concurrency: int = 2

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class CloneSettingsService:
    KEY_MD5_MUTATION_ENABLED = "clone.md5_mutation_enabled"
    KEY_DOWNLOAD_GROUP_CONCURRENCY = "clone.download_group_concurrency"

    DEFAULT_MD5_MUTATION_ENABLED = False
    DEFAULT_DOWNLOAD_GROUP_CONCURRENCY = 2
    MIN_DOWNLOAD_GROUP_CONCURRENCY = 1
    MAX_DOWNLOAD_GROUP_CONCURRENCY = 5

    def __init__(self, db: Database):
        self.db = db

    @classmethod
    def _parse_bool(cls, value: str | None, default: bool) -> bool:
        if value is None:
            return bool(default)
        low = str(value).strip().lower()
        if low in {"1", "true", "yes", "on"}:
            return True
        if low in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @classmethod
    def validate_download_group_concurrency(cls, value: int) -> int:
        number = int(value)
        if not (cls.MIN_DOWNLOAD_GROUP_CONCURRENCY <= number <= cls.MAX_DOWNLOAD_GROUP_CONCURRENCY):
            raise ValueError(
                f"download_group_concurrency 必须在 "
                f"{cls.MIN_DOWNLOAD_GROUP_CONCURRENCY}..{cls.MAX_DOWNLOAD_GROUP_CONCURRENCY} 之间"
            )
        return number

    @classmethod
    def _parse_download_group_concurrency(cls, value: str | None) -> int:
        if value is None:
            return cls.DEFAULT_DOWNLOAD_GROUP_CONCURRENCY
        try:
            return cls.validate_download_group_concurrency(int(value))
        except Exception:
            return cls.DEFAULT_DOWNLOAD_GROUP_CONCURRENCY

    async def get_settings(self) -> CloneRuntimeSettings:
        md5_value = await self.db.get_setting(self.KEY_MD5_MUTATION_ENABLED)
        concurrency_value = await self.db.get_setting(self.KEY_DOWNLOAD_GROUP_CONCURRENCY)
        return CloneRuntimeSettings(
            md5_mutation_enabled=self._parse_bool(md5_value, self.DEFAULT_MD5_MUTATION_ENABLED),
            download_group_concurrency=self._parse_download_group_concurrency(concurrency_value),
        )

    async def get_effective_settings(self, source_group_id: int | None = None) -> CloneRuntimeSettings:
        base = await self.get_settings()
        if not source_group_id:
            return base
        override = await self.db.get_source_group_md5_override(int(source_group_id))
        if override is None:
            return base
        return CloneRuntimeSettings(
            md5_mutation_enabled=bool(override),
            download_group_concurrency=base.download_group_concurrency,
        )

    async def update_settings(
        self,
        *,
        md5_mutation_enabled: bool,
        download_group_concurrency: int,
    ) -> CloneRuntimeSettings:
        validated_concurrency = self.validate_download_group_concurrency(download_group_concurrency)
        await self.db.set_setting(
            self.KEY_MD5_MUTATION_ENABLED,
            "1" if bool(md5_mutation_enabled) else "0",
        )
        await self.db.set_setting(
            self.KEY_DOWNLOAD_GROUP_CONCURRENCY,
            str(validated_concurrency),
        )
        return CloneRuntimeSettings(
            md5_mutation_enabled=bool(md5_mutation_enabled),
            download_group_concurrency=validated_concurrency,
        )
