import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from telethon import functions
from telethon.utils import get_peer_id

from app.db import Database
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


class ChannelService:
    def __init__(self, db: Database, telegram: TelegramManager):
        self.db = db
        self.telegram = telegram
        # 对已验证频道做周期性复检，减少每轮都请求管理员权限。
        self.admin_recheck_interval = timedelta(minutes=10)

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None

    def _needs_admin_recheck(self, channel_row: dict[str, Any] | None) -> bool:
        if not channel_row:
            return True

        last_checked = self._parse_timestamp(channel_row.get("admin_check_at"))
        if not last_checked:
            return True

        now = datetime.now(timezone.utc)
        return (now - last_checked) >= self.admin_recheck_interval

    async def refresh_standby_channels(self) -> dict[str, Any]:
        await self.telegram.ensure_bot_connected()

        discovered = 0
        checked_permissions = 0
        skipped_permission_checks = 0
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        async for dialog in self.telegram.bot_client.iter_dialogs():
            entity = dialog.entity
            if not dialog.is_channel:
                continue
            if not getattr(entity, "broadcast", False):
                continue

            chat_id = int(get_peer_id(entity))
            title = entity.title or str(chat_id)
            channel_row = await self.db.get_channel(chat_id)
            need_check = self._needs_admin_recheck(channel_row)

            is_admin = True
            if need_check:
                checked_permissions += 1
                try:
                    permissions = await self.telegram.bot_client.get_permissions(entity, "me")
                    is_admin = bool(permissions.is_admin)
                except Exception:
                    continue
            else:
                skipped_permission_checks += 1

            active_bindings = await self.db.get_binding_by_channel(chat_id)
            if not is_admin:
                await self.db.upsert_channel(
                    chat_id=chat_id,
                    title=title,
                    is_standby=False,
                    in_use=bool(active_bindings),
                    admin_check_at=now_iso if need_check else None,
                )
                continue

            if active_bindings:
                await self.db.upsert_channel(
                    chat_id=chat_id,
                    title=title,
                    is_standby=False,
                    in_use=True,
                    admin_check_at=now_iso if need_check else None,
                )
            else:
                await self.db.upsert_channel(
                    chat_id=chat_id,
                    title=title,
                    is_standby=True,
                    in_use=False,
                    admin_check_at=now_iso if need_check else None,
                )
            discovered += 1

        return {
            "discovered": discovered,
            "checked_permissions": checked_permissions,
            "skipped_permission_checks": skipped_permission_checks,
            "standby_count": len(await self.db.list_standby_channels()),
        }

    async def rename_channel(self, channel_chat_id: int, new_title: str) -> None:
        entity = await self.telegram.bot_client.get_entity(channel_chat_id)
        await self.telegram.bot_client(
            functions.channels.EditTitleRequest(
                channel=entity,
                title=(new_title or "未命名话题")[:128],
            )
        )
        await self.db.mark_channel_last_seen(channel_chat_id, title=(new_title or "未命名话题")[:128])

    async def check_channel_access(self, channel_chat_id: int) -> tuple[bool, str | None]:
        try:
            entity = await self.telegram.bot_client.get_entity(channel_chat_id)
            title = getattr(entity, "title", str(channel_chat_id))
            await self.db.mark_channel_last_seen(channel_chat_id, title=title)
            return True, None
        except Exception as exc:
            return False, str(exc)

    async def list_channels(self) -> list[dict[str, Any]]:
        return await self.db.list_channels()

    async def list_standby(self) -> list[dict[str, Any]]:
        return await self.db.list_standby_channels()
