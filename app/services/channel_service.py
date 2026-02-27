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
        if not await self.telegram.is_user_authorized():
            return {
                "scanned_channels": 0,
                "discovered": 0,
                "checked_permissions": 0,
                "skipped_permission_checks": 0,
                "standby_count": len(await self.db.list_standby_channels()),
                "warning": "用户账号未登录，无法扫描备用频道",
            }

        if not await self.telegram.is_bot_authorized():
            return {
                "scanned_channels": 0,
                "discovered": 0,
                "checked_permissions": 0,
                "skipped_permission_checks": 0,
                "standby_count": len(await self.db.list_standby_channels()),
                "warning": "Bot 未登录，无法校验备用频道权限",
            }

        await self.telegram.ensure_user_connected()
        await self.telegram.ensure_bot_connected()

        discovered = 0
        scanned_channels = 0
        checked_permissions = 0
        skipped_permission_checks = 0
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        # Bot 账号无法调用 iter_dialogs；改为用户账号扫描频道，再由 Bot 账号校验管理权限。
        async for dialog in self.telegram.user_client.iter_dialogs():
            entity = dialog.entity
            if not dialog.is_channel:
                continue
            if not getattr(entity, "broadcast", False):
                continue

            scanned_channels += 1
            chat_id = int(get_peer_id(entity))
            title = entity.title or str(chat_id)
            channel_row = await self.db.get_channel(chat_id)
            need_check = self._needs_admin_recheck(channel_row)

            # 复检窗口内复用上次状态，降低 Telegram API 压力。
            is_admin = bool(channel_row and (channel_row.get("is_standby") or channel_row.get("in_use")))
            if need_check:
                checked_permissions += 1
                try:
                    bot_entity = await self.telegram.bot_client.get_entity(chat_id)
                    permissions = await self.telegram.bot_client.get_permissions(bot_entity, "me")
                    is_admin = bool(permissions.is_admin)
                except Exception:
                    is_admin = False
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
            "scanned_channels": scanned_channels,
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
