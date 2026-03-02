import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from telethon import errors as tg_errors
from telethon import functions
from telethon.tl.types import Channel
from telethon.utils import get_peer_id

from app.db import Database
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


class ChannelService:
    def __init__(self, db: Database, telegram: TelegramManager):
        self.db = db
        self.telegram = telegram

    async def _is_bot_admin(self, chat_id: int) -> bool:
        try:
            bot_entity = await self.telegram.bot_client.get_entity(chat_id)
            permissions = await self.telegram.bot_client.get_permissions(bot_entity, "me")
            return bool(permissions and permissions.is_admin)
        except Exception:
            return False

    async def refresh_standby_channels(self) -> dict[str, Any]:
        if not await self.telegram.is_bot_authorized():
            return {
                "scanned_channels": 0,
                "discovered": 0,
                "checked_permissions": 0,
                "skipped_permission_checks": 0,
                "standby_count": len(await self.db.list_standby_channels()),
                "warning": "Bot 未登录，无法校验备用频道权限",
            }

        await self.telegram.ensure_bot_connected()

        removed = 0
        scanned_channels = 0
        checked_permissions = 0
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        standby_channels = await self.db.list_standby_channels()

        # 仅校验当前备用池，不从历史 channels 缓存扩容。
        for channel_row in standby_channels:
            scanned_channels += 1
            chat_id = int(channel_row["chat_id"])
            title = str(channel_row.get("title") or chat_id)
            checked_permissions += 1
            is_admin = await self._is_bot_admin(chat_id)

            active_bindings = await self.db.get_binding_by_channel(chat_id)
            if not is_admin:
                if active_bindings:
                    await self.db.upsert_channel(
                        chat_id=chat_id,
                        title=title,
                        is_standby=False,
                        in_use=True,
                        admin_check_at=now_iso,
                    )
                else:
                    await self.db.delete_channel(chat_id)
                    removed += 1
                continue

            await self.db.upsert_channel(
                chat_id=chat_id,
                title=title,
                is_standby=not bool(active_bindings),
                in_use=bool(active_bindings),
                admin_check_at=now_iso,
            )

        return {
            "scanned_channels": scanned_channels,
            "discovered": len(await self.db.list_standby_channels()),
            "removed": removed,
            "checked_permissions": checked_permissions,
            "skipped_permission_checks": 0,
            "standby_count": len(await self.db.list_standby_channels()),
        }

    async def add_standby_channels_batch(self, refs_text: str) -> dict[str, Any]:
        if not await self.telegram.is_bot_authorized():
            raise RuntimeError("Bot 未登录，无法添加备用频道")
        await self.telegram.ensure_bot_connected()

        refs = [line.strip() for line in (refs_text or "").splitlines() if line.strip()]
        if not refs:
            return {
                "ok": True,
                "added": 0,
                "updated": 0,
                "failed": [],
                "standby_count": len(await self.db.list_standby_channels()),
            }

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        added = 0
        updated = 0
        failed: list[dict[str, str]] = []
        visited: set[str] = set()

        for ref in refs:
            if ref in visited:
                continue
            visited.add(ref)

            try:
                entity = await self.telegram.resolve_chat(ref, prefer_user=False)
                if not isinstance(entity, Channel) or not getattr(entity, "broadcast", False):
                    raise ValueError("仅支持频道，且必须是 Bot 可访问的频道")

                chat_id = int(get_peer_id(entity))
                title = (getattr(entity, "title", None) or str(chat_id)).strip()
                if not await self._is_bot_admin(chat_id):
                    raise ValueError("Bot 不是该频道管理员，请先在 Telegram 里设置 Bot 为管理员")

                active_bindings = await self.db.get_binding_by_channel(chat_id)
                existed = await self.db.get_channel(chat_id)
                await self.db.upsert_channel(
                    chat_id=chat_id,
                    title=title,
                    is_standby=not bool(active_bindings),
                    in_use=bool(active_bindings),
                    admin_check_at=now_iso,
                )
                if existed:
                    updated += 1
                else:
                    added += 1
            except Exception as exc:
                failed.append({"ref": ref, "error": str(exc)})

        return {
            "ok": True,
            "added": added,
            "updated": updated,
            "failed": failed,
            "standby_count": len(await self.db.list_standby_channels()),
        }

    async def remove_standby_channel(self, chat_id: int) -> dict[str, Any]:
        channel_row = await self.db.get_channel(chat_id)
        if not channel_row:
            return {
                "ok": False,
                "removed": False,
                "error": f"频道不存在: {chat_id}",
            }

        active_bindings = await self.db.get_binding_by_channel(chat_id)
        if active_bindings or int(channel_row.get("in_use", 0)) == 1:
            return {
                "ok": False,
                "removed": False,
                "error": f"频道 {chat_id} 正在绑定使用，不能从备用池删除",
            }

        if int(channel_row.get("is_standby", 0)) != 1:
            return {
                "ok": False,
                "removed": False,
                "error": f"频道 {chat_id} 不在备用池",
            }

        await self.db.delete_channel(chat_id)
        return {
            "ok": True,
            "removed": True,
            "chat_id": chat_id,
            "standby_count": len(await self.db.list_standby_channels()),
        }

    async def remove_standby_channels_batch(self, chat_ids: list[int]) -> dict[str, Any]:
        removed = 0
        failed: list[dict[str, Any]] = []
        seen: set[int] = set()

        for raw_id in chat_ids:
            chat_id = int(raw_id)
            if chat_id in seen:
                continue
            seen.add(chat_id)
            result = await self.remove_standby_channel(chat_id)
            if result.get("ok"):
                removed += 1
            else:
                failed.append(
                    {
                        "chat_id": chat_id,
                        "error": str(result.get("error") or "删除失败"),
                    }
                )

        return {
            "ok": True,
            "removed": removed,
            "failed": failed,
            "standby_count": len(await self.db.list_standby_channels()),
        }

    async def clear_standby_channels(self) -> dict[str, Any]:
        channels_before = await self.db.list_channels()
        count = sum(1 for row in channels_before if int(row.get("in_use", 0)) == 0)
        if count > 0:
            await self.db.clear_standby_channels()
        return {
            "ok": True,
            "cleared": count,
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

    @staticmethod
    def _friendly_channel_access_error(exc: Exception, actor: str) -> str:
        text = str(exc).strip()
        low = text.lower()
        if isinstance(exc, tg_errors.UserNotParticipantError) or (
            "not a member of the specified megagroup or channel" in low
            and "getparticipantrequest" in low
        ):
            return f"{actor}不在该频道里，请先加入频道并确保具备管理员权限"
        if isinstance(exc, tg_errors.ChatAdminRequiredError) or "chatadminrequirederror" in low:
            return f"{actor}不是该频道管理员，请先授予管理员权限后再绑定"
        if isinstance(exc, tg_errors.ChannelPrivateError) or "channelprivateerror" in low:
            return f"该频道当前不可访问，请确认 {actor} 仍在频道中且具备访问权限"
        if isinstance(exc, tg_errors.ChannelInvalidError) or "channelinvaliderror" in low:
            return "频道无效，请检查频道 ID/@用户名/链接是否正确"
        if "could not find the input entity for peerchannel" in low:
            return f"{actor}当前会话未找到该频道实体缓存，请先在 Telegram 客户端打开该频道后重试"
        if "auth key unregistered" in low or "unauthorized" in low:
            return f"{actor}未登录，无法校验该频道权限"
        return text

    @staticmethod
    def _has_send_permission(permissions: Any) -> bool:
        # Telethon 在不同会话/实体上权限字段可能略有差异，优先检查常见发送权限字段。
        post_messages = getattr(permissions, "post_messages", None)
        send_messages = getattr(permissions, "send_messages", None)
        if post_messages is False or send_messages is False:
            return False
        return True

    @staticmethod
    def _is_entity_not_found_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return "could not find the input entity for peerchannel" in text

    @staticmethod
    def _entity_chat_id(entity: Any) -> int | None:
        try:
            return int(get_peer_id(entity))
        except Exception:
            raw_id = getattr(entity, "id", None)
            if raw_id is None:
                raw_id = getattr(entity, "channel_id", None)
            if raw_id is None:
                return None
            try:
                value = int(raw_id)
            except Exception:
                return None
            if value < 0:
                return value
            return int(f"-100{value}")

    async def _find_dialog_entity_by_chat_id(self, client: Any, channel_chat_id: int) -> Any | None:
        try:
            dialogs = await client.get_dialogs(limit=2000)
            for dialog in dialogs:
                entity = getattr(dialog, "entity", None)
                if not entity:
                    continue
                entity_chat_id = self._entity_chat_id(entity)
                if entity_chat_id == int(channel_chat_id):
                    return entity
        except Exception as exc:
            logger.warning("通过 dialogs 回填频道实体失败: channel=%s reason=%s", channel_chat_id, exc)
        return None

    async def _check_actor_access(
        self,
        *,
        actor: str,
        client: Any,
        channel_chat_id: int,
        require_admin: bool,
        require_send_permission: bool,
    ) -> tuple[bool, str | None, str | None]:
        for attempt in range(2):
            try:
                try:
                    entity = await client.get_entity(channel_chat_id)
                except Exception as exc:
                    if not self._is_entity_not_found_error(exc):
                        raise
                    # Telethon 仅凭 ID 可能命中不到本地实体缓存；回填 dialogs 后再校验，避免误判“频道失效”。
                    hydrated_entity = await self._find_dialog_entity_by_chat_id(client, channel_chat_id)
                    if not hydrated_entity:
                        return False, None, self._friendly_channel_access_error(exc, actor)
                    entity = hydrated_entity
                # 强制走一次远端接口，避免 get_entity 命中本地缓存导致“频道已失效却被判定可用”。
                await client(functions.channels.GetFullChannelRequest(channel=entity))
                permissions = await client.get_permissions(entity, "me")

                if require_admin and (not permissions or not bool(getattr(permissions, "is_admin", False))):
                    return False, None, f"{actor}不是该频道管理员"

                if require_send_permission and permissions and not self._has_send_permission(permissions):
                    return False, None, f"{actor}在该频道缺少发送权限"

                title = str(getattr(entity, "title", channel_chat_id))
                return True, title, None
            except tg_errors.FloodWaitError as exc:
                wait_seconds = int(getattr(exc, "seconds", 0) or 0)
                if attempt == 0 and 0 < wait_seconds <= 15:
                    logger.warning(
                        "频道访问检查触发 FloodWait，等待 %ss 后重试: actor=%s channel=%s",
                        wait_seconds,
                        actor,
                        channel_chat_id,
                    )
                    await asyncio.sleep(wait_seconds + 1)
                    continue
                return False, None, f"{actor}请求过于频繁，请 {max(wait_seconds, 1)} 秒后重试"
            except Exception as exc:
                return False, None, self._friendly_channel_access_error(exc, actor)
        return False, None, f"{actor}频道访问检查失败"

    async def check_channel_access(self, channel_chat_id: int) -> tuple[bool, str | None]:
        # 目标频道可用性必须以 Bot 为准：克隆发送链路实际由 Bot 执行。
        bot_ok, bot_title, bot_error = await self._check_actor_access(
            actor="Bot",
            client=self.telegram.bot_client,
            channel_chat_id=channel_chat_id,
            require_admin=True,
            require_send_permission=True,
        )
        if not bot_ok:
            return False, bot_error or "Bot 无法访问目标频道"

        if bot_title:
            await self.db.mark_channel_last_seen(channel_chat_id, title=bot_title)
        else:
            await self.db.mark_channel_last_seen(channel_chat_id)

        # 用户账号访问仅作告警参考，不阻断目标频道判定。
        user_ok, _, user_error = await self._check_actor_access(
            actor="用户账号",
            client=self.telegram.user_client,
            channel_chat_id=channel_chat_id,
            require_admin=False,
            require_send_permission=False,
        )
        if not user_ok and user_error:
            logger.info(
                "频道访问检查: Bot 正常，用户账号检查失败(不阻断): channel=%s reason=%s",
                channel_chat_id,
                user_error,
            )

        return True, None

    async def list_channels(self) -> list[dict[str, Any]]:
        return await self.db.list_channels()

    async def list_standby(self) -> list[dict[str, Any]]:
        return await self.db.list_standby_channels()
