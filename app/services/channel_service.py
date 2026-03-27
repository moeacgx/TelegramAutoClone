import asyncio
import json
import logging
from pathlib import Path
import re
from datetime import datetime, timezone
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from telethon import errors as tg_errors
from telethon import functions
from telethon.tl import types as tl_types
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

    @staticmethod
    def _is_not_modified_error(exc: Exception) -> bool:
        low = str(exc).lower()
        return (
            "not modified" in low
            or "wasn't modified" in low
            or "chat_not_modified" in low
        )

    @staticmethod
    def _friendly_channel_profile_error(exc: Exception, action: str) -> str:
        text = str(exc).strip()
        low = text.lower()
        if isinstance(exc, tg_errors.ChatAdminRequiredError) or "chatadminrequirederror" in low:
            return f"Bot 不是该频道管理员，无法更新频道{action}"
        if "chat_restricted" in low:
            return f"频道受限，无法更新频道{action}"
        if "channelprivateerror" in low or isinstance(exc, tg_errors.ChannelPrivateError):
            return f"频道不可访问，无法更新频道{action}"
        if "not enough rights" in low or "have no rights" in low:
            return f"Bot 权限不足，无法更新频道{action}"
        return text or f"更新频道{action}失败"

    def _resolve_topic_avatar_file(self, avatar_path: str | None) -> Path | None:
        name = str(avatar_path or "").strip()
        if not name:
            return None
        safe_name = Path(name).name
        if safe_name != name:
            return None
        return Path(self.telegram.settings.topic_avatar_dir) / safe_name

    async def rename_channel(self, channel_chat_id: int, new_title: str) -> None:
        entity = await self.telegram.bot_client.get_entity(channel_chat_id)
        try:
            await self.telegram.bot_client(
                functions.channels.EditTitleRequest(
                    channel=entity,
                    title=(new_title or "未命名话题")[:128],
                )
            )
        except Exception as exc:
            if not self._is_not_modified_error(exc):
                raise RuntimeError(self._friendly_channel_profile_error(exc, "标题")) from exc
        await self.db.mark_channel_last_seen(channel_chat_id, title=(new_title or "未命名话题")[:128])

    async def set_channel_avatar(self, channel_chat_id: int, avatar_path: str) -> None:
        avatar_file = self._resolve_topic_avatar_file(avatar_path)
        if avatar_file is None or not avatar_file.exists():
            raise RuntimeError("频道头像文件不存在，请重新上传")

        raw_bytes = await asyncio.to_thread(avatar_file.read_bytes)
        if not raw_bytes:
            raise RuntimeError("频道头像文件为空，请重新上传")

        entity = await self.telegram.bot_client.get_entity(channel_chat_id)
        try:
            uploaded = await self.telegram.bot_client.upload_file(raw_bytes, file_name=avatar_file.name)
            await self.telegram.bot_client(
                functions.channels.EditPhotoRequest(
                    channel=entity,
                    photo=tl_types.InputChatUploadedPhoto(file=uploaded),
                )
            )
        except Exception as exc:
            if not self._is_not_modified_error(exc):
                raise RuntimeError(self._friendly_channel_profile_error(exc, "头像")) from exc

    async def apply_topic_profile(
        self,
        channel_chat_id: int,
        topic_title: str,
        topic_avatar_path: str | None = None,
    ) -> dict[str, bool]:
        await self.rename_channel(channel_chat_id, topic_title)
        avatar_applied = False
        if str(topic_avatar_path or "").strip():
            await self.set_channel_avatar(channel_chat_id, topic_avatar_path)
            avatar_applied = True
        return {"title_applied": True, "avatar_applied": avatar_applied}

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
    def _friendly_bot_probe_error(exc: Exception) -> str:
        text = str(exc).strip()
        low = text.lower()
        if "bot token" in low and "未配置" in low:
            return "BOT_TOKEN 未配置，无法执行频道探测"
        if "chat not found" in low or "bot was kicked" in low or "not a member of the channel chat" in low:
            return "Bot 不在该频道里，或已被移出频道"
        if "not enough rights" in low or "have no rights" in low or "can't send messages" in low:
            return "Bot 在该频道没有发言权限"
        if "forbidden" in low:
            return "Bot 在该频道无访问权限"
        if "too many requests" in low or "retry after" in low:
            return "Bot 请求过于频繁，请稍后重试"
        if "timed out" in low or "timeout" in low:
            return "Bot 探测请求超时，请稍后重试"
        if "temporary failure" in low or "temporarily unavailable" in low:
            return "Bot 探测网络临时异常，请稍后重试"
        return text

    @staticmethod
    def _extract_retry_after_seconds(exc: Exception) -> int:
        text = str(exc).lower()
        matched = re.search(r"retry after\s+(\d+)", text)
        if not matched:
            return 0
        try:
            return int(matched.group(1))
        except Exception:
            return 0

    @staticmethod
    def is_transient_probe_error_text(error_text: str | None) -> bool:
        low = str(error_text or "").lower()
        if not low:
            return False
        transient_keywords = (
            "retry after",
            "too many requests",
            "请求过于频繁",
            "timed out",
            "timeout",
            "temporary failure",
            "temporarily unavailable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "connection reset",
            "network is unreachable",
            "name or service not known",
            "http 429",
            "http 502",
            "http 503",
            "http 504",
        )
        return any(word in low for word in transient_keywords)

    @staticmethod
    def _extract_bot_api_http_error(http_exc: urlerror.HTTPError) -> str:
        status_code = int(getattr(http_exc, "code", 0) or 0)
        reason = str(getattr(http_exc, "reason", "") or "").strip()
        try:
            raw = http_exc.read().decode("utf-8", errors="ignore")
            payload = json.loads(raw)
            description = str(payload.get("description") or "").strip()
            retry_after = int(((payload.get("parameters") or {}).get("retry_after") or 0))
            if description:
                if retry_after > 0 and "retry after" not in description.lower():
                    return f"{description} (retry after {retry_after})"
                return description
        except Exception:
            pass

        if status_code and reason:
            return f"HTTP {status_code}: {reason}"
        if status_code:
            return f"HTTP {status_code}"
        return str(http_exc)

    async def _bot_api_request(
        self,
        method: str,
        payload: dict[str, Any],
        timeout_seconds: int = 12,
    ) -> Any:
        return await asyncio.to_thread(
            self._bot_api_request_sync,
            method,
            payload,
            timeout_seconds,
        )

    def _bot_api_request_sync(
        self,
        method: str,
        payload: dict[str, Any],
        timeout_seconds: int,
    ) -> Any:
        bot_token = str(self.telegram.settings.bot_token or "").strip()
        if not bot_token:
            raise RuntimeError("BOT_TOKEN 未配置")

        api_url = f"https://api.telegram.org/bot{bot_token}/{method}"
        body = json.dumps(payload).encode("utf-8")
        req = urlrequest.Request(
            api_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except urlerror.HTTPError as http_exc:
            raise RuntimeError(self._extract_bot_api_http_error(http_exc)) from http_exc
        except urlerror.URLError as net_exc:
            reason = str(getattr(net_exc, "reason", "") or "").strip()
            raise RuntimeError(reason or "Bot API 网络请求失败") from net_exc
        data = json.loads(raw)
        if not data.get("ok"):
            raise RuntimeError(data.get("description") or f"Bot API {method} 失败")
        return data.get("result")

    async def _delete_probe_message_with_retry(
        self,
        channel_chat_id: int,
        message_id: int,
    ) -> None:
        if message_id <= 0:
            return
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                await self._bot_api_request(
                    "deleteMessage",
                    {
                        "chat_id": int(channel_chat_id),
                        "message_id": message_id,
                    },
                    timeout_seconds=10,
                )
                return
            except tg_errors.FloodWaitError as delete_flood:
                last_exc = delete_flood
                wait_seconds = int(getattr(delete_flood, "seconds", 0) or 0)
                logger.warning(
                    "探测消息删除触发 FloodWait，等待 %ss 后重试: channel=%s",
                    wait_seconds,
                    channel_chat_id,
                )
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds + 1)
                else:
                    await asyncio.sleep(2)
            except Exception as delete_exc:
                last_exc = delete_exc
                wait_seconds = self._extract_retry_after_seconds(delete_exc)
                if wait_seconds > 0:
                    logger.warning(
                        "探测消息删除触发限流，等待 %ss 后重试: channel=%s",
                        wait_seconds,
                        channel_chat_id,
                    )
                    await asyncio.sleep(min(wait_seconds, 15) + 1)
                elif attempt == 0:
                    await asyncio.sleep(2)

        logger.warning(
            "探测消息删除失败，准备延迟清理: channel=%s reason=%s",
            channel_chat_id,
            last_exc,
        )

        async def _delayed_cleanup() -> None:
            await asyncio.sleep(20)
            try:
                await self._bot_api_request(
                    "deleteMessage",
                    {
                        "chat_id": int(channel_chat_id),
                        "message_id": message_id,
                    },
                    timeout_seconds=10,
                )
                logger.info(
                    "探测消息延迟清理成功: channel=%s message_id=%s",
                    channel_chat_id,
                    message_id,
                )
            except Exception as delete_exc:
                logger.warning(
                    "探测消息延迟清理失败: channel=%s reason=%s",
                    channel_chat_id,
                    delete_exc,
                )

        asyncio.create_task(_delayed_cleanup())

    async def _probe_bot_send_then_delete(
        self,
        channel_chat_id: int,
    ) -> tuple[bool, str | None, str | None]:
        for attempt in range(2):
            try:
                probe_text = f"[AutoClone Probe] {datetime.now(timezone.utc).isoformat(timespec='seconds')}"
                probe_message = await self._bot_api_request(
                    "sendMessage",
                    {
                        "chat_id": int(channel_chat_id),
                        "text": probe_text,
                        "disable_notification": True,
                        "disable_web_page_preview": True,
                        "allow_sending_without_reply": True,
                    },
                )
                message_id = int((probe_message or {}).get("message_id") or 0)
                chat_title = str((((probe_message or {}).get("chat") or {}).get("title") or "")).strip()
                await self._delete_probe_message_with_retry(channel_chat_id, message_id)
                title = chat_title or str(channel_chat_id)
                return True, title, None
            except Exception as exc:
                wait_seconds = self._extract_retry_after_seconds(exc)
                if attempt == 0 and 0 < wait_seconds <= 15:
                    logger.warning(
                        "发送探测消息触发 FloodWait，等待 %ss 后重试: channel=%s",
                        wait_seconds,
                        channel_chat_id,
                    )
                    await asyncio.sleep(wait_seconds + 1)
                    continue
                if wait_seconds > 0:
                    return False, None, f"Bot请求过于频繁，请 {max(wait_seconds, 1)} 秒后重试"
                return False, None, self._friendly_bot_probe_error(exc)
        return False, None, "Bot 发送探测消息失败"

    async def check_channel_access(self, channel_chat_id: int) -> tuple[bool, str | None]:
        # 用户要求：以 Bot 实发测试消息+立即删除来判定频道是否可用。
        bot_ok, bot_title, bot_error = await self._probe_bot_send_then_delete(channel_chat_id)
        if not bot_ok:
            return False, bot_error or "Bot 无法访问目标频道"

        if bot_title:
            await self.db.mark_channel_last_seen(channel_chat_id, title=bot_title)
        else:
            await self.db.mark_channel_last_seen(channel_chat_id)

        return True, None

    async def list_channels(self) -> list[dict[str, Any]]:
        return await self.db.list_channels()

    async def list_standby(self) -> list[dict[str, Any]]:
        return await self.db.list_standby_channels()
