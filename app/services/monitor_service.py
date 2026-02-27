import logging
from telethon import errors as tg_errors

from app.db import Database
from app.services.channel_service import ChannelService
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


def is_channel_unavailable_error(exc: Exception) -> bool:
    unavailable_errors = (
        tg_errors.ChannelPrivateError,
        tg_errors.ChannelInvalidError,
        tg_errors.ChannelPublicGroupNaError,
        tg_errors.ChatAdminRequiredError,
    )
    if isinstance(exc, unavailable_errors):
        return True

    text = str(exc).lower()
    keywords = [
        "channelprivateerror",
        "channelinvaliderror",
        "chatadminrequirederror",
        "forbidden",
        "private channel",
        "have no rights",
    ]
    return any(word in text for word in keywords)


class MonitorService:
    def __init__(
        self,
        db: Database,
        telegram: TelegramManager,
        channel_service: ChannelService,
        interval_seconds: int,
    ):
        self.db = db
        self.telegram = telegram
        self.channel_service = channel_service
        self.interval_seconds = interval_seconds

    async def scan_once(self) -> dict[str, int]:
        scanned = 0
        skipped_source_disabled = 0
        unavailable = 0
        enqueued = 0
        bindings = await self.db.list_active_bindings()
        for binding in bindings:
            scanned += 1
            if int(binding.get("source_enabled", 0)) != 1:
                skipped_source_disabled += 1
                continue

            ok, error_text = await self.channel_service.check_channel_access(
                int(binding["channel_chat_id"])
            )
            if ok:
                continue

            unavailable += 1
            await self.db.add_banned_channel(
                source_group_id=int(binding["source_group_id"]),
                topic_id=int(binding["topic_id"]),
                channel_chat_id=int(binding["channel_chat_id"]),
                reason=error_text or "频道不可访问",
            )
            queue_id = await self.db.enqueue_recovery(
                source_group_id=int(binding["source_group_id"]),
                topic_id=int(binding["topic_id"]),
                old_channel_chat_id=int(binding["channel_chat_id"]),
                reason=error_text or "频道不可访问",
            )
            source_title = str(binding.get("source_title") or f"source_group_id={binding['source_group_id']}")
            topic_title = str(binding.get("topic_title") or f"topic_id={binding['topic_id']}")
            channel_title = str(binding.get("channel_title") or f"频道{binding['channel_chat_id']}")
            await self.telegram.send_notification(
                f"⚠️ 检测到频道失效\n"
                f"任务组: {source_title} (id={binding['source_group_id']})\n"
                f"话题: {topic_title} (topic_id={binding['topic_id']})\n"
                f"旧频道: {channel_title} ({binding['channel_chat_id']})\n"
                f"已进入恢复队列 #{queue_id}"
            )
            enqueued += 1
            logger.warning(
                "检测到失效频道，已入队: source_group_id=%s topic_id=%s channel=%s queue_id=%s",
                binding["source_group_id"],
                binding["topic_id"],
                binding["channel_chat_id"],
                queue_id,
            )

        return {
            "scanned": scanned,
            "skipped_source_disabled": skipped_source_disabled,
            "unavailable": unavailable,
            "enqueued": enqueued,
        }
