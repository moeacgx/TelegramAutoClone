import logging

from telethon import events

from app.db import Database
from app.services.clone_service import CloneService
from app.services.monitor_service import is_channel_unavailable_error
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


class ListenerService:
    def __init__(self, db: Database, telegram: TelegramManager, clone_service: CloneService):
        self.db = db
        self.telegram = telegram
        self.clone_service = clone_service
        self._registered = False
        self._handler_ref = None

    async def start(self) -> None:
        if self._registered:
            return

        @self.telegram.user_client.on(events.NewMessage())
        async def _handler(event):
            await self.on_new_message(event)

        self._handler_ref = _handler
        self._registered = True

    async def stop(self) -> None:
        if not self._registered:
            return
        if self._handler_ref is not None:
            self.telegram.user_client.remove_event_handler(self._handler_ref)
        self._registered = False

    async def on_new_message(self, event) -> None:
        try:
            chat_id = int(event.chat_id or 0)
            if chat_id == 0:
                return

            source_group = await self.db.get_source_group_by_chat_id(chat_id)
            if not source_group or int(source_group.get("enabled", 0)) != 1:
                return

            message = event.message
            topic_id = self.clone_service.extract_topic_id(message)
            if topic_id is None:
                topic_id = int(getattr(message, "id", 0) or 0)
            if topic_id == 0:
                return

            topic = await self.db.get_topic(int(source_group["id"]), int(topic_id))
            if not topic or int(topic.get("enabled", 0)) != 1:
                return

            binding = await self.db.get_binding(int(source_group["id"]), int(topic_id))
            if not binding or int(binding.get("active", 0)) != 1:
                return

            try:
                await self.clone_service.clone_message_no_reference(
                    message=message,
                    target_channel=int(binding["channel_chat_id"]),
                )
            except Exception as exc:
                if not is_channel_unavailable_error(exc):
                    raise
                await self.db.add_banned_channel(
                    source_group_id=int(source_group["id"]),
                    topic_id=int(topic_id),
                    channel_chat_id=int(binding["channel_chat_id"]),
                    reason=str(exc),
                )
                queue_id = await self.db.enqueue_recovery(
                    source_group_id=int(source_group["id"]),
                    topic_id=int(topic_id),
                    old_channel_chat_id=int(binding["channel_chat_id"]),
                    reason=str(exc),
                )
                await self.telegram.send_notification(
                    f"⚠️ 实时克隆发现频道失效\n"
                    f"source_group_id={source_group['id']} topic_id={topic_id}\n"
                    f"旧频道={binding['channel_chat_id']}\n"
                    f"已进入恢复队列 #{queue_id}"
                )

        except Exception as exc:
            logger.exception("实时监听处理失败: %s", exc)
