import asyncio
import tempfile
from collections.abc import Awaitable, Callable
from typing import Any

from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from app.services.telegram_manager import TelegramManager


class CloneService:
    def __init__(self, telegram: TelegramManager):
        self.telegram = telegram

    @staticmethod
    def extract_topic_id(message: Message) -> int | None:
        reply_to = getattr(message, "reply_to", None)
        if not reply_to:
            return None

        topic_id = getattr(reply_to, "reply_to_top_id", None)
        if topic_id:
            return int(topic_id)

        if getattr(reply_to, "forum_topic", False):
            fallback = getattr(reply_to, "reply_to_msg_id", None)
            if fallback:
                return int(fallback)

        return None

    @classmethod
    def in_topic(cls, message: Message, topic_id: int) -> bool:
        msg_topic_id = cls.extract_topic_id(message)
        if msg_topic_id is None and int(getattr(message, "id", 0)) == int(topic_id):
            return True
        return int(msg_topic_id or 0) == int(topic_id)

    @staticmethod
    def is_cloneable(message: Message) -> bool:
        if hasattr(message, "action") and message.action:
            return False
        if getattr(message, "deleted", False):
            return False
        if message.media:
            return True
        text = message.message or message.text
        return bool(text and str(text).strip())

    async def clone_message_no_reference(self, message: Message, target_channel: int) -> bool:
        if not self.is_cloneable(message):
            return False

        text = message.message or message.text
        entities = message.entities

        for _ in range(2):
            try:
                if message.media:
                    caption = text if text else None
                    caption_entities = entities if caption else None
                    try:
                        await self.telegram.bot_client.send_file(
                            entity=target_channel,
                            file=message.media,
                            caption=caption,
                            formatting_entities=caption_entities,
                        )
                        return True
                    except Exception:
                        with tempfile.TemporaryDirectory(prefix="tg_clone_") as temp_dir:
                            file_path = await self.telegram.user_client.download_media(message, file=temp_dir)
                            if not file_path:
                                raise RuntimeError(f"下载媒体失败: message_id={message.id}")
                            await self.telegram.bot_client.send_file(
                                entity=target_channel,
                                file=file_path,
                                caption=caption,
                                formatting_entities=caption_entities,
                            )
                            return True

                if text:
                    await self.telegram.bot_client.send_message(
                        entity=target_channel,
                        message=text,
                        formatting_entities=entities,
                    )
                    return True
                return False
            except FloodWaitError as flood:
                await asyncio.sleep(int(flood.seconds) + 1)
        return False

    async def clone_topic_history(
        self,
        source_chat_id: int,
        topic_id: int,
        target_channel: int,
        start_message_id: int | None = None,
        progress_hook: Callable[[int], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        total = 0
        cloned = 0
        skipped = 0
        checkpoint_message_id = int(start_message_id or 0)
        pending_checkpoint_count = 0

        async for message in self.telegram.user_client.iter_messages(
            source_chat_id,
            reverse=True,
            min_id=int(start_message_id or 0),
        ):
            if not self.in_topic(message, topic_id):
                continue

            total += 1
            if self.is_cloneable(message):
                ok = await self.clone_message_no_reference(message, target_channel)
                if ok:
                    cloned += 1
                else:
                    skipped += 1
            else:
                skipped += 1

            checkpoint_message_id = int(message.id)
            pending_checkpoint_count += 1

            if progress_hook and pending_checkpoint_count >= 20:
                await progress_hook(checkpoint_message_id)
                pending_checkpoint_count = 0

            await asyncio.sleep(0.05)

        if progress_hook and checkpoint_message_id > 0:
            await progress_hook(checkpoint_message_id)

        return {
            "total": total,
            "cloned": cloned,
            "skipped": skipped,
            "last_cloned_message_id": checkpoint_message_id,
        }
