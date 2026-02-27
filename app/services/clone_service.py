import asyncio
import logging
import tempfile
from collections.abc import Awaitable, Callable
from typing import Any

from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


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

    async def _forward_ids_no_reference(
        self,
        source_chat_id: int,
        target_channel: int,
        message_ids: list[int],
    ) -> bool:
        if not message_ids:
            return False

        for _ in range(2):
            try:
                await self.telegram.bot_client.forward_messages(
                    entity=target_channel,
                    messages=message_ids,
                    from_peer=source_chat_id,
                    drop_author=True,
                )
                logger.info(
                    "无引用转发成功: source=%s target=%s ids=%s",
                    source_chat_id,
                    target_channel,
                    message_ids,
                )
                return True
            except FloodWaitError as flood:
                logger.warning(
                    "无引用转发触发 FloodWait: source=%s target=%s ids=%s wait=%ss",
                    source_chat_id,
                    target_channel,
                    message_ids,
                    int(flood.seconds),
                )
                await asyncio.sleep(int(flood.seconds) + 1)
            except Exception as exc:
                logger.warning(
                    "无引用转发失败，将回退复制: source=%s target=%s ids=%s reason=%s",
                    source_chat_id,
                    target_channel,
                    message_ids,
                    str(exc),
                )
                return False
        return False

    async def _copy_single_message(self, message: Message, target_channel: int) -> bool:
        if not self.is_cloneable(message):
            return False

        message_id = int(getattr(message, "id", 0) or 0)
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
                        logger.info(
                            "媒体复制成功(直传媒体对象): msg_id=%s target=%s",
                            message_id,
                            target_channel,
                        )
                        return True
                    except Exception as direct_send_exc:
                        logger.warning(
                            "直传媒体对象失败，改为下载重传: msg_id=%s target=%s reason=%s",
                            message_id,
                            target_channel,
                            str(direct_send_exc),
                        )
                        with tempfile.TemporaryDirectory(prefix="tg_clone_") as temp_dir:
                            file_path = await self.telegram.user_client.download_media(message, file=temp_dir)
                            if not file_path:
                                logger.warning(
                                    "下载媒体失败: msg_id=%s target=%s",
                                    message_id,
                                    target_channel,
                                )
                                return False

                            thumb_path: str | None = None
                            try:
                                downloaded_thumb = await self.telegram.user_client.download_media(
                                    message, file=temp_dir, thumb=-1
                                )
                                if downloaded_thumb and downloaded_thumb != file_path:
                                    thumb_path = str(downloaded_thumb)
                            except Exception:
                                thumb_path = None

                            document = getattr(getattr(message, "media", None), "document", None)
                            send_kwargs: dict[str, Any] = {
                                "entity": target_channel,
                                "file": file_path,
                                "caption": caption,
                                "formatting_entities": caption_entities,
                            }
                            if thumb_path:
                                send_kwargs["thumb"] = thumb_path
                            if document and getattr(document, "attributes", None):
                                send_kwargs["attributes"] = document.attributes
                            if document and getattr(document, "mime_type", None):
                                send_kwargs["mime_type"] = document.mime_type
                            if getattr(message, "video", False):
                                send_kwargs["supports_streaming"] = True

                            await self.telegram.bot_client.send_file(**send_kwargs)
                            logger.info(
                                "媒体复制成功(下载重传): msg_id=%s target=%s thumb=%s",
                                message_id,
                                target_channel,
                                bool(thumb_path),
                            )
                            return True

                if text:
                    await self.telegram.bot_client.send_message(
                        entity=target_channel,
                        message=text,
                        formatting_entities=entities,
                    )
                    logger.info(
                        "文本复制成功: msg_id=%s target=%s",
                        message_id,
                        target_channel,
                    )
                    return True

                return False
            except FloodWaitError as flood:
                logger.warning(
                    "复制发送触发 FloodWait: msg_id=%s target=%s wait=%ss",
                    message_id,
                    target_channel,
                    int(flood.seconds),
                )
                await asyncio.sleep(int(flood.seconds) + 1)
            except Exception as exc:
                logger.warning(
                    "复制发送失败: msg_id=%s target=%s reason=%s",
                    message_id,
                    target_channel,
                    str(exc),
                )
                return False

        return False

    async def clone_message_no_reference(
        self,
        message: Message,
        target_channel: int,
        source_chat_id: int | None = None,
    ) -> bool:
        if not self.is_cloneable(message):
            return False

        source_id = int(source_chat_id or getattr(message, "chat_id", 0) or 0)
        if source_id:
            forwarded = await self._forward_ids_no_reference(
                source_chat_id=source_id,
                target_channel=target_channel,
                message_ids=[int(message.id)],
            )
            if forwarded:
                return True

        return await self._copy_single_message(message, target_channel)

    async def _collect_media_group_messages(
        self,
        source_chat_id: int,
        topic_id: int,
        reference_message: Message,
        search_window: int = 80,
    ) -> list[Message]:
        grouped_id = getattr(reference_message, "grouped_id", None)
        if not grouped_id:
            return [reference_message]
        if not self.in_topic(reference_message, topic_id):
            return []

        min_id = max(0, int(reference_message.id) - search_window)
        max_id = int(reference_message.id) + search_window

        # 对 grouped_id 相册按组收集，不再逐条做话题过滤，避免部分相册子消息缺少 reply_to_top_id 被误判跳过。
        collected: dict[int, Message] = {int(reference_message.id): reference_message}
        try:
            async for msg in self.telegram.user_client.iter_messages(
                source_chat_id,
                min_id=min_id,
                max_id=max_id,
                limit=search_window * 4,
            ):
                if getattr(msg, "grouped_id", None) != grouped_id:
                    continue
                collected[int(msg.id)] = msg
        except Exception as exc:
            logger.warning(
                "收集相册消息失败，使用已收集结果继续: source=%s grouped_id=%s ref_msg_id=%s reason=%s",
                source_chat_id,
                int(grouped_id),
                int(reference_message.id),
                str(exc),
            )

        if len(collected) <= 1:
            # 兜底：附近窗口没收集全时，扩大范围再扫一遍，避免相册被拆散导致“卡在某条消息”。
            try:
                scanned = 0
                found_any = False
                async for msg in self.telegram.user_client.iter_messages(
                    source_chat_id,
                    limit=1200,
                ):
                    scanned += 1
                    if getattr(msg, "grouped_id", None) != grouped_id:
                        if found_any and scanned >= 200:
                            break
                        continue
                    found_any = True
                    collected[int(msg.id)] = msg
            except Exception as exc:
                logger.warning(
                    "扩大范围收集相册失败: source=%s grouped_id=%s reason=%s",
                    source_chat_id,
                    int(grouped_id),
                    str(exc),
                )

        return sorted(collected.values(), key=lambda x: int(x.id))

    async def _clone_media_group_no_reference(
        self,
        messages: list[Message],
        source_chat_id: int,
        target_channel: int,
    ) -> int:
        if not messages:
            return 0

        ids = [int(msg.id) for msg in messages]
        forwarded = await self._forward_ids_no_reference(
            source_chat_id=source_chat_id,
            target_channel=target_channel,
            message_ids=ids,
        )
        if forwarded:
            logger.info(
                "相册无引用整组转发成功: source=%s target=%s count=%s grouped_ids=%s",
                source_chat_id,
                target_channel,
                len(messages),
                sorted({int(getattr(m, 'grouped_id', 0) or 0) for m in messages}),
            )
            return len(messages)

        logger.warning(
            "相册整组转发失败，改为逐条处理: source=%s target=%s ids=%s",
            source_chat_id,
            target_channel,
            ids,
        )
        success = 0
        for msg in messages:
            single_forwarded = await self._forward_ids_no_reference(
                source_chat_id=source_chat_id,
                target_channel=target_channel,
                message_ids=[int(msg.id)],
            )
            if single_forwarded:
                success += 1
                await asyncio.sleep(0.03)
                continue

            ok = await self._copy_single_message(msg, target_channel)
            if ok:
                success += 1
            await asyncio.sleep(0.03)
        logger.info(
            "相册逐条处理完成: source=%s target=%s success=%s total=%s",
            source_chat_id,
            target_channel,
            success,
            len(messages),
        )
        return success

    async def clone_topic_history(
        self,
        source_chat_id: int,
        topic_id: int,
        target_channel: int,
        start_message_id: int | None = None,
        progress_hook: Callable[[int], Awaitable[None]] | None = None,
        should_stop: Callable[[], Awaitable[bool]] | None = None,
    ) -> dict[str, Any]:
        total = 0
        cloned = 0
        skipped = 0
        requested_start_message_id = int(start_message_id or 0)
        # 历史克隆默认从话题根消息 ID 开始，避免扫描整个群历史。
        effective_start_message_id = max(requested_start_message_id, int(topic_id))
        checkpoint_message_id = effective_start_message_id
        pending_checkpoint_count = 0
        processed_groups: set[int] = set()
        processed_units = 0

        logger.info(
            "开始克隆话题历史: source=%s topic=%s target=%s request_start=%s effective_start=%s",
            source_chat_id,
            topic_id,
            target_channel,
            requested_start_message_id,
            effective_start_message_id,
        )

        async for message in self.telegram.user_client.iter_messages(
            source_chat_id,
            reverse=True,
            min_id=effective_start_message_id,
        ):
            if should_stop and await should_stop():
                raise RuntimeError("任务已手动停止")

            if not self.in_topic(message, topic_id):
                continue

            grouped_id = getattr(message, "grouped_id", None)
            current_message_id = int(getattr(message, "id", 0) or 0)
            if grouped_id:
                grouped_id = int(grouped_id)
                if grouped_id in processed_groups:
                    continue
                processed_groups.add(grouped_id)

                group_messages = await self._collect_media_group_messages(
                    source_chat_id=source_chat_id,
                    topic_id=topic_id,
                    reference_message=message,
                )
                if not group_messages:
                    raise RuntimeError(
                        f"相册消息收集失败: source={source_chat_id} topic={topic_id} "
                        f"grouped_id={grouped_id} ref_msg_id={current_message_id}"
                    )

                cloneable_messages = [m for m in group_messages if self.is_cloneable(m)]
                total += len(group_messages)
                skipped += len(group_messages) - len(cloneable_messages)

                if cloneable_messages:
                    ok_count = await self._clone_media_group_no_reference(
                        messages=cloneable_messages,
                        source_chat_id=source_chat_id,
                        target_channel=target_channel,
                    )
                    if ok_count != len(cloneable_messages):
                        raise RuntimeError(
                            f"相册克隆失败: source={source_chat_id} topic={topic_id} "
                            f"grouped_id={grouped_id} success={ok_count}/{len(cloneable_messages)}"
                        )
                    cloned += ok_count

                checkpoint_message_id = max(int(m.id) for m in group_messages)
                pending_checkpoint_count += 1
            else:
                total += 1
                if self.is_cloneable(message):
                    ok = await self.clone_message_no_reference(
                        message=message,
                        target_channel=target_channel,
                        source_chat_id=source_chat_id,
                    )
                    if not ok:
                        raise RuntimeError(
                            f"单条克隆失败: source={source_chat_id} topic={topic_id} "
                            f"msg_id={current_message_id}"
                        )
                    cloned += 1
                else:
                    skipped += 1

                checkpoint_message_id = current_message_id
                pending_checkpoint_count += 1

            if progress_hook and pending_checkpoint_count >= 5:
                await progress_hook(checkpoint_message_id)
                pending_checkpoint_count = 0

            processed_units += 1
            if processed_units % 20 == 0:
                logger.info(
                    "克隆进度: source=%s topic=%s target=%s processed_units=%s cloned=%s skipped=%s checkpoint=%s",
                    source_chat_id,
                    topic_id,
                    target_channel,
                    processed_units,
                    cloned,
                    skipped,
                    checkpoint_message_id,
                )

            await asyncio.sleep(0.03)

        if progress_hook and checkpoint_message_id > 0:
            await progress_hook(checkpoint_message_id)

        logger.info(
            "克隆话题历史完成: source=%s topic=%s target=%s total=%s cloned=%s skipped=%s checkpoint=%s",
            source_chat_id,
            topic_id,
            target_channel,
            total,
            cloned,
            skipped,
            checkpoint_message_id,
        )

        return {
            "total": total,
            "cloned": cloned,
            "skipped": skipped,
            "started_min_id": effective_start_message_id,
            "last_cloned_message_id": checkpoint_message_id,
        }
