import asyncio
import copy
import logging
import mimetypes
import tempfile
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message
from telethon.tl.types import DocumentAttributeVideo, InputMediaUploadedDocument, InputMediaUploadedPhoto

from app.db import Database
from app.services.clone_runtime_settings import CloneRuntimeSettings, CloneSettingsService
from app.services.fast_telethon_transfer import fast_upload_file
from app.services.media_md5_mutator import mutate_media_file_md5
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PreparedMediaItem:
    message: Message
    file_path: str
    thumb_path: str | None
    mime_type: str | None
    attributes: list[Any]
    is_photo: bool
    supports_streaming: bool
    caption: str | None
    caption_entities: list[Any] | None


@dataclass(slots=True)
class HistoryCloneUnit:
    messages: list[Message]
    checkpoint_message_id: int
    total_count: int
    skipped_count: int
    runtime_settings: CloneRuntimeSettings
    prepared_media_items: list[PreparedMediaItem] = field(default_factory=list)
    temp_dir_obj: Any | None = None
    needs_prefetch_download: bool = False

    @property
    def cloneable_count(self) -> int:
        return len(self.messages)


class CloneService:
    def __init__(
        self,
        telegram: TelegramManager,
        db: Database,
        settings_service: CloneSettingsService | None = None,
        download_temp_dir: str | None = None,
    ):
        self.telegram = telegram
        self.db = db
        self.settings_service = settings_service or CloneSettingsService(db)
        self.download_temp_dir = str(download_temp_dir or "").strip() or None

    def _create_temp_dir(self) -> tempfile.TemporaryDirectory:
        base_dir = self.download_temp_dir
        if base_dir:
            Path(base_dir).mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(prefix="tg_clone_", dir=base_dir)

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
        if getattr(message, "media", None):
            return True
        text = getattr(message, "message", None) or getattr(message, "text", None)
        return bool(text and str(text).strip())

    async def _load_runtime_settings(self, source_group_id: int | None = None) -> CloneRuntimeSettings:
        return await self.settings_service.get_effective_settings(source_group_id)

    async def _forward_ids_no_reference(
        self,
        source_chat_id: int,
        target_channel: int,
        message_ids: list[int],
        on_error: Callable[[Exception], None] | None = None,
    ) -> bool:
        if not message_ids:
            return False

        for _ in range(2):
            try:
                await self.telegram.user_client.forward_messages(
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
                if on_error is not None:
                    on_error(exc)
                logger.warning(
                    "无引用转发失败，将回退复制: source=%s target=%s ids=%s reason=%s",
                    source_chat_id,
                    target_channel,
                    message_ids,
                    str(exc),
                )
                return False
        return False

    async def _send_text_message(
        self,
        message: Message,
        target_channel: int,
        on_error: Callable[[Exception], None] | None = None,
    ) -> bool:
        message_id = int(getattr(message, "id", 0) or 0)
        text = getattr(message, "message", None) or getattr(message, "text", None)
        entities = getattr(message, "entities", None)
        if not text:
            return False

        for _ in range(2):
            try:
                await self.telegram.user_client.send_message(
                    entity=target_channel,
                    message=text,
                    formatting_entities=entities,
                )
                logger.info("文本复制成功: msg_id=%s target=%s", message_id, target_channel)
                return True
            except FloodWaitError as flood:
                logger.warning(
                    "文本复制触发 FloodWait: msg_id=%s target=%s wait=%ss",
                    message_id,
                    target_channel,
                    int(flood.seconds),
                )
                await asyncio.sleep(int(flood.seconds) + 1)
            except Exception as exc:
                if on_error is not None:
                    on_error(exc)
                logger.warning("文本复制失败: msg_id=%s target=%s reason=%s", message_id, target_channel, str(exc))
                return False
        return False

    async def _copy_single_message(
        self,
        message: Message,
        target_channel: int,
        runtime_settings: CloneRuntimeSettings,
        on_error: Callable[[Exception], None] | None = None,
    ) -> bool:
        if not self.is_cloneable(message):
            return False

        if getattr(message, "media", None):
            if runtime_settings.md5_mutation_enabled:
                return await self._copy_media_messages_by_download(
                    messages=[message],
                    target_channel=target_channel,
                    runtime_settings=runtime_settings,
                    on_error=on_error,
                )
            return await self._copy_single_message_with_direct_fallback(
                message=message,
                target_channel=target_channel,
                on_error=on_error,
            )

        return await self._send_text_message(message, target_channel, on_error=on_error)

    async def _copy_single_message_with_direct_fallback(
        self,
        message: Message,
        target_channel: int,
        on_error: Callable[[Exception], None] | None = None,
    ) -> bool:
        message_id = int(getattr(message, "id", 0) or 0)
        caption = (getattr(message, "message", None) or getattr(message, "text", None)) or None
        caption_entities = getattr(message, "entities", None) if caption else None

        for _ in range(2):
            try:
                try:
                    await self.telegram.user_client.send_file(
                        entity=target_channel,
                        file=getattr(message, "media", None),
                        caption=caption,
                        formatting_entities=caption_entities,
                    )
                    logger.info("媒体复制成功(直传媒体对象): msg_id=%s target=%s", message_id, target_channel)
                    return True
                except Exception as direct_send_exc:
                    logger.warning(
                        "直传媒体对象失败，改为下载重传: msg_id=%s target=%s reason=%s",
                        message_id,
                        target_channel,
                        str(direct_send_exc),
                    )
                    fallback_settings = CloneRuntimeSettings(md5_mutation_enabled=False, download_group_concurrency=1)
                    return await self._copy_media_messages_by_download(
                        messages=[message],
                        target_channel=target_channel,
                        runtime_settings=fallback_settings,
                        on_error=on_error,
                    )
            except FloodWaitError as flood:
                logger.warning(
                    "复制发送触发 FloodWait: msg_id=%s target=%s wait=%ss",
                    message_id,
                    target_channel,
                    int(flood.seconds),
                )
                await asyncio.sleep(int(flood.seconds) + 1)
            except Exception as exc:
                if on_error is not None:
                    on_error(exc)
                logger.warning("复制发送失败: msg_id=%s target=%s reason=%s", message_id, target_channel, str(exc))
                return False
        return False

    async def clone_message_no_reference(
        self,
        message: Message,
        target_channel: int,
        source_chat_id: int | None = None,
        raise_on_send_error: bool = False,
        source_group_id: int | None = None,
    ) -> bool:
        if not self.is_cloneable(message):
            return False

        runtime_settings = await self._load_runtime_settings(source_group_id)
        source_id = int(source_chat_id or getattr(message, "chat_id", 0) or 0)
        send_error: Exception | None = None

        def capture_error(exc: Exception) -> None:
            nonlocal send_error
            send_error = exc

        if source_id and not runtime_settings.md5_mutation_enabled:
            forwarded = await self._forward_ids_no_reference(
                source_chat_id=source_id,
                target_channel=target_channel,
                message_ids=[int(message.id)],
                on_error=capture_error,
            )
            if forwarded:
                return True

        copied = await self._copy_single_message(
            message,
            target_channel,
            runtime_settings=runtime_settings,
            on_error=capture_error,
        )
        if copied:
            return True

        if raise_on_send_error and send_error is not None:
            raise send_error
        return False

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
            try:
                scanned = 0
                found_any = False
                async for msg in self.telegram.user_client.iter_messages(source_chat_id, limit=1200):
                    scanned += 1
                    if getattr(msg, "grouped_id", None) != grouped_id:
                        if found_any and scanned >= 200:
                            break
                        continue
                    found_any = True
                    collected[int(msg.id)] = msg
            except Exception as exc:
                logger.warning("扩大范围收集相册失败: source=%s grouped_id=%s reason=%s", source_chat_id, int(grouped_id), str(exc))

        return sorted(collected.values(), key=lambda x: int(x.id))

    async def _clone_media_group_no_reference(
        self,
        messages: list[Message],
        source_chat_id: int,
        target_channel: int,
        runtime_settings: CloneRuntimeSettings,
    ) -> int:
        if not messages:
            return 0

        if runtime_settings.md5_mutation_enabled:
            ok = await self._copy_media_messages_by_download(
                messages=messages,
                target_channel=target_channel,
                runtime_settings=runtime_settings,
            )
            return len(messages) if ok else 0

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

        logger.warning("相册整组转发失败，改为逐条处理: source=%s target=%s ids=%s", source_chat_id, target_channel, ids)
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

            ok = await self._copy_single_message(msg, target_channel, runtime_settings=runtime_settings)
            if ok:
                success += 1
            await asyncio.sleep(0.03)
        logger.info("相册逐条处理完成: source=%s target=%s success=%s total=%s", source_chat_id, target_channel, success, len(messages))
        return success

    async def _iter_history_units(
        self,
        source_chat_id: int,
        topic_id: int,
        effective_start_message_id: int,
        source_group_id: int | None = None,
    ):
        processed_groups: set[int] = set()
        async for message in self.telegram.user_client.iter_messages(
            source_chat_id,
            reverse=True,
            min_id=effective_start_message_id,
        ):
            if not self.in_topic(message, topic_id):
                continue

            runtime_settings = await self._load_runtime_settings(source_group_id)
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
                        f"相册消息收集失败: source={source_chat_id} topic={topic_id} grouped_id={grouped_id} ref_msg_id={current_message_id}"
                    )

                cloneable_messages = [m for m in group_messages if self.is_cloneable(m)]
                yield HistoryCloneUnit(
                    messages=cloneable_messages,
                    checkpoint_message_id=max(int(m.id) for m in group_messages),
                    total_count=len(group_messages),
                    skipped_count=len(group_messages) - len(cloneable_messages),
                    runtime_settings=runtime_settings,
                    needs_prefetch_download=runtime_settings.md5_mutation_enabled and any(getattr(m, "media", None) for m in cloneable_messages),
                )
            else:
                cloneable_messages = [message] if self.is_cloneable(message) else []
                yield HistoryCloneUnit(
                    messages=cloneable_messages,
                    checkpoint_message_id=current_message_id,
                    total_count=1,
                    skipped_count=0 if cloneable_messages else 1,
                    runtime_settings=runtime_settings,
                    needs_prefetch_download=runtime_settings.md5_mutation_enabled and bool(getattr(message, "media", None)),
                )

    async def _prepare_history_unit(self, unit: HistoryCloneUnit) -> HistoryCloneUnit:
        if not unit.needs_prefetch_download or not unit.messages:
            return unit

        temp_dir_obj = self._create_temp_dir()
        unit.temp_dir_obj = temp_dir_obj
        try:
            for index, message in enumerate(unit.messages):
                if not getattr(message, "media", None):
                    continue
                prepared_item = await self._download_media_item(
                    message=message,
                    temp_root=temp_dir_obj.name,
                    item_index=index,
                    runtime_settings=unit.runtime_settings,
                )
                unit.prepared_media_items.append(prepared_item)
            return unit
        except Exception:
            temp_dir_obj.cleanup()
            unit.temp_dir_obj = None
            raise

    async def _download_media_item(
        self,
        message: Message,
        temp_root: str,
        item_index: int,
        runtime_settings: CloneRuntimeSettings,
    ) -> PreparedMediaItem:
        item_dir = Path(temp_root) / f"item_{item_index + 1}_{int(getattr(message, 'id', 0) or 0)}"
        item_dir.mkdir(parents=True, exist_ok=True)

        file_path = await self.telegram.user_client.download_media(message, file=str(item_dir))
        if not file_path:
            raise RuntimeError(f"下载媒体失败: msg_id={int(getattr(message, 'id', 0) or 0)}")
        file_path = str(file_path)

        thumb_path: str | None = None
        try:
            downloaded_thumb = await self.telegram.user_client.download_media(message, file=str(item_dir), thumb=-1)
            if downloaded_thumb and str(downloaded_thumb) != file_path:
                thumb_path = str(downloaded_thumb)
        except Exception:
            thumb_path = None

        document = getattr(getattr(message, "media", None), "document", None)
        mime_type = str(getattr(document, "mime_type", "") or "").strip() or None
        if mime_type is None:
            mime_type = mimetypes.guess_type(file_path)[0]
        if runtime_settings.md5_mutation_enabled:
            file_path = mutate_media_file_md5(file_path, mime_type, str(item_dir))

        return PreparedMediaItem(
            message=message,
            file_path=file_path,
            thumb_path=thumb_path,
            mime_type=mime_type,
            attributes=self._clone_document_attributes(document),
            is_photo=self._is_photo_message(message),
            supports_streaming=bool(getattr(message, "video", False)),
            caption=(getattr(message, "message", None) or getattr(message, "text", None)) or None,
            caption_entities=getattr(message, "entities", None),
        )

    @staticmethod
    def _clone_document_attributes(document: Any) -> list[Any]:
        if not document or not getattr(document, "attributes", None):
            return []
        return [copy.copy(attr) for attr in list(document.attributes)]

    @staticmethod
    def _is_photo_message(message: Message) -> bool:
        if getattr(message, "photo", None):
            return True
        media = getattr(message, "media", None)
        return bool(getattr(media, "photo", None))

    async def _copy_media_messages_by_download(
        self,
        messages: list[Message],
        target_channel: int,
        runtime_settings: CloneRuntimeSettings,
        on_error: Callable[[Exception], None] | None = None,
    ) -> bool:
        temp_dir_obj = self._create_temp_dir()
        try:
            prepared_items: list[PreparedMediaItem] = []
            for index, message in enumerate(messages):
                prepared_items.append(
                    await self._download_media_item(
                        message=message,
                        temp_root=temp_dir_obj.name,
                        item_index=index,
                        runtime_settings=runtime_settings,
                    )
                )
            return await self._send_prepared_media_items(
                prepared_items=prepared_items,
                target_channel=target_channel,
                on_error=on_error,
            )
        except Exception as exc:
            if on_error is not None:
                on_error(exc)
            logger.warning(
                "下载上传失败: msg_ids=%s target=%s reason=%s",
                [int(getattr(message, 'id', 0) or 0) for message in messages],
                target_channel,
                str(exc),
            )
            return False
        finally:
            temp_dir_obj.cleanup()

    async def _send_prepared_media_items(
        self,
        prepared_items: list[PreparedMediaItem],
        target_channel: int,
        on_error: Callable[[Exception], None] | None = None,
    ) -> bool:
        if not prepared_items:
            return False

        message_ids = [int(getattr(item.message, "id", 0) or 0) for item in prepared_items]
        for _ in range(2):
            try:
                media_inputs = []
                for item in prepared_items:
                    media_inputs.append(await self._build_input_media(item))

                if len(media_inputs) == 1:
                    item = prepared_items[0]
                    await self.telegram.user_client.send_file(
                        entity=target_channel,
                        file=media_inputs[0],
                        caption=item.caption,
                        formatting_entities=item.caption_entities,
                    )
                else:
                    await self.telegram.user_client.send_file(
                        entity=target_channel,
                        file=media_inputs,
                        caption=[item.caption or "" for item in prepared_items],
                        formatting_entities=[item.caption_entities or [] for item in prepared_items],
                    )
                logger.info(
                    "媒体复制成功(下载重传): ids=%s target=%s count=%s",
                    message_ids,
                    target_channel,
                    len(prepared_items),
                )
                return True
            except FloodWaitError as flood:
                logger.warning(
                    "下载上传触发 FloodWait: ids=%s target=%s wait=%ss",
                    message_ids,
                    target_channel,
                    int(flood.seconds),
                )
                await asyncio.sleep(int(flood.seconds) + 1)
            except Exception as exc:
                if on_error is not None:
                    on_error(exc)
                logger.warning("下载上传发送失败: ids=%s target=%s reason=%s", message_ids, target_channel, str(exc))
                return False
        return False

    async def _build_input_media(self, item: PreparedMediaItem):
        uploaded_file = await fast_upload_file(self.telegram.user_client, item.file_path)
        if item.is_photo:
            return InputMediaUploadedPhoto(file=uploaded_file)

        thumb_uploaded = None
        if item.thumb_path:
            thumb_uploaded = await self.telegram.user_client.upload_file(item.thumb_path)

        attributes = self._normalize_document_attributes(item.attributes, item.supports_streaming)
        mime_type = item.mime_type or mimetypes.guess_type(item.file_path)[0] or "application/octet-stream"
        return InputMediaUploadedDocument(
            file=uploaded_file,
            thumb=thumb_uploaded,
            mime_type=mime_type,
            attributes=attributes,
        )

    @staticmethod
    def _normalize_document_attributes(attributes: list[Any], supports_streaming: bool) -> list[Any]:
        normalized: list[Any] = [copy.copy(attr) for attr in attributes]
        if supports_streaming:
            for attr in normalized:
                if isinstance(attr, DocumentAttributeVideo):
                    attr.supports_streaming = True
        return normalized

    async def _consume_history_unit(self, unit: HistoryCloneUnit, source_chat_id: int, target_channel: int) -> int:
        try:
            if not unit.messages:
                return 0

            if unit.runtime_settings.md5_mutation_enabled:
                if unit.prepared_media_items:
                    ok = await self._send_prepared_media_items(
                        prepared_items=unit.prepared_media_items,
                        target_channel=target_channel,
                    )
                else:
                    ok = await self._send_text_message(unit.messages[0], target_channel)
            elif len(unit.messages) > 1:
                ok_count = await self._clone_media_group_no_reference(
                    messages=unit.messages,
                    source_chat_id=source_chat_id,
                    target_channel=target_channel,
                    runtime_settings=unit.runtime_settings,
                )
                return ok_count
            else:
                ok = await self._copy_single_message(
                    unit.messages[0],
                    target_channel,
                    runtime_settings=unit.runtime_settings,
                )

            return len(unit.messages) if ok else 0
        finally:
            if unit.temp_dir_obj is not None:
                unit.temp_dir_obj.cleanup()
                unit.temp_dir_obj = None

    async def clone_topic_history(
        self,
        source_chat_id: int,
        topic_id: int,
        target_channel: int,
        start_message_id: int | None = None,
        progress_hook: Callable[[int], Awaitable[None]] | None = None,
        should_stop: Callable[[], Awaitable[bool]] | None = None,
        source_group_id: int | None = None,
    ) -> dict[str, Any]:
        total = 0
        cloned = 0
        skipped = 0
        requested_start_message_id = int(start_message_id or 0)
        effective_start_message_id = max(requested_start_message_id, int(topic_id))
        checkpoint_message_id = effective_start_message_id
        pending_checkpoint_count = 0
        processed_units = 0
        pending_units: deque[HistoryCloneUnit] = deque()
        pending_tasks: deque[asyncio.Task[HistoryCloneUnit]] = deque()
        pending_prefetch_count = 0

        logger.info(
            "开始克隆话题历史: source=%s topic=%s target=%s request_start=%s effective_start=%s",
            source_chat_id,
            topic_id,
            target_channel,
            requested_start_message_id,
            effective_start_message_id,
        )

        async def consume_next_pending() -> None:
            nonlocal cloned, skipped, total, checkpoint_message_id, pending_checkpoint_count, processed_units, pending_prefetch_count
            unit = pending_units.popleft()
            task = pending_tasks.popleft()
            if unit.needs_prefetch_download:
                pending_prefetch_count -= 1
            prepared_unit = await task
            total += prepared_unit.total_count
            skipped += prepared_unit.skipped_count
            ok_count = await self._consume_history_unit(prepared_unit, source_chat_id, target_channel)
            if ok_count != prepared_unit.cloneable_count:
                unit_type = "相册" if len(prepared_unit.messages) > 1 else "单条"
                raise RuntimeError(
                    f"{unit_type}克隆失败: source={source_chat_id} topic={topic_id} checkpoint={prepared_unit.checkpoint_message_id} success={ok_count}/{prepared_unit.cloneable_count}"
                )
            cloned += ok_count
            checkpoint_message_id = prepared_unit.checkpoint_message_id
            pending_checkpoint_count += 1
            processed_units += 1

            if progress_hook and pending_checkpoint_count >= 5:
                await progress_hook(checkpoint_message_id)
                pending_checkpoint_count = 0

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

        try:
            async for unit in self._iter_history_units(
                source_chat_id=source_chat_id,
                topic_id=topic_id,
                effective_start_message_id=effective_start_message_id,
                source_group_id=source_group_id,
            ):
                if should_stop and await should_stop():
                    raise RuntimeError("任务已手动停止")

                task = asyncio.create_task(self._prepare_history_unit(unit))
                pending_units.append(unit)
                pending_tasks.append(task)
                if unit.needs_prefetch_download:
                    pending_prefetch_count += 1

                if not unit.needs_prefetch_download:
                    await consume_next_pending()
                    await asyncio.sleep(0.03)
                    continue

                limit = max(1, int(unit.runtime_settings.download_group_concurrency))
                while pending_tasks and pending_prefetch_count >= limit:
                    if should_stop and await should_stop():
                        raise RuntimeError("任务已手动停止")
                    await consume_next_pending()
                    await asyncio.sleep(0.03)

            while pending_tasks:
                if should_stop and await should_stop():
                    raise RuntimeError("任务已手动停止")
                await consume_next_pending()
                await asyncio.sleep(0.03)
        finally:
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            for unit in pending_units:
                if unit.temp_dir_obj is not None:
                    unit.temp_dir_obj.cleanup()
                    unit.temp_dir_obj = None

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
