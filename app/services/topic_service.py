import logging
from typing import Any

from telethon import functions
from telethon.tl.types import Channel
from telethon.utils import get_peer_id

from app.db import Database
from app.services.telegram_manager import TelegramManager


class TopicService:
    def __init__(self, db: Database, telegram: TelegramManager):
        self.db = db
        self.telegram = telegram
        self.logger = logging.getLogger(__name__)

    async def _get_topics_by_ids(
        self,
        request_cls: Any,
        use_messages_namespace: bool,
        source_chat_id: int,
        topic_ids: list[int],
    ) -> list[Any]:
        if not topic_ids:
            return []
        if use_messages_namespace:
            request = request_cls(
                peer=source_chat_id,
                topics=topic_ids,
            )
        else:
            request = request_cls(
                channel=source_chat_id,
                topics=topic_ids,
            )
        response = await self.telegram.user_client(request)
        return list(response.topics or [])

    async def _get_topics_by_ids_resilient(
        self,
        request_cls: Any,
        use_messages_namespace: bool,
        source_chat_id: int,
        topic_ids: list[int],
    ) -> list[Any]:
        if not topic_ids:
            return []

        result: list[Any] = []
        queue: list[list[int]] = [list(topic_ids)]
        while queue:
            batch = queue.pop(0)
            try:
                chunk = await self._get_topics_by_ids(
                    request_cls=request_cls,
                    use_messages_namespace=use_messages_namespace,
                    source_chat_id=source_chat_id,
                    topic_ids=batch,
                )
                result.extend(chunk)
            except Exception as exc:
                if len(batch) == 1:
                    self.logger.warning("topic_id=%s 回查失败，已跳过: %s", batch[0], exc)
                    continue
                mid = len(batch) // 2
                queue.insert(0, batch[mid:])
                queue.insert(0, batch[:mid])
        return result

    async def add_source_group(self, chat_ref: str | int) -> dict[str, Any]:
        entity = await self.telegram.resolve_chat(chat_ref, prefer_user=True)
        if not isinstance(entity, Channel) or not getattr(entity, "megagroup", False):
            raise ValueError("仅支持超级群组")

        chat_id = int(get_peer_id(entity))
        title = entity.title or str(chat_id)
        return await self.db.add_or_update_source_group(chat_id=chat_id, title=title)

    async def sync_topics(self, source_group_id: int) -> list[dict[str, Any]]:
        source_group = await self.db.get_source_group_by_id(source_group_id)
        if not source_group:
            raise ValueError("任务组不存在")

        source_chat_id = int(source_group["chat_id"])
        client = self.telegram.user_client

        topic_map: dict[int, dict[str, Any]] = {}
        seen_pages: set[tuple[int, int]] = set()
        offset_topic = 0
        offset_id = 0

        get_topics_request_cls = getattr(functions.messages, "GetForumTopicsRequest", None)
        get_topics_by_id_cls = getattr(functions.messages, "GetForumTopicsByIDRequest", None)
        use_messages_namespace = True
        if get_topics_request_cls is None:
            get_topics_request_cls = getattr(functions.channels, "GetForumTopicsRequest", None)
            get_topics_by_id_cls = getattr(functions.channels, "GetForumTopicsByIDRequest", None)
            use_messages_namespace = False
        if get_topics_request_cls is None:
            raise RuntimeError("当前 Telethon 版本不支持论坛话题同步接口")

        try:
            for _ in range(100):
                page_key = (int(offset_topic), int(offset_id))
                if page_key in seen_pages:
                    break
                seen_pages.add(page_key)

                if use_messages_namespace:
                    request = get_topics_request_cls(
                        peer=source_chat_id,
                        offset_date=None,
                        offset_id=int(offset_id),
                        offset_topic=offset_topic,
                        limit=100,
                        q="",
                    )
                else:
                    request = get_topics_request_cls(
                        channel=source_chat_id,
                        offset_date=None,
                        offset_id=int(offset_id),
                        offset_topic=offset_topic,
                        limit=100,
                        q="",
                    )
                response = await client(request)
                chunk = response.topics or []
                if not chunk:
                    break

                for topic in chunk:
                    topic_id = int(topic.id)
                    topic_title = str(getattr(topic, "title", "") or topic_id)
                    topic_map[topic_id] = {"topic_id": topic_id, "title": topic_title}

                if len(chunk) < 100:
                    break
                last_topic = chunk[-1]
                next_offset_topic = int(getattr(last_topic, "id", 0) or 0)
                next_offset_id = int(getattr(last_topic, "top_message", 0) or 0)

                if next_offset_topic == int(offset_topic) and next_offset_id == int(offset_id):
                    break
                offset_topic = next_offset_topic
                offset_id = next_offset_id
        except Exception as exc:
            self.logger.warning(
                "论坛话题分页拉取失败，降级到按 topic_id 回查: source_group_id=%s chat_id=%s err=%s",
                source_group_id,
                source_chat_id,
                exc,
            )

        # 兜底：对数据库已有 topic_id 逐批回查，确保改名后的标题一定能刷新。
        existing_topics = await self.db.list_topics(source_group_id)
        existing_ids = sorted({int(row["topic_id"]) for row in existing_topics})
        if get_topics_by_id_cls is not None:
            for idx in range(0, len(existing_ids), 100):
                topic_ids = existing_ids[idx : idx + 100]
                if not topic_ids:
                    continue
                by_id_chunk = await self._get_topics_by_ids_resilient(
                    request_cls=get_topics_by_id_cls,
                    use_messages_namespace=use_messages_namespace,
                    source_chat_id=source_chat_id,
                    topic_ids=topic_ids,
                )
                for topic in by_id_chunk:
                    topic_id = int(topic.id)
                    topic_title = str(getattr(topic, "title", "") or topic_id)
                    topic_map[topic_id] = {"topic_id": topic_id, "title": topic_title}

        await self.db.upsert_topics(source_group_id, list(topic_map.values()))
        return await self.db.list_topics(source_group_id)

    async def list_source_groups(self) -> list[dict[str, Any]]:
        return await self.db.list_source_groups()

    async def delete_source_group(self, source_group_id: int) -> dict[str, int]:
        source_group = await self.db.get_source_group_by_id(source_group_id)
        if not source_group:
            raise ValueError("任务组不存在")
        return await self.db.delete_source_group(source_group_id)

    async def list_topics(self, source_group_id: int | None = None) -> list[dict[str, Any]]:
        return await self.db.list_topics(source_group_id)

    async def set_topic_enabled(self, source_group_id: int, topic_id: int, enabled: bool) -> None:
        await self.db.set_topic_enabled(source_group_id, topic_id, enabled)

    async def set_source_group_enabled(self, source_group_id: int, enabled: bool) -> None:
        await self.db.set_source_group_enabled(source_group_id, enabled)
