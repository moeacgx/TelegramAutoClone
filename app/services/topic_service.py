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

        topics: list[dict[str, Any]] = []
        offset_topic = 0

        for _ in range(10):
            response = await client(
                functions.channels.GetForumTopicsRequest(
                    channel=source_chat_id,
                    offset_date=None,
                    offset_id=0,
                    offset_topic=offset_topic,
                    limit=100,
                    q="",
                )
            )
            chunk = response.topics or []
            if not chunk:
                break

            for topic in chunk:
                topics.append({"topic_id": int(topic.id), "title": topic.title})

            if len(chunk) < 100:
                break
            offset_topic = int(chunk[-1].id)

        await self.db.upsert_topics(source_group_id, topics)
        return await self.db.list_topics(source_group_id)

    async def list_source_groups(self) -> list[dict[str, Any]]:
        return await self.db.list_source_groups()

    async def list_topics(self, source_group_id: int | None = None) -> list[dict[str, Any]]:
        return await self.db.list_topics(source_group_id)

    async def set_topic_enabled(self, source_group_id: int, topic_id: int, enabled: bool) -> None:
        await self.db.set_topic_enabled(source_group_id, topic_id, enabled)

    async def set_source_group_enabled(self, source_group_id: int, enabled: bool) -> None:
        await self.db.set_source_group_enabled(source_group_id, enabled)
