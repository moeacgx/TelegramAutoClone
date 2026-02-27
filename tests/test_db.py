import pytest

from app.db import Database


@pytest.mark.asyncio
async def test_queue_deduplicate(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    sg = await db.add_or_update_source_group(chat_id=-1001, title="sg")
    await db.upsert_topics(
        sg["id"],
        [
            {"topic_id": 10, "title": "topic-10"},
        ],
    )

    queue_id_1 = await db.enqueue_recovery(
        source_group_id=sg["id"],
        topic_id=10,
        old_channel_chat_id=-1002,
        reason="x",
    )
    queue_id_2 = await db.enqueue_recovery(
        source_group_id=sg["id"],
        topic_id=10,
        old_channel_chat_id=-1002,
        reason="y",
    )

    assert queue_id_1 == queue_id_2


@pytest.mark.asyncio
async def test_binding_upsert(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    sg = await db.add_or_update_source_group(chat_id=-10011, title="sg")
    await db.upsert_topics(
        sg["id"],
        [
            {"topic_id": 100, "title": "topic-100"},
        ],
    )

    await db.upsert_channel(chat_id=-10021, title="c1", is_standby=True)
    binding = await db.upsert_binding(sg["id"], 100, -10021)

    assert binding["channel_chat_id"] == -10021
    standby = await db.list_standby_channels()
    assert standby == []
