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


@pytest.mark.asyncio
async def test_banned_and_recovery_list_include_titles(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    sg = await db.add_or_update_source_group(chat_id=-100111, title="源任务组")
    await db.upsert_topics(
        sg["id"],
        [
            {"topic_id": 1069, "title": "话题名称"},
        ],
    )
    await db.upsert_channel(chat_id=-1003483845368, title="旧频道名称", is_standby=False, in_use=True)
    await db.upsert_channel(chat_id=-1003999999999, title="新频道名称", is_standby=False, in_use=True)

    await db.add_banned_channel(
        source_group_id=sg["id"],
        topic_id=1069,
        channel_chat_id=-1003483845368,
        reason="x",
    )

    queue_id = await db.enqueue_recovery(
        source_group_id=sg["id"],
        topic_id=1069,
        old_channel_chat_id=-1003483845368,
        reason="y",
    )
    await db.mark_recovery_assigned_channel(queue_id, -1003999999999)

    banned = await db.list_banned_channels()
    assert banned[0]["source_title"] == "源任务组"
    assert banned[0]["topic_title"] == "话题名称"
    assert banned[0]["channel_title"] == "旧频道名称"

    queue = await db.list_recovery_queue()
    assert queue[0]["source_title"] == "源任务组"
    assert queue[0]["topic_title"] == "话题名称"
    assert queue[0]["old_channel_title"] == "旧频道名称"
    assert queue[0]["new_channel_title"] == "新频道名称"
