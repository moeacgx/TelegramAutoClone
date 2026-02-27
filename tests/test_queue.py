import pytest

from app.db import Database


@pytest.mark.asyncio
async def test_recovery_claim_and_retry(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    sg = await db.add_or_update_source_group(chat_id=-10030, title="sg")
    await db.upsert_topics(sg["id"], [{"topic_id": 200, "title": "topic"}])

    queue_id = await db.enqueue_recovery(
        source_group_id=sg["id"],
        topic_id=200,
        old_channel_chat_id=-10040,
        reason="fail",
    )

    claimed = await db.claim_next_recovery()
    assert claimed is not None
    assert claimed["id"] == queue_id
    assert claimed["status"] == "running"
    assert claimed["last_cloned_message_id"] == 0

    await db.mark_recovery_failed(queue_id, retry_count=0, error_text="err", max_retry=3)
    row = [x for x in await db.list_recovery_queue() if x["id"] == queue_id][0]
    assert row["status"] == "pending"
    assert row["retry_count"] == 1

    claimed2 = await db.claim_next_recovery()
    assert claimed2 is not None
    await db.mark_recovery_failed(queue_id, retry_count=2, error_text="err2", max_retry=3)
    row2 = [x for x in await db.list_recovery_queue() if x["id"] == queue_id][0]
    assert row2["status"] == "failed"


@pytest.mark.asyncio
async def test_recovery_checkpoint_and_done(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.init()

    sg = await db.add_or_update_source_group(chat_id=-10031, title="sg2")
    await db.upsert_topics(sg["id"], [{"topic_id": 201, "title": "topic2"}])

    queue_id = await db.enqueue_recovery(
        source_group_id=sg["id"],
        topic_id=201,
        old_channel_chat_id=-10041,
        reason="fail",
    )

    claimed = await db.claim_next_recovery()
    assert claimed is not None

    await db.mark_recovery_assigned_channel(queue_id, -10051)
    await db.update_recovery_progress(queue_id, 12345)
    await db.mark_recovery_done(
        queue_id=queue_id,
        new_channel_chat_id=-10051,
        summary="ok",
        last_cloned_message_id=12345,
    )

    row = [x for x in await db.list_recovery_queue() if x["id"] == queue_id][0]
    assert row["status"] == "done"
    assert row["new_channel_chat_id"] == -10051
    assert row["last_cloned_message_id"] == 12345
