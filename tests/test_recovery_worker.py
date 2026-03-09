from types import SimpleNamespace

import pytest

from app.services.recovery_worker import RecoveryWorker


class DummyDB:
    def __init__(self):
        self.progress_updates = []
        self.done_calls = []
        self.deleted_queue_ids = []
        self.removed_banned = []
        self.assigned_channel_calls = []

    async def claim_next_recovery(self):
        return {
            "id": 41,
            "source_group_id": 8,
            "topic_id": 99,
            "old_channel_chat_id": -100300,
            "new_channel_chat_id": -100400,
            "retry_count": 0,
            "last_cloned_message_id": 0,
        }

    async def claim_recovery_by_id(self, _queue_id: int):
        return None

    async def get_source_group_by_id(self, source_group_id: int):
        return {"id": source_group_id, "title": "源任务组", "chat_id": -100200}

    async def get_topic(self, source_group_id: int, topic_id: int):
        return {"source_group_id": source_group_id, "topic_id": topic_id, "title": "话题名称", "avatar_path": ""}

    async def get_channel(self, channel_chat_id: int):
        return {"title": f"频道{channel_chat_id}"}

    async def is_recovery_stop_requested(self, _queue_id: int):
        return False

    async def update_recovery_progress(self, queue_id: int, last_cloned_message_id: int):
        self.progress_updates.append((queue_id, last_cloned_message_id))

    async def mark_recovery_done(self, **kwargs):
        self.done_calls.append(kwargs)

    async def remove_banned_channel(self, **kwargs):
        self.removed_banned.append(kwargs)

    async def delete_recovery_task(self, queue_id: int):
        self.deleted_queue_ids.append(queue_id)
        return True

    async def mark_recovery_assigned_channel(self, queue_id: int, new_channel_chat_id: int):
        self.assigned_channel_calls.append((queue_id, new_channel_chat_id))


class DummyTelegram:
    def __init__(self):
        self.notifications = []
        self.bot_client = SimpleNamespace()

    async def send_notification(self, message: str):
        self.notifications.append(message)


class DummyCloneService:
    def __init__(self):
        self.calls = []

    async def clone_topic_history(self, **kwargs):
        self.calls.append(kwargs)
        await kwargs["progress_hook"](321)
        return {
            "total": 3,
            "cloned": 3,
            "skipped": 0,
            "started_min_id": 99,
            "last_cloned_message_id": 321,
        }


class DummyChannelService:
    def __init__(self):
        self.calls = []

    async def apply_topic_profile(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.asyncio
async def test_recovery_worker_calls_clone_service_and_updates_checkpoint():
    db = DummyDB()
    telegram = DummyTelegram()
    clone_service = DummyCloneService()
    channel_service = DummyChannelService()
    settings = SimpleNamespace(recovery_max_retry=3)
    worker = RecoveryWorker(db, telegram, clone_service, channel_service, settings)

    processed = await worker.run_once()

    assert processed is True
    assert len(clone_service.calls) == 1
    assert clone_service.calls[0]["source_chat_id"] == -100200
    assert clone_service.calls[0]["topic_id"] == 99
    assert clone_service.calls[0]["target_channel"] == -100400
    assert db.progress_updates == [(41, 321)]
    assert db.done_calls[0]["last_cloned_message_id"] == 321
    assert db.deleted_queue_ids == [41]
    assert len(channel_service.calls) == 1
    assert any("频道恢复完成" in message for message in telegram.notifications)
