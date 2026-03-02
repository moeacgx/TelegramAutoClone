import sys
from types import ModuleType, SimpleNamespace

import pytest

if "qrcode" not in sys.modules:
    fake_qrcode = ModuleType("qrcode")
    fake_qrcode.make = lambda *_args, **_kwargs: None
    sys.modules["qrcode"] = fake_qrcode

from app.services.listener_service import ListenerService


class DummyDB:
    def __init__(self) -> None:
        self.banned_calls: list[dict] = []
        self.enqueue_calls: list[dict] = []

    async def get_source_group_by_chat_id(self, chat_id: int):
        return {"id": 3, "enabled": 1, "title": "源群"}

    async def get_topic(self, source_group_id: int, topic_id: int):
        return {"enabled": 1, "title": f"话题{topic_id}"}

    async def get_binding(self, source_group_id: int, topic_id: int):
        return {"active": 1, "channel_chat_id": -100123456}

    async def add_banned_channel(self, **kwargs):
        self.banned_calls.append(kwargs)

    async def enqueue_recovery_with_status(self, **kwargs):
        self.enqueue_calls.append(kwargs)
        return 88, True

    async def get_channel(self, channel_chat_id: int):
        return {"title": "目标频道"}


class DummyTelegram:
    def __init__(self) -> None:
        self.notifications: list[str] = []
        self.user_client = SimpleNamespace()

    async def send_notification(self, message: str) -> None:
        self.notifications.append(message)


class DummyCloneService:
    def __init__(self, *, raise_exc: Exception | None = None, clone_result: bool = True) -> None:
        self.raise_exc = raise_exc
        self.clone_result = clone_result
        self.calls: list[dict] = []

    @staticmethod
    def extract_topic_id(message) -> int | None:
        return None

    async def clone_message_no_reference(self, **kwargs) -> bool:
        self.calls.append(kwargs)
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.clone_result


def build_event(text: str, message_id: int = 9527):
    msg = SimpleNamespace(
        id=message_id,
        message=text,
        text=text,
        media=None,
        action=None,
        deleted=False,
    )
    return SimpleNamespace(chat_id=-100300000001, message=msg)


@pytest.mark.asyncio
async def test_clone_send_error_enqueues_recovery_and_notifies():
    db = DummyDB()
    tg = DummyTelegram()
    clone = DummyCloneService(raise_exc=RuntimeError("CHAT_WRITE_FORBIDDEN"))
    service = ListenerService(db, tg, clone)

    await service.on_new_message(build_event("普通文本消息"))

    assert len(clone.calls) == 1
    assert clone.calls[0]["raise_on_send_error"] is True
    assert len(db.banned_calls) == 1
    assert len(db.enqueue_calls) == 1
    assert len(tg.notifications) == 1
    assert "实时克隆发现频道失效" in tg.notifications[0]
