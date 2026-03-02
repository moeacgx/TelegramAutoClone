import sys
from types import ModuleType, SimpleNamespace

import pytest

if "qrcode" not in sys.modules:
    fake_qrcode = ModuleType("qrcode")
    fake_qrcode.make = lambda *_args, **_kwargs: None
    sys.modules["qrcode"] = fake_qrcode

from app.services.channel_service import ChannelService


class FakeDB:
    def __init__(self) -> None:
        self.marked: list[tuple[int, str | None]] = []

    async def mark_channel_last_seen(self, chat_id: int, title: str | None = None) -> None:
        self.marked.append((chat_id, title))


class FakeTelegram:
    def __init__(self, bot_token: str = "fake-token") -> None:
        self.settings = SimpleNamespace(bot_token=bot_token)
        self.bot_client = SimpleNamespace()


@pytest.mark.asyncio
async def test_check_channel_access_send_and_delete_success() -> None:
    db = FakeDB()
    telegram = FakeTelegram()
    service = ChannelService(db, telegram)

    async def fake_bot_api_request(method: str, payload: dict, timeout_seconds: int = 12):
        if method == "sendMessage":
            assert payload["chat_id"] == -100123
            return {"message_id": 9527, "chat": {"title": "频道A"}}
        if method == "deleteMessage":
            assert payload["chat_id"] == -100123
            assert payload["message_id"] == 9527
            return True
        raise AssertionError(f"unexpected method: {method}")

    service._bot_api_request = fake_bot_api_request  # type: ignore[method-assign]

    ok, error_text = await service.check_channel_access(-100123)

    assert ok
    assert error_text is None
    assert db.marked == [(-100123, "频道A")]


@pytest.mark.asyncio
async def test_check_channel_access_delete_failure_does_not_mark_unavailable() -> None:
    db = FakeDB()
    telegram = FakeTelegram()
    service = ChannelService(db, telegram)

    async def fake_bot_api_request(method: str, payload: dict, timeout_seconds: int = 12):
        if method == "sendMessage":
            return {"message_id": 9528, "chat": {"title": "频道B"}}
        if method == "deleteMessage":
            raise RuntimeError("message can't be deleted")
        raise AssertionError(f"unexpected method: {method}")

    service._bot_api_request = fake_bot_api_request  # type: ignore[method-assign]

    ok, error_text = await service.check_channel_access(-100124)

    assert ok
    assert error_text is None
    assert db.marked == [(-100124, "频道B")]


@pytest.mark.asyncio
async def test_check_channel_access_send_failure_marks_unavailable() -> None:
    db = FakeDB()
    telegram = FakeTelegram()
    service = ChannelService(db, telegram)

    async def fake_bot_api_request(method: str, payload: dict, timeout_seconds: int = 12):
        if method == "sendMessage":
            raise RuntimeError("Forbidden: bot is not a member of the channel chat")
        raise AssertionError(f"unexpected method: {method}")

    service._bot_api_request = fake_bot_api_request  # type: ignore[method-assign]

    ok, error_text = await service.check_channel_access(-100125)

    assert not ok
    assert "Bot 不在该频道里" in str(error_text)
    assert db.marked == []


@pytest.mark.asyncio
async def test_check_channel_access_rate_limit_message() -> None:
    db = FakeDB()
    telegram = FakeTelegram()
    service = ChannelService(db, telegram)

    async def fake_bot_api_request(method: str, payload: dict, timeout_seconds: int = 12):
        if method == "sendMessage":
            raise RuntimeError("Too Many Requests: retry after 20")
        raise AssertionError(f"unexpected method: {method}")

    service._bot_api_request = fake_bot_api_request  # type: ignore[method-assign]

    ok, error_text = await service.check_channel_access(-100126)

    assert not ok
    assert "Bot请求过于频繁" in str(error_text)
    assert db.marked == []
