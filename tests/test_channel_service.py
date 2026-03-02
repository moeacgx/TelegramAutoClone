import sys
from types import ModuleType, SimpleNamespace

import pytest

if "qrcode" not in sys.modules:
    fake_qrcode = ModuleType("qrcode")
    fake_qrcode.make = lambda *_args, **_kwargs: None
    sys.modules["qrcode"] = fake_qrcode

from app.services.channel_service import ChannelService


class FakePermissions:
    def __init__(
        self,
        *,
        is_admin: bool = True,
        post_messages: bool | None = True,
        send_messages: bool | None = True,
    ) -> None:
        self.is_admin = is_admin
        self.post_messages = post_messages
        self.send_messages = send_messages


class FakeClient:
    def __init__(
        self,
        *,
        permissions: FakePermissions | None = None,
        error: Exception | None = None,
        get_entity_error: Exception | None = None,
        call_error: Exception | None = None,
        permissions_error: Exception | None = None,
        dialogs: list[object] | None = None,
    ) -> None:
        self.permissions = permissions or FakePermissions()
        self.error = error
        self.get_entity_error = get_entity_error
        self.call_error = call_error
        self.permissions_error = permissions_error
        self.dialogs = dialogs or []

    async def get_entity(self, channel_chat_id: int):
        if self.error is not None:
            raise self.error
        if self.get_entity_error is not None:
            raise self.get_entity_error
        return SimpleNamespace(title=f"channel-{channel_chat_id}")

    async def __call__(self, _request):
        if self.error is not None:
            raise self.error
        if self.call_error is not None:
            raise self.call_error
        return SimpleNamespace()

    async def get_permissions(self, _entity, _who: str):
        if self.error is not None:
            raise self.error
        if self.permissions_error is not None:
            raise self.permissions_error
        return self.permissions

    async def get_dialogs(self, limit: int = 2000):
        _ = limit
        return list(self.dialogs)


class FakeDB:
    def __init__(self) -> None:
        self.marked: list[tuple[int, str | None]] = []

    async def mark_channel_last_seen(self, chat_id: int, title: str | None = None) -> None:
        self.marked.append((chat_id, title))


class FakeTelegram:
    def __init__(self, *, bot_client: FakeClient, user_client: FakeClient) -> None:
        self.bot_client = bot_client
        self.user_client = user_client


@pytest.mark.asyncio
async def test_check_channel_access_requires_bot_admin() -> None:
    db = FakeDB()
    telegram = FakeTelegram(
        bot_client=FakeClient(permissions=FakePermissions(is_admin=False)),
        user_client=FakeClient(permissions=FakePermissions(is_admin=True)),
    )
    service = ChannelService(db, telegram)

    ok, error_text = await service.check_channel_access(-100123)

    assert not ok
    assert "Bot不是该频道管理员" in str(error_text)
    assert db.marked == []


@pytest.mark.asyncio
async def test_check_channel_access_requires_bot_send_permission() -> None:
    db = FakeDB()
    telegram = FakeTelegram(
        bot_client=FakeClient(
            permissions=FakePermissions(is_admin=True, post_messages=False, send_messages=False)
        ),
        user_client=FakeClient(permissions=FakePermissions(is_admin=True)),
    )
    service = ChannelService(db, telegram)

    ok, error_text = await service.check_channel_access(-100124)

    assert not ok
    assert "Bot在该频道缺少发送权限" in str(error_text)
    assert db.marked == []


@pytest.mark.asyncio
async def test_check_channel_access_user_failure_does_not_block_when_bot_ok() -> None:
    db = FakeDB()
    telegram = FakeTelegram(
        bot_client=FakeClient(permissions=FakePermissions(is_admin=True)),
        user_client=FakeClient(error=RuntimeError("user unavailable")),
    )
    service = ChannelService(db, telegram)

    ok, error_text = await service.check_channel_access(-100125)

    assert ok
    assert error_text is None
    assert db.marked == [(-100125, "channel--100125")]


@pytest.mark.asyncio
async def test_check_channel_access_entity_cache_miss_can_be_hydrated_from_dialogs() -> None:
    db = FakeDB()
    channel_chat_id = -1003483845368
    internal_channel_id = 3483845368
    cached_entity = SimpleNamespace(id=internal_channel_id, title="频道A")
    bot = FakeClient(
        permissions=FakePermissions(is_admin=True),
        get_entity_error=RuntimeError(
            "Could not find the input entity for PeerChannel(channel_id=3483845368) (PeerChannel)."
        ),
        dialogs=[SimpleNamespace(entity=cached_entity)],
    )
    user = FakeClient(permissions=FakePermissions(is_admin=True))
    telegram = FakeTelegram(bot_client=bot, user_client=user)
    service = ChannelService(db, telegram)

    ok, error_text = await service.check_channel_access(channel_chat_id)

    assert ok
    assert error_text is None
    assert db.marked == [(channel_chat_id, "频道A")]
