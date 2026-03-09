from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import DocumentAttributeVideo, InputFile, InputMediaUploadedDocument, InputMediaUploadedPhoto

from app.services.clone_service import CloneService


class DummyDB:
    def __init__(self, *, md5_enabled: bool, concurrency: int = 2):
        self.values = {
            "clone.md5_mutation_enabled": "1" if md5_enabled else "0",
            "clone.download_group_concurrency": str(concurrency),
        }

    async def get_setting(self, key: str):
        return self.values.get(key)


class DummyBotClient:
    def __init__(self, *, fail_direct_file=None):
        self.fail_direct_file = fail_direct_file
        self.direct_failed = False
        self.forward_calls = []
        self.send_file_calls = []
        self.send_message_calls = []
        self.upload_calls = []
        self._file_id = 10
        self._message_id = 1000

    async def forward_messages(self, **kwargs):
        self.forward_calls.append(kwargs)
        return True

    async def send_file(self, **kwargs):
        self.send_file_calls.append(kwargs)
        if self.fail_direct_file is not None and kwargs.get("file") is self.fail_direct_file and not self.direct_failed:
            self.direct_failed = True
            raise RuntimeError("direct send failed")
        self._message_id += 1
        return SimpleNamespace(id=self._message_id)

    async def send_message(self, **kwargs):
        self.send_message_calls.append(kwargs)
        self._message_id += 1
        return SimpleNamespace(id=self._message_id)

    async def upload_file(self, file):
        self.upload_calls.append(file)
        self._file_id += 1
        return InputFile(self._file_id, 1, Path(str(file)).name, "0" * 32)


class DummyUserClient:
    def __init__(self, *, history_messages=None, group_messages=None):
        self.history_messages = list(history_messages or [])
        self.group_messages = list(group_messages or [])
        self.download_calls = []

    async def download_media(self, message, file, thumb=None):
        self.download_calls.append({"message_id": int(message.id), "file": file, "thumb": thumb})
        base_dir = Path(str(file))
        base_dir.mkdir(parents=True, exist_ok=True)
        if thumb == -1:
            thumb_path = base_dir / f"thumb_{int(message.id)}.jpg"
            thumb_path.write_bytes(b"thumb")
            return str(thumb_path)

        suffix = ".jpg" if getattr(message, "photo", None) or getattr(getattr(message, "media", None), "photo", None) else ".mp4" if getattr(message, "video", False) else ".bin"
        media_path = base_dir / f"media_{int(message.id)}{suffix}"
        media_path.write_bytes(f"data-{int(message.id)}".encode("utf-8"))
        return str(media_path)

    async def iter_messages(self, _source_chat_id, reverse=False, min_id=0, **_kwargs):
        messages = self.history_messages if reverse else self.group_messages
        for message in messages:
            if int(getattr(message, "id", 0) or 0) > int(min_id or 0):
                yield message


def build_message(
    message_id: int,
    *,
    text: str = "",
    entities=None,
    media=None,
    grouped_id: int | None = None,
    video: bool = False,
    photo: bool = False,
    reply_to=None,
):
    media_obj = media
    photo_marker = object() if photo else None
    if photo and media_obj is None:
        media_obj = SimpleNamespace(photo=photo_marker)
    return SimpleNamespace(
        id=message_id,
        message=text,
        text=text,
        media=media_obj,
        entities=entities,
        grouped_id=grouped_id,
        video=video,
        action=None,
        deleted=False,
        photo=photo_marker,
        reply_to=reply_to,
    )


@pytest.mark.asyncio
async def test_text_message_keeps_formatting_entities():
    db = DummyDB(md5_enabled=True)
    bot = DummyBotClient()
    tg = SimpleNamespace(bot_client=bot, user_client=DummyUserClient())
    service = CloneService(tg, db)
    entities = [SimpleNamespace(type="bold")]
    message = build_message(1, text="hello", entities=entities)

    ok = await service.clone_message_no_reference(message, -100123, source_chat_id=-100888)

    assert ok is True
    assert bot.forward_calls == []
    assert len(bot.send_message_calls) == 1
    assert bot.send_message_calls[0]["formatting_entities"] is entities


@pytest.mark.asyncio
async def test_media_with_toggle_off_prefers_direct_send_without_download():
    raw_media = object()
    db = DummyDB(md5_enabled=False)
    bot = DummyBotClient()
    user = DummyUserClient()
    tg = SimpleNamespace(bot_client=bot, user_client=user)
    service = CloneService(tg, db)
    message = build_message(2, text="caption", entities=[SimpleNamespace()], media=raw_media)

    ok = await service.clone_message_no_reference(message, -100123)

    assert ok is True
    assert len(bot.send_file_calls) == 1
    assert bot.send_file_calls[0]["file"] is raw_media
    assert user.download_calls == []


@pytest.mark.asyncio
async def test_media_direct_send_failure_falls_back_to_download_upload_and_keeps_video_attrs():
    video_attr = DocumentAttributeVideo(duration=3, w=1280, h=720, round_message=False, supports_streaming=False)
    document = SimpleNamespace(mime_type="video/mp4", attributes=[video_attr])
    media = SimpleNamespace(document=document)
    db = DummyDB(md5_enabled=False)
    bot = DummyBotClient(fail_direct_file=media)
    user = DummyUserClient()
    tg = SimpleNamespace(bot_client=bot, user_client=user)
    service = CloneService(tg, db)
    message = build_message(3, text="video-caption", entities=[SimpleNamespace()], media=media, video=True)

    ok = await service.clone_message_no_reference(message, -100123)

    assert ok is True
    assert len(user.download_calls) >= 1
    assert len(bot.send_file_calls) == 2
    uploaded_media = bot.send_file_calls[-1]["file"]
    assert isinstance(uploaded_media, InputMediaUploadedDocument)
    assert uploaded_media.mime_type == "video/mp4"
    assert uploaded_media.thumb is not None
    assert any(isinstance(attr, DocumentAttributeVideo) and attr.supports_streaming for attr in uploaded_media.attributes)


@pytest.mark.asyncio
async def test_media_with_toggle_on_uses_download_and_md5_mutation(monkeypatch, tmp_path: Path):
    db = DummyDB(md5_enabled=True)
    bot = DummyBotClient()
    user = DummyUserClient()
    tg = SimpleNamespace(bot_client=bot, user_client=user)
    service = CloneService(tg, db)
    media = SimpleNamespace(photo=object())
    message = build_message(4, text="image-caption", entities=[SimpleNamespace()], media=media, photo=True)
    mutate_calls = []

    def fake_mutate(file_path: str, mime_type: str | None, workdir: str) -> str:
        mutate_calls.append((file_path, mime_type, workdir))
        new_path = Path(workdir) / "mutated.jpg"
        new_path.write_bytes(Path(file_path).read_bytes() + b"-mutated")
        return str(new_path)

    monkeypatch.setattr("app.services.clone_service.mutate_media_file_md5", fake_mutate)

    ok = await service.clone_message_no_reference(message, -100123, source_chat_id=-100888)

    assert ok is True
    assert bot.forward_calls == []
    assert len(user.download_calls) >= 1
    assert len(mutate_calls) == 1
    assert isinstance(bot.send_file_calls[0]["file"], InputMediaUploadedPhoto)


@pytest.mark.asyncio
async def test_clone_topic_history_preserves_album_order_and_text_after_album(monkeypatch):
    db = DummyDB(md5_enabled=True, concurrency=2)
    reply_to = SimpleNamespace(reply_to_top_id=100, forum_topic=False)
    album_1 = build_message(101, text="相册第一条", entities=[SimpleNamespace()], grouped_id=7, media=SimpleNamespace(photo=object()), photo=True, reply_to=reply_to)
    album_2 = build_message(102, text="", entities=[], grouped_id=7, media=SimpleNamespace(photo=object()), photo=True, reply_to=reply_to)
    text_message = build_message(103, text="普通文本", entities=[SimpleNamespace()], reply_to=reply_to)
    user = DummyUserClient(history_messages=[album_1, album_2, text_message], group_messages=[album_1, album_2])
    bot = DummyBotClient()
    tg = SimpleNamespace(bot_client=bot, user_client=user)
    service = CloneService(tg, db)
    progress_updates = []

    monkeypatch.setattr("app.services.clone_service.mutate_media_file_md5", lambda file_path, _mime_type, _workdir: file_path)

    result = await service.clone_topic_history(
        source_chat_id=-100001,
        topic_id=100,
        target_channel=-100777,
        progress_hook=lambda checkpoint: _collect_progress(progress_updates, checkpoint),
    )

    assert result["cloned"] == 3
    assert result["last_cloned_message_id"] == 103
    assert len(bot.send_file_calls) == 1
    album_call = bot.send_file_calls[0]
    assert isinstance(album_call["file"], list)
    assert len(album_call["file"]) == 2
    assert all(isinstance(item, InputMediaUploadedPhoto) for item in album_call["file"])
    assert album_call["caption"] == ["相册第一条", ""]
    assert len(bot.send_message_calls) == 1
    assert bot.send_message_calls[0]["message"] == "普通文本"
    assert progress_updates[-1] == 103


async def _collect_progress(container: list[int], checkpoint: int) -> None:
    container.append(checkpoint)
