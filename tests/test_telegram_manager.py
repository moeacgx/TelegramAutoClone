import pytest

pytest.importorskip("qrcode")

from app.services.telegram_manager import TelegramManager


def test_normalize_chat_ref_from_tme_c_message_link() -> None:
    chat_ref = "https://t.me/c/3301983683/879/9606"
    normalized = TelegramManager.normalize_chat_ref(chat_ref)
    assert normalized == -1003301983683


def test_normalize_chat_ref_from_username_link() -> None:
    chat_ref = "https://t.me/example_group/123"
    normalized = TelegramManager.normalize_chat_ref(chat_ref)
    assert normalized == "@example_group"
