import asyncio
import base64
import io
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import qrcode
from telethon import TelegramClient
from telethon import errors as tg_errors

from app.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class PendingQRLogin:
    session_id: str
    created_at: datetime
    qr_login: Any


class TelegramManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        session_dir = Path(settings.sessions_dir)
        session_dir.mkdir(parents=True, exist_ok=True)

        self.user_client = TelegramClient(
            str(session_dir / "user"),
            settings.api_id,
            settings.api_hash,
        )
        self.bot_client = TelegramClient(
            str(session_dir / "bot"),
            settings.api_id,
            settings.api_hash,
        )

        self._pending_phone_hash: dict[str, str] = {}
        self._pending_qr: dict[str, PendingQRLogin] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        await self.user_client.connect()

        if self.settings.bot_token:
            try:
                await self.bot_client.start(bot_token=self.settings.bot_token)
                logger.info("Bot 客户端已启动")
            except Exception as exc:
                logger.error("Bot 客户端启动失败: %s", exc)
        else:
            await self.bot_client.connect()
            logger.warning("未配置 BOT_TOKEN，Bot 客户端仅连接未授权")

        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self.user_client.disconnect()
        await self.bot_client.disconnect()
        self._started = False

    async def ensure_user_connected(self) -> None:
        if not self.user_client.is_connected():
            await self.user_client.connect()

    async def ensure_bot_connected(self) -> None:
        if not self.bot_client.is_connected():
            await self.bot_client.connect()

    async def is_user_authorized(self) -> bool:
        await self.ensure_user_connected()
        return await self.user_client.is_user_authorized()

    async def is_bot_authorized(self) -> bool:
        await self.ensure_bot_connected()
        return await self.bot_client.is_user_authorized()

    async def get_auth_status(self) -> dict[str, Any]:
        user_authorized = await self.is_user_authorized()
        bot_authorized = await self.is_bot_authorized()
        user_me = await self.user_client.get_me() if user_authorized else None
        bot_me = await self.bot_client.get_me() if bot_authorized else None
        return {
            "user_authorized": user_authorized,
            "bot_authorized": bot_authorized,
            "user": {
                "id": user_me.id,
                "username": user_me.username,
                "first_name": user_me.first_name,
            }
            if user_me
            else None,
            "bot": {
                "id": bot_me.id,
                "username": bot_me.username,
                "first_name": bot_me.first_name,
            }
            if bot_me
            else None,
        }

    async def send_phone_code(self, phone: str) -> dict[str, Any]:
        await self.ensure_user_connected()
        result = await self.user_client.send_code_request(phone)
        self._pending_phone_hash[phone] = result.phone_code_hash
        return {"ok": True, "phone": phone}

    async def sign_in_with_code(self, phone: str, code: str, password: str | None = None) -> dict[str, Any]:
        await self.ensure_user_connected()
        phone_hash = self._pending_phone_hash.get(phone)
        try:
            if password:
                await self.user_client.sign_in(password=password)
            else:
                await self.user_client.sign_in(phone=phone, code=code, phone_code_hash=phone_hash)
            return {"ok": True, "need_password": False}
        except tg_errors.SessionPasswordNeededError:
            return {"ok": False, "need_password": True, "error": "账号开启了二步验证，请输入密码"}
        except tg_errors.PhoneCodeInvalidError:
            return {"ok": False, "need_password": False, "error": "验证码错误"}
        except tg_errors.PhoneCodeExpiredError:
            return {"ok": False, "need_password": False, "error": "验证码已过期，请重新发送"}

    async def create_qr_login(self) -> dict[str, Any]:
        await self.ensure_user_connected()
        qr_login = await self.user_client.qr_login()
        session_id = str(uuid.uuid4())
        self._pending_qr[session_id] = PendingQRLogin(
            session_id=session_id,
            created_at=datetime.utcnow(),
            qr_login=qr_login,
        )

        qr_url = qr_login.url
        img = qrcode.make(qr_url)
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        qr_image_base64 = base64.b64encode(buffer.getvalue()).decode("ascii")

        return {
            "ok": True,
            "session_id": session_id,
            "qr_url": qr_url,
            "qr_image_base64": qr_image_base64,
        }

    async def check_qr_login(self, session_id: str, timeout_seconds: int = 1) -> dict[str, Any]:
        pending = self._pending_qr.get(session_id)
        if not pending:
            return {"ok": False, "status": "expired", "error": "二维码会话不存在或已过期"}

        if pending.created_at < datetime.utcnow() - timedelta(minutes=5):
            self._pending_qr.pop(session_id, None)
            return {"ok": False, "status": "expired", "error": "二维码已过期，请重新生成"}

        try:
            await pending.qr_login.wait(timeout=timeout_seconds)
            self._pending_qr.pop(session_id, None)
            return {"ok": True, "status": "authorized"}
        except asyncio.TimeoutError:
            return {"ok": True, "status": "pending"}
        except Exception as exc:
            logger.error("扫码登录失败: %s", exc)
            return {"ok": False, "status": "failed", "error": str(exc)}

    @staticmethod
    def normalize_chat_ref(chat_ref: str | int) -> str | int:
        if isinstance(chat_ref, int):
            return chat_ref
        text = str(chat_ref).strip()
        if not text:
            raise ValueError("chat_ref 不能为空")

        lowered = text.lower()
        if "t.me/" in lowered:
            text = text.split("t.me/", 1)[1].strip("/")
            if "/" in text:
                text = text.split("/", 1)[0]

        if text.lstrip("-").isdigit():
            return int(text)

        if not text.startswith("@"):
            text = f"@{text}"
        return text

    async def resolve_chat(self, chat_ref: str | int, prefer_user: bool = True):
        normalized = self.normalize_chat_ref(chat_ref)
        if prefer_user:
            await self.ensure_user_connected()
            return await self.user_client.get_entity(normalized)
        await self.ensure_bot_connected()
        return await self.bot_client.get_entity(normalized)

    async def send_notification(self, message: str) -> None:
        if not self.settings.notify_chat_id:
            return
        await self.ensure_bot_connected()
        try:
            await self.bot_client.send_message(self.settings.notify_chat_id, message)
        except Exception as exc:
            logger.error("发送通知失败: %s", exc)

    async def cleanup(self) -> None:
        now = datetime.utcnow()
        expired = [
            sid for sid, data in self._pending_qr.items() if data.created_at < now - timedelta(minutes=5)
        ]
        for sid in expired:
            self._pending_qr.pop(sid, None)
