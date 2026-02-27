import asyncio
import base64
import io
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import qrcode
from telethon import TelegramClient
from telethon import errors as tg_errors
from telethon.sessions import SQLiteSession

from app.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class PendingQRLogin:
    session_id: str
    created_at: datetime
    qr_login: Any
    need_password: bool = False
    status: str = "pending"
    error: str | None = None
    relogin_required: bool = False
    wait_task: asyncio.Task[Any] | None = None


class TelegramManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._session_dir = Path(settings.sessions_dir)
        self._session_dir.mkdir(parents=True, exist_ok=True)
        self._user_session_name = str(self._session_dir / "user")
        self._bot_session_name = str(self._session_dir / "bot")

        self.user_client = TelegramClient(
            self._user_session_name,
            settings.api_id,
            settings.api_hash,
        )
        self.bot_client = TelegramClient(
            self._bot_session_name,
            settings.api_id,
            settings.api_hash,
        )

        self._pending_phone_hash: dict[str, str] = {}
        self._pending_qr: dict[str, PendingQRLogin] = {}
        self._user_session_lock = asyncio.Lock()
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
        if self.user_client.is_connected():
            return

        try:
            await self.user_client.connect()
        except Exception as exc:
            if not self._is_broken_session_storage(exc):
                raise
            logger.warning("检测到会话存储损坏，准备自动重建 user.session: %s", exc)
            await self.reset_user_session()

    async def ensure_bot_connected(self) -> None:
        if not self.bot_client.is_connected():
            await self.bot_client.connect()

    async def is_user_authorized(self) -> bool:
        await self.ensure_user_connected()
        return await self.user_client.is_user_authorized()

    async def is_bot_authorized(self) -> bool:
        await self.ensure_bot_connected()
        return await self.bot_client.is_user_authorized()

    @staticmethod
    def _is_broken_session_storage(exc: Exception) -> bool:
        if isinstance(exc, sqlite3.OperationalError):
            text = str(exc).lower()
            return (
                "no such table: sessions" in text
                or "file is not a database" in text
                or "database disk image is malformed" in text
            )
        text = str(exc).lower()
        return "no such table: sessions" in text

    async def _drop_pending_qr(self, session_id: str) -> None:
        pending = self._pending_qr.pop(session_id, None)
        if not pending:
            return
        task = pending.wait_task
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _clear_pending_qr(self) -> None:
        tasks: list[asyncio.Task[Any]] = []
        for pending in self._pending_qr.values():
            task = pending.wait_task
            if task and not task.done():
                task.cancel()
                tasks.append(task)
        self._pending_qr.clear()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch_qr_login(self, session_id: str) -> None:
        pending = self._pending_qr.get(session_id)
        if not pending:
            return
        try:
            await pending.qr_login.wait()
            pending.status = "authorized"
            pending.need_password = False
            pending.error = None
        except tg_errors.SessionPasswordNeededError:
            pending.status = "need_password"
            pending.need_password = True
            pending.error = "该账号开启了二级密码，请输入二级密码后登录"
        except asyncio.TimeoutError:
            pending.status = "expired"
            pending.error = "二维码已过期，请重新生成"
        except tg_errors.AuthKeyUnregisteredError:
            pending.status = "expired"
            pending.relogin_required = True
            pending.error = "当前登录会话已失效，请重新生成二维码"
        except Exception as exc:
            if self._is_broken_session_storage(exc):
                pending.status = "expired"
                pending.relogin_required = True
                pending.error = "本地会话存储异常，请重新生成二维码"
            else:
                pending.status = "failed"
                pending.error = str(exc)
            logger.error("扫码登录监听异常(session=%s): %s", session_id, exc)

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
        except tg_errors.AuthKeyUnregisteredError:
            return {
                "ok": False,
                "need_password": False,
                "relogin_required": True,
                "error": "当前会话已失效，请重新发送验证码或重新扫码登录",
            }
        except tg_errors.SessionPasswordNeededError:
            return {"ok": False, "need_password": True, "error": "账号开启了二步验证，请输入密码"}
        except tg_errors.PhoneCodeInvalidError:
            return {"ok": False, "need_password": False, "error": "验证码错误"}
        except tg_errors.PhoneCodeExpiredError:
            return {"ok": False, "need_password": False, "error": "验证码已过期，请重新发送"}
        except Exception as exc:
            if not self._is_broken_session_storage(exc):
                raise
            await self.reset_user_session()
            return {
                "ok": False,
                "need_password": False,
                "relogin_required": True,
                "error": "本地会话存储异常，已自动重置，请重新扫码或重新发送验证码",
            }

    async def sign_in_with_password_only(self, password: str, session_id: str | None = None) -> dict[str, Any]:
        await self.ensure_user_connected()
        if not password:
            return {"ok": False, "error": "二级密码不能为空"}

        if not session_id:
            return {
                "ok": False,
                "need_password": True,
                "error": "请先生成二维码并扫码，再提交二级密码",
            }

        pending = self._pending_qr.get(session_id)
        if not pending:
            return {
                "ok": False,
                "relogin_required": True,
                "error": "二维码会话不存在或已过期，请重新生成二维码",
            }

        if pending.created_at < datetime.utcnow() - timedelta(minutes=5):
            await self._drop_pending_qr(session_id)
            return {
                "ok": False,
                "relogin_required": True,
                "error": "二维码已过期，请重新生成",
            }

        if pending.status == "pending" and pending.wait_task:
            # 监听任务在二维码生成时已启动；这里等待结果，避免用户手动检查扫码状态。
            try:
                await asyncio.wait_for(asyncio.shield(pending.wait_task), timeout=20)
            except asyncio.TimeoutError:
                return {
                    "ok": False,
                    "need_password": True,
                    "error": "20秒内未检测到扫码确认，请先在手机上完成扫码并确认登录后再提交二级密码",
                }

        if pending.status == "authorized":
            await self._drop_pending_qr(session_id)
            return {"ok": True, "need_password": False}

        if pending.status in {"expired", "failed"}:
            error_message = pending.error or "二维码会话已失效，请重新生成二维码"
            relogin_required = pending.relogin_required or pending.status == "expired"
            await self._drop_pending_qr(session_id)
            return {
                "ok": False,
                "need_password": False,
                "relogin_required": relogin_required,
                "error": error_message,
            }

        if pending.status != "need_password":
            return {
                "ok": False,
                "need_password": True,
                "error": "尚未完成扫码确认，请先在手机完成确认后再提交二级密码",
            }

        try:
            await self.user_client.sign_in(password=password)
            await self._drop_pending_qr(session_id)
            return {"ok": True, "need_password": False}
        except tg_errors.AuthKeyUnregisteredError:
            # 会话密钥已失效（常见于容器重建/会话文件损坏/登录流程超时），自动清理旧会话。
            await self.reset_user_session()
            return {
                "ok": False,
                "need_password": False,
                "relogin_required": True,
                "error": "登录会话已失效，请重新生成二维码并扫码，或重新发送验证码登录",
            }
        except tg_errors.PasswordHashInvalidError:
            return {"ok": False, "need_password": True, "error": "二级密码错误"}
        except Exception as exc:
            if not self._is_broken_session_storage(exc):
                raise
            await self.reset_user_session()
            return {
                "ok": False,
                "need_password": False,
                "relogin_required": True,
                "error": "本地会话存储异常，已自动重置，请重新扫码登录",
            }

    async def reset_user_session(self) -> None:
        async with self._user_session_lock:
            self._pending_phone_hash.clear()
            await self._clear_pending_qr()

            try:
                if self.user_client.is_connected():
                    await self.user_client.disconnect()
            except Exception as exc:
                logger.warning("断开用户客户端失败: %s", exc)

            try:
                self.user_client.session.close()
            except Exception:
                pass

            try:
                self.user_client.session.delete()
            except Exception as exc:
                logger.warning("删除 Telethon 会话失败: %s", exc)

            session_base = Path(f"{self._user_session_name}.session")
            for suffix in ("", "-journal", "-wal", "-shm"):
                path = Path(f"{session_base}{suffix}")
                try:
                    if path.exists():
                        path.unlink()
                except Exception as exc:
                    logger.warning("删除会话文件失败(%s): %s", path, exc)

            # 保留同一个 TelegramClient 实例，重置其 session 存储，避免事件处理器丢失。
            self.user_client.session = SQLiteSession(self._user_session_name)
            await self.user_client.connect()

    async def create_qr_login(self) -> dict[str, Any]:
        # 每次重新生成二维码都自动重置旧 user 会话，避免手动删除 session 文件。
        await self.reset_user_session()
        try:
            qr_login = await self.user_client.qr_login()
        except Exception as exc:
            if not self._is_broken_session_storage(exc):
                raise
            await self.reset_user_session()
            qr_login = await self.user_client.qr_login()
        session_id = str(uuid.uuid4())
        pending = PendingQRLogin(
            session_id=session_id,
            created_at=datetime.utcnow(),
            qr_login=qr_login,
            need_password=False,
        )
        self._pending_qr[session_id] = pending
        pending.wait_task = asyncio.create_task(self._watch_qr_login(session_id))

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
            await self._drop_pending_qr(session_id)
            return {"ok": False, "status": "expired", "error": "二维码已过期，请重新生成"}

        if pending.status == "pending" and pending.wait_task:
            try:
                await asyncio.wait_for(asyncio.shield(pending.wait_task), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                pass

        if pending.status == "authorized":
            await self._drop_pending_qr(session_id)
            return {"ok": True, "status": "authorized"}

        if pending.status == "pending":
            return {"ok": True, "status": "pending"}

        if pending.status == "need_password":
            return {
                "ok": False,
                "status": "need_password",
                "need_password": True,
                "session_id": session_id,
                "error": pending.error or "该账号开启了二级密码，请输入二级密码后登录",
            }

        if pending.status == "expired":
            error_message = pending.error or "当前登录会话已失效，请重新生成二维码"
            await self._drop_pending_qr(session_id)
            return {
                "ok": False,
                "status": "expired",
                "relogin_required": pending.relogin_required,
                "error": error_message,
            }

        await self._drop_pending_qr(session_id)
        return {"ok": False, "status": "failed", "error": pending.error or "扫码登录失败"}

    @staticmethod
    def normalize_chat_ref(chat_ref: str | int) -> str | int:
        if isinstance(chat_ref, int):
            return chat_ref
        text = str(chat_ref).strip()
        if not text:
            raise ValueError("chat_ref 不能为空")

        lowered = text.lower()
        if "t.me/" in lowered:
            link_path = text.split("t.me/", 1)[1].strip("/")
            if "?" in link_path:
                link_path = link_path.split("?", 1)[0]

            # 支持私密消息链接: https://t.me/c/<internal_id>/<topic_id>/<msg_id>
            # 其中 internal_id 对应超级群/频道的无 -100 前缀 ID。
            if link_path.startswith("c/"):
                parts = link_path.split("/")
                if len(parts) >= 2 and parts[1].isdigit():
                    return int(f"-100{parts[1]}")

            text = link_path
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
            await self._drop_pending_qr(sid)
