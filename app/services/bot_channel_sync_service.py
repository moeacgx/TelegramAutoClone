import asyncio
import json
from datetime import datetime, timezone
from typing import Any
from urllib import request as urlrequest

from app.config import Settings
from app.db import Database


class BotChannelSyncService:
    """通过 Bot API 的 my_chat_member 更新，实时同步 Bot 管理频道到本地频道库。"""

    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self._offset_key = "bot_updates_offset"

    async def sync_once(self, timeout_seconds: int = 20) -> dict[str, Any]:
        if not self.settings.bot_token:
            return {"ok": False, "warning": "BOT_TOKEN 未配置，跳过 Bot 频道同步"}

        offset_value = await self.db.get_setting(self._offset_key)
        if offset_value is None or str(offset_value).strip() in {"", "0"}:
            # 首次启动不回放历史更新，直接快进到当前最新 offset，避免旧事件回灌。
            latest = await self._fetch_updates(offset=-1, timeout_seconds=0)
            next_offset = 0
            for item in latest:
                update_id = int(item.get("update_id", 0))
                if update_id >= next_offset:
                    next_offset = update_id + 1
            await self.db.set_setting(self._offset_key, str(next_offset))
            return {"ok": True, "bootstrap": True, "received": 0, "tracked_channels": 0}

        offset = int(offset_value) if offset_value.isdigit() else 0

        updates = await self._fetch_updates(offset=offset, timeout_seconds=timeout_seconds)
        if not updates:
            return {"ok": True, "received": 0, "tracked_channels": 0}

        max_update_id = offset
        tracked_channels = 0
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        for item in updates:
            update_id = int(item.get("update_id", 0))
            if update_id >= max_update_id:
                max_update_id = update_id + 1

            payload = item.get("my_chat_member")
            if not payload:
                continue

            chat = payload.get("chat") or {}
            if chat.get("type") != "channel":
                continue

            chat_id = int(chat.get("id", 0))
            if not chat_id:
                continue

            title = (chat.get("title") or str(chat_id)).strip()
            status = ((payload.get("new_chat_member") or {}).get("status") or "").lower()
            active_bindings = await self.db.get_binding_by_channel(chat_id)
            is_admin = status in {"administrator", "creator"}
            is_left = status in {"left", "kicked"}

            if (is_left or not is_admin) and not active_bindings:
                await self.db.delete_channel(chat_id)
                tracked_channels += 1
                continue

            await self.db.upsert_channel(
                chat_id=chat_id,
                title=title,
                # 备用池来源只认 Bot 事件：被设置为管理员即候选备用频道。
                is_standby=is_admin and not bool(active_bindings),
                in_use=bool(active_bindings),
                admin_check_at=now_iso,
            )
            tracked_channels += 1

        await self.db.set_setting(self._offset_key, str(max_update_id))
        return {
            "ok": True,
            "received": len(updates),
            "tracked_channels": tracked_channels,
        }

    async def _fetch_updates(self, offset: int, timeout_seconds: int) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._fetch_updates_sync,
            offset,
            timeout_seconds,
        )

    def _fetch_updates_sync(self, offset: int, timeout_seconds: int) -> list[dict[str, Any]]:
        payload = {
            "offset": offset,
            "timeout": timeout_seconds,
            "allowed_updates": ["my_chat_member"],
        }
        body = json.dumps(payload).encode("utf-8")
        api_url = f"https://api.telegram.org/bot{self.settings.bot_token}/getUpdates"
        req = urlrequest.Request(
            api_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlrequest.urlopen(req, timeout=timeout_seconds + 10) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw)
        if not data.get("ok"):
            raise RuntimeError(data.get("description") or "Bot API getUpdates 失败")
        result = data.get("result") or []
        return [item for item in result if isinstance(item, dict)]
