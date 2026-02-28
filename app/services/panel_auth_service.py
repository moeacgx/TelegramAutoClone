import hashlib
import hmac
import time
from typing import Any

from fastapi import Response


class PanelAuthService:
    cookie_name = "panel_session"

    def __init__(self, settings: Any):
        self._password = str(getattr(settings, "panel_password", ""))
        self._ttl_seconds = int(getattr(settings, "panel_session_ttl_seconds", 86400))
        self._key = self._password.encode("utf-8")

    def verify_password(self, raw_password: str) -> bool:
        if raw_password is None:
            return False
        return hmac.compare_digest(raw_password, self._password)

    def build_session_token(self, current_ts: int | None = None) -> str:
        now_ts = int(current_ts if current_ts is not None else time.time())
        exp_ts = now_ts + self._ttl_seconds
        exp_raw = str(exp_ts)
        signature = hmac.new(
            self._key,
            exp_raw.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{exp_raw}.{signature}"

    def verify_session_token(self, token: str | None, current_ts: int | None = None) -> bool:
        if not token:
            return False

        parts = token.split(".", 1)
        if len(parts) != 2:
            return False

        exp_raw, signature = parts
        if not exp_raw.isdigit() or not signature:
            return False

        expected = hmac.new(
            self._key,
            exp_raw.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return False

        now_ts = int(current_ts if current_ts is not None else time.time())
        return int(exp_raw) >= now_ts

    def set_session_cookie(self, response: Response, token: str) -> None:
        response.set_cookie(
            key=self.cookie_name,
            value=token,
            httponly=True,
            samesite="lax",
            secure=False,
            max_age=self._ttl_seconds,
            path="/",
        )

    def clear_session_cookie(self, response: Response) -> None:
        response.delete_cookie(key=self.cookie_name, path="/")
