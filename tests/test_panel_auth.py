from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

PANEL_AUTH_PATH = Path(__file__).resolve().parent.parent / "app" / "services" / "panel_auth_service.py"
spec = spec_from_file_location("panel_auth_service", PANEL_AUTH_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"无法加载模块: {PANEL_AUTH_PATH}")
module = module_from_spec(spec)
spec.loader.exec_module(module)
PanelAuthService = module.PanelAuthService


@dataclass
class DummySettings:
    panel_password: str = "abc123"
    panel_session_ttl_seconds: int = 60


def test_verify_password_ok_and_fail():
    service = PanelAuthService(DummySettings())

    assert service.verify_password("abc123") is True
    assert service.verify_password("wrong") is False
    assert service.verify_password("") is False


def test_token_valid_and_expired_and_tampered():
    service = PanelAuthService(DummySettings(panel_password="secret", panel_session_ttl_seconds=10))

    token = service.build_session_token(current_ts=100)
    assert service.verify_session_token(token, current_ts=109) is True
    assert service.verify_session_token(token, current_ts=111) is False

    exp, sign = token.split(".", 1)
    tampered = f"{exp}.{sign[:-1]}0"
    assert service.verify_session_token(tampered, current_ts=105) is False
