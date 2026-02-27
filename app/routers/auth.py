from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.deps import get_state

router = APIRouter(prefix="/api/auth", tags=["auth"])


class PhoneCodeRequest(BaseModel):
    phone: str = Field(..., description="手机号，建议 E.164 格式")


class PhoneLoginRequest(BaseModel):
    phone: str
    code: str = ""
    password: str | None = None


class PasswordOnlyLoginRequest(BaseModel):
    password: str
    session_id: str | None = None


@router.get("/status")
async def auth_status(request: Request):
    state = get_state(request)
    return await state.telegram.get_auth_status()


@router.post("/phone/send")
async def send_phone_code(payload: PhoneCodeRequest, request: Request):
    state = get_state(request)
    try:
        return await state.telegram.send_phone_code(payload.phone)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/phone/login")
async def phone_login(payload: PhoneLoginRequest, request: Request):
    state = get_state(request)
    try:
        return await state.telegram.sign_in_with_code(payload.phone, payload.code, payload.password)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/password/login")
async def password_login(payload: PasswordOnlyLoginRequest, request: Request):
    state = get_state(request)
    try:
        return await state.telegram.sign_in_with_password_only(payload.password, payload.session_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/qr/create")
async def create_qr(request: Request):
    state = get_state(request)
    try:
        return await state.telegram.create_qr_login()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/qr/poll/{session_id}")
async def poll_qr(session_id: str, request: Request):
    state = get_state(request)
    return await state.telegram.check_qr_login(session_id)
