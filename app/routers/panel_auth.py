from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.deps import get_state

router = APIRouter(prefix="/api/panel-auth", tags=["panel-auth"])


class PanelLoginRequest(BaseModel):
    password: str


@router.post("/login")
async def panel_login(payload: PanelLoginRequest, request: Request):
    state = get_state(request)
    service = state.panel_auth
    if not service.verify_password(payload.password):
        raise HTTPException(status_code=401, detail="后台密码错误")

    token = service.build_session_token()
    response = JSONResponse({"ok": True})
    service.set_session_cookie(response, token)
    return response


@router.post("/logout")
async def panel_logout(request: Request):
    state = get_state(request)
    service = state.panel_auth
    response = JSONResponse({"ok": True})
    service.clear_session_cookie(response)
    return response


@router.get("/status")
async def panel_status(request: Request):
    state = get_state(request)
    service = state.panel_auth
    token = request.cookies.get(service.cookie_name)
    return {"ok": True, "authorized": service.verify_session_token(token)}
