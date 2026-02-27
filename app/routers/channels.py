from fastapi import APIRouter, Request

from app.deps import get_state

router = APIRouter(prefix="/api/channels", tags=["channels"])


@router.get("")
async def list_channels(request: Request):
    state = get_state(request)
    return await state.channel_service.list_channels()


@router.get("/standby")
async def list_standby(request: Request):
    state = get_state(request)
    return await state.channel_service.list_standby()


@router.post("/refresh-standby")
async def refresh_standby(request: Request):
    state = get_state(request)
    return await state.channel_service.refresh_standby_channels()


@router.get("/banned")
async def list_banned(request: Request):
    state = get_state(request)
    return await state.db.list_banned_channels()
