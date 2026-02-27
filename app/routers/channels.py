from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.deps import get_state

router = APIRouter(prefix="/api/channels", tags=["channels"])


class BatchStandbyRequest(BaseModel):
    refs_text: str


class DeleteStandbyBatchRequest(BaseModel):
    chat_ids: list[int]


@router.get("")
async def list_channels(request: Request):
    state = get_state(request)
    return await state.channel_service.list_channels()


@router.get("/standby")
async def list_standby(request: Request):
    state = get_state(request)
    return await state.channel_service.list_standby()


@router.post("/standby/batch")
async def batch_add_standby(payload: BatchStandbyRequest, request: Request):
    state = get_state(request)
    try:
        return await state.channel_service.add_standby_channels_batch(payload.refs_text)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/standby/{chat_id}")
async def delete_standby(chat_id: int, request: Request):
    state = get_state(request)
    result = await state.channel_service.remove_standby_channel(chat_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "删除失败")
    return result


@router.post("/standby/delete")
async def delete_standby_batch(payload: DeleteStandbyBatchRequest, request: Request):
    state = get_state(request)
    return await state.channel_service.remove_standby_channels_batch(payload.chat_ids)


@router.post("/standby/clear")
async def clear_standby(request: Request):
    state = get_state(request)
    return await state.channel_service.clear_standby_channels()


@router.post("/refresh-standby")
async def refresh_standby(request: Request):
    state = get_state(request)
    return await state.channel_service.refresh_standby_channels()


@router.get("/banned")
async def list_banned(request: Request):
    state = get_state(request)
    return await state.db.list_banned_channels()


@router.post("/banned/clear")
async def clear_banned(request: Request):
    state = get_state(request)
    cleared = await state.db.clear_banned_channels()
    return {"ok": True, "cleared": cleared}
