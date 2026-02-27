from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.deps import get_state

router = APIRouter(prefix="/api/bindings", tags=["bindings"])


class BindRequest(BaseModel):
    source_group_id: int
    topic_id: int
    channel_chat_id: int


class BindingActiveRequest(BaseModel):
    active: bool


@router.get("")
async def list_bindings(request: Request, source_group_id: int | None = None):
    state = get_state(request)
    return await state.db.list_bindings(source_group_id)


@router.post("")
async def bind_topic(payload: BindRequest, request: Request):
    state = get_state(request)
    topic = await state.db.get_topic(payload.source_group_id, payload.topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="话题不存在")

    await state.db.upsert_channel(
        chat_id=payload.channel_chat_id,
        title=str(payload.channel_chat_id),
        is_standby=False,
        in_use=True,
    )
    binding = await state.db.upsert_binding(
        source_group_id=payload.source_group_id,
        topic_id=payload.topic_id,
        channel_chat_id=payload.channel_chat_id,
    )
    return {"ok": True, "binding": binding}


@router.post("/{source_group_id}/{topic_id}/active")
async def set_binding_active(
    source_group_id: int,
    topic_id: int,
    payload: BindingActiveRequest,
    request: Request,
):
    state = get_state(request)
    await state.db.set_binding_active(source_group_id, topic_id, payload.active)
    return {"ok": True}
