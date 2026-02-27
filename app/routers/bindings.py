from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from telethon.tl.types import Channel
from telethon.utils import get_peer_id

from app.deps import get_state

router = APIRouter(prefix="/api/bindings", tags=["bindings"])


class BindRequest(BaseModel):
    source_group_id: int
    topic_id: int
    channel_chat_id: int | None = None
    channel_ref: str | None = None


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

    channel_ref = (payload.channel_ref or "").strip()
    if not channel_ref and payload.channel_chat_id is not None:
        channel_ref = str(payload.channel_chat_id)
    if not channel_ref:
        raise HTTPException(status_code=400, detail="请提供频道ID/@用户名/频道链接")

    try:
        entity = await state.telegram.resolve_chat(channel_ref, prefer_user=False)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"无法解析频道: {channel_ref}, {exc}") from exc

    if not isinstance(entity, Channel) or not bool(getattr(entity, "broadcast", False)):
        raise HTTPException(status_code=400, detail="仅支持绑定频道")

    channel_chat_id = int(get_peer_id(entity))
    channel_title = (getattr(entity, "title", None) or str(channel_chat_id)).strip()

    ok, error_text = await state.channel_service.check_channel_access(channel_chat_id)
    if not ok:
        raise HTTPException(
            status_code=400,
            detail=error_text or "Bot 无法访问该频道，请确认 Bot 已加入且为管理员",
        )

    await state.db.upsert_channel(
        chat_id=channel_chat_id,
        title=channel_title,
        is_standby=False,
        in_use=True,
    )
    binding = await state.db.upsert_binding(
        source_group_id=payload.source_group_id,
        topic_id=payload.topic_id,
        channel_chat_id=channel_chat_id,
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
