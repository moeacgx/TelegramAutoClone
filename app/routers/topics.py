from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.deps import get_state

router = APIRouter(prefix="/api/topics", tags=["topics"])


class TopicEnableRequest(BaseModel):
    enabled: bool


@router.get("")
async def list_topics(request: Request, source_group_id: int | None = None):
    state = get_state(request)
    return await state.topic_service.list_topics(source_group_id)


@router.post("/{source_group_id}/{topic_id}/enabled")
async def set_topic_enabled(source_group_id: int, topic_id: int, payload: TopicEnableRequest, request: Request):
    state = get_state(request)
    await state.topic_service.set_topic_enabled(source_group_id, topic_id, payload.enabled)
    return {"ok": True}
