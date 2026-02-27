from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.deps import get_state

router = APIRouter(prefix="/api/source-groups", tags=["source-groups"])


class AddSourceGroupRequest(BaseModel):
    chat_ref: str


class EnableRequest(BaseModel):
    enabled: bool


@router.get("")
async def list_source_groups(request: Request):
    state = get_state(request)
    return await state.topic_service.list_source_groups()


@router.post("")
async def add_source_group(payload: AddSourceGroupRequest, request: Request):
    state = get_state(request)
    try:
        return await state.topic_service.add_source_group(payload.chat_ref)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{source_group_id}/sync-topics")
async def sync_topics(source_group_id: int, request: Request):
    state = get_state(request)
    try:
        before_topics = await state.db.list_topics(source_group_id)
        before_map = {int(item["topic_id"]): str(item.get("title") or "") for item in before_topics}
        topics = await state.topic_service.sync_topics(source_group_id)
        changed: list[dict[str, str | int]] = []
        for item in topics:
            topic_id = int(item["topic_id"])
            new_title = str(item.get("title") or "")
            old_title = before_map.get(topic_id, "")
            if old_title != new_title:
                changed.append(
                    {
                        "topic_id": topic_id,
                        "old_title": old_title,
                        "new_title": new_title,
                    }
                )
        return {
            "ok": True,
            "topics": topics,
            "total": len(topics),
            "changed": len(changed),
            "changed_samples": changed[:10],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{source_group_id}/enabled")
async def set_source_group_enabled(source_group_id: int, payload: EnableRequest, request: Request):
    state = get_state(request)
    await state.topic_service.set_source_group_enabled(source_group_id, payload.enabled)
    return {"ok": True}
