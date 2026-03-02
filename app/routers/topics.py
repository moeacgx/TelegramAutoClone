from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
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


@router.get("/{source_group_id}/{topic_id}/avatar")
async def get_topic_avatar(source_group_id: int, topic_id: int, request: Request):
    state = get_state(request)
    avatar_file = await state.topic_service.get_topic_avatar_file(source_group_id, topic_id)
    if avatar_file is None:
        raise HTTPException(status_code=404, detail="该话题未配置头像")
    return FileResponse(path=avatar_file, media_type="image/jpeg", filename=avatar_file.name)


@router.post("/{source_group_id}/{topic_id}/avatar")
async def upload_topic_avatar(
    source_group_id: int,
    topic_id: int,
    request: Request,
    avatar: UploadFile = File(...),
):
    state = get_state(request)
    topic = await state.db.get_topic(source_group_id, topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="话题不存在")

    raw = await avatar.read()
    if not raw:
        raise HTTPException(status_code=400, detail="头像文件为空")
    if len(raw) > 15 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="头像文件过大，请控制在 15MB 内")

    try:
        updated_topic = await state.topic_service.save_topic_avatar(
            source_group_id=source_group_id,
            topic_id=topic_id,
            raw_bytes=raw,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "ok": True,
        "topic": updated_topic,
        "avatar_url": f"/api/topics/{source_group_id}/{topic_id}/avatar",
        "applied_now": False,
    }


@router.delete("/{source_group_id}/{topic_id}/avatar")
async def delete_topic_avatar(source_group_id: int, topic_id: int, request: Request):
    state = get_state(request)
    try:
        topic = await state.topic_service.clear_topic_avatar(source_group_id, topic_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "topic": topic}
