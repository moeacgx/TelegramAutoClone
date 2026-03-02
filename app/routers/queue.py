import logging
import time

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from telethon.tl.types import Channel
from telethon.utils import get_peer_id

from app.deps import get_state

router = APIRouter(prefix="/api/queue", tags=["queue"])
logger = logging.getLogger(__name__)


class QueueActionRequest(BaseModel):
    run_now: bool = True


class QueueClearRequest(BaseModel):
    include_running: bool = False


class ManualRecoveryRequest(BaseModel):
    source_group_id: int
    topic_id: int
    channel_chat_id: int | None = None
    channel_ref: str | None = None
    run_now: bool = True


@router.get("/recovery")
async def list_recovery_queue(request: Request):
    state = get_state(request)
    return await state.db.list_recovery_queue()


@router.post("/recovery/run-once")
async def run_recovery_once(request: Request):
    state = get_state(request)
    processed = await state.recovery_worker.run_once()
    return {"ok": True, "processed": processed}


@router.post("/recovery/manual-start")
async def start_manual_recovery(payload: ManualRecoveryRequest, request: Request):
    state = get_state(request)
    topic = await state.db.get_topic(payload.source_group_id, payload.topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="话题不存在")
    existing_binding = await state.db.get_binding(payload.source_group_id, payload.topic_id)

    channel_ref = (payload.channel_ref or "").strip()
    if not channel_ref and payload.channel_chat_id is not None:
        channel_ref = str(payload.channel_chat_id)
    if not channel_ref:
        raise HTTPException(status_code=400, detail="请提供频道ID/@用户名/频道链接")

    try:
        entity = await state.telegram.resolve_chat(channel_ref, prefer_user=True)
    except Exception as user_exc:
        try:
            entity = await state.telegram.resolve_chat(channel_ref, prefer_user=False)
        except Exception as bot_exc:
            raise HTTPException(
                status_code=400,
                detail=f"无法解析频道: {channel_ref}, user={user_exc}, bot={bot_exc}",
            ) from bot_exc

    if not isinstance(entity, Channel) or not bool(getattr(entity, "broadcast", False)):
        raise HTTPException(status_code=400, detail="仅支持频道恢复任务")

    channel_chat_id = int(get_peer_id(entity))
    channel_title = (getattr(entity, "title", None) or str(channel_chat_id)).strip()

    ok, error_text = await state.channel_service.check_channel_access(channel_chat_id)
    if not ok:
        raise HTTPException(
            status_code=400,
            detail=error_text or "Bot 无法访问或无权限发送到该频道，请确认 Bot 已在频道内且具备管理员发送权限",
        )

    bound_same_channel = bool(
        existing_binding
        and int(existing_binding.get("active", 0)) == 1
        and int(existing_binding.get("channel_chat_id") or 0) == channel_chat_id
    )
    profile_applied = False
    if not bound_same_channel:
        await state.channel_service.apply_topic_profile(
            channel_chat_id=channel_chat_id,
            topic_title=str(topic.get("title") or payload.topic_id),
            topic_avatar_path=str(topic.get("avatar_path") or ""),
        )
        profile_applied = True

    await state.db.upsert_channel(
        chat_id=channel_chat_id,
        title=str(topic.get("title") or channel_title),
        is_standby=False,
        in_use=True,
    )
    await state.db.upsert_binding(
        source_group_id=payload.source_group_id,
        topic_id=payload.topic_id,
        channel_chat_id=channel_chat_id,
    )
    queue_id = await state.db.enqueue_manual_recovery(
        source_group_id=payload.source_group_id,
        topic_id=payload.topic_id,
        channel_chat_id=channel_chat_id,
        reason="从话题工作列表手动触发恢复任务",
    )

    processed = False
    if payload.run_now:
        processed = await state.recovery_worker.run_once(queue_id=queue_id)
    return {
        "ok": True,
        "queue_id": queue_id,
        "processed": processed,
        "profile_applied": profile_applied,
    }


@router.post("/recovery/{queue_id}/run-once")
async def run_recovery_by_id(queue_id: int, request: Request):
    state = get_state(request)
    processed = await state.recovery_worker.run_once(queue_id=queue_id)
    return {"ok": True, "processed": processed, "queue_id": queue_id}


@router.post("/recovery/{queue_id}/continue")
async def continue_recovery_task(queue_id: int, body: QueueActionRequest, request: Request):
    state = get_state(request)
    try:
        task = await state.db.requeue_recovery_task(queue_id=queue_id, restart=False)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if task is None:
        raise HTTPException(status_code=404, detail=f"任务不存在: {queue_id}")

    processed = False
    if body.run_now:
        processed = await state.recovery_worker.run_once(queue_id=queue_id)
    return {
        "ok": True,
        "queue_id": queue_id,
        "action": "continue",
        "processed": processed,
    }


@router.post("/recovery/{queue_id}/restart")
async def restart_recovery_task(queue_id: int, body: QueueActionRequest, request: Request):
    state = get_state(request)
    try:
        task = await state.db.requeue_recovery_task(queue_id=queue_id, restart=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if task is None:
        raise HTTPException(status_code=404, detail=f"任务不存在: {queue_id}")

    processed = False
    if body.run_now:
        processed = await state.recovery_worker.run_once(queue_id=queue_id)
    return {
        "ok": True,
        "queue_id": queue_id,
        "action": "restart",
        "processed": processed,
    }


@router.post("/recovery/{queue_id}/stop")
async def stop_recovery_task(queue_id: int, request: Request):
    state = get_state(request)
    try:
        task = await state.db.stop_recovery_task(queue_id=queue_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if task is None:
        raise HTTPException(status_code=404, detail=f"任务不存在: {queue_id}")
    return {"ok": True, "queue_id": queue_id, "status": task.get("status")}


@router.delete("/recovery/{queue_id}")
async def delete_recovery_task(queue_id: int, request: Request):
    state = get_state(request)
    try:
        ok = await state.db.delete_recovery_task(queue_id=queue_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if ok is None:
        raise HTTPException(status_code=404, detail=f"任务不存在: {queue_id}")
    return {"ok": True, "queue_id": queue_id}


@router.post("/recovery/clear")
async def clear_recovery_queue(payload: QueueClearRequest, request: Request):
    state = get_state(request)
    result = await state.db.clear_recovery_queue(include_running=payload.include_running)
    return {"ok": True, **result}


@router.post("/recovery/reset-running")
async def reset_running_recovery_tasks(request: Request):
    state = get_state(request)
    reset_count = await state.db.reset_running_recovery_tasks()
    return {"ok": True, "reset_count": reset_count}


@router.post("/monitor/run-once")
async def run_monitor_once(request: Request):
    state = get_state(request)
    started = time.monotonic()
    logger.info("手动巡检开始")
    result = await state.monitor_service.scan_once()
    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "手动巡检完成: scanned=%s transient_errors=%s unavailable=%s enqueued=%s already_queued=%s skipped_source_disabled=%s elapsed_ms=%s",
        result.get("scanned", 0),
        result.get("transient_errors", 0),
        result.get("unavailable", 0),
        result.get("enqueued", 0),
        result.get("already_queued", 0),
        result.get("skipped_source_disabled", 0),
        elapsed_ms,
    )
    return {"ok": True, "elapsed_ms": elapsed_ms, **result}
