from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.deps import get_state

router = APIRouter(prefix="/api/queue", tags=["queue"])


class QueueActionRequest(BaseModel):
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


@router.post("/recovery/reset-running")
async def reset_running_recovery_tasks(request: Request):
    state = get_state(request)
    reset_count = await state.db.reset_running_recovery_tasks()
    return {"ok": True, "reset_count": reset_count}


@router.post("/monitor/run-once")
async def run_monitor_once(request: Request):
    state = get_state(request)
    result = await state.monitor_service.scan_once()
    return {"ok": True, **result}
