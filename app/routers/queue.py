from fastapi import APIRouter, Request

from app.deps import get_state

router = APIRouter(prefix="/api/queue", tags=["queue"])


@router.get("/recovery")
async def list_recovery_queue(request: Request):
    state = get_state(request)
    return await state.db.list_recovery_queue()


@router.post("/recovery/run-once")
async def run_recovery_once(request: Request):
    state = get_state(request)
    processed = await state.recovery_worker.run_once()
    return {"ok": True, "processed": processed}


@router.post("/monitor/run-once")
async def run_monitor_once(request: Request):
    state = get_state(request)
    await state.monitor_service.scan_once()
    return {"ok": True}
