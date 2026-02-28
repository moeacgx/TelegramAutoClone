from fastapi import APIRouter, HTTPException, Request

from app.deps import get_state

router = APIRouter(prefix="/api/update", tags=["update"])


@router.get("/status")
async def update_status(request: Request):
    state = get_state(request)
    try:
        return await state.update_service.get_status()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/check")
async def check_update(request: Request):
    state = get_state(request)
    try:
        return await state.update_service.check_and_notify()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/confirm")
async def confirm_update(request: Request):
    state = get_state(request)
    try:
        return await state.update_service.confirm_and_trigger_update()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
