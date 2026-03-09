from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.deps import get_state

router = APIRouter(prefix="/api/settings", tags=["settings"])


class CloneSettingsPayload(BaseModel):
    md5_mutation_enabled: bool = False
    download_group_concurrency: int = Field(default=2, ge=1, le=5)


@router.get("/clone")
async def get_clone_settings(request: Request):
    state = get_state(request)
    try:
        settings = await state.clone_settings_service.get_settings()
        return settings.to_dict()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/clone")
async def update_clone_settings(payload: CloneSettingsPayload, request: Request):
    state = get_state(request)
    try:
        settings = await state.clone_settings_service.update_settings(
            md5_mutation_enabled=payload.md5_mutation_enabled,
            download_group_concurrency=payload.download_group_concurrency,
        )
        return {"ok": True, **settings.to_dict()}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
