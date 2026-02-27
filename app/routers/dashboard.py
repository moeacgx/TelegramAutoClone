from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.deps import get_state

router = APIRouter(tags=["dashboard"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    state = get_state(request)
    return state.templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name": state.settings.app_name,
        },
    )
