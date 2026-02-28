from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.deps import get_state

router = APIRouter(tags=["dashboard"])


def _is_panel_authorized(request: Request) -> bool:
    state = get_state(request)
    service = state.panel_auth
    token = request.cookies.get(service.cookie_name)
    return service.verify_session_token(token)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    state = get_state(request)
    if not _is_panel_authorized(request):
        return RedirectResponse(url="/login", status_code=302)

    return state.templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name": state.settings.app_name,
        },
    )


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    state = get_state(request)
    if _is_panel_authorized(request):
        return RedirectResponse(url="/", status_code=302)

    return state.templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "app_name": state.settings.app_name,
        },
    )
