from fastapi import Request


def get_state(request: Request):
    return request.app.state
