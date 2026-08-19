from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from services.user_auth_service import (
    SESSION_COOKIE,
    authenticate,
    clear_session_cookie,
    create_session,
    current_user,
    current_user_with_csrf,
    public_user,
    revoke_session,
    session_snapshot,
    set_session_cookie,
    signup,
)

router = APIRouter(prefix="/auth", tags=["auth"])


class SignupRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


@router.post("/signup")
def create_account(payload: SignupRequest, response: Response):
    try:
        user = signup(payload.email, payload.password)
        token, csrf, expires = create_session(user["id"])
        set_session_cookie(response, token)
        return {"ok": True, "user": user, "csrf_token": csrf, "expires_at": expires}
    except RuntimeError as exc:
        code = str(exc)
        status = 409 if code == "EMAIL_ALREADY_REGISTERED" else 400
        raise HTTPException(status_code=status, detail=code) from exc


@router.post("/login")
def login(payload: LoginRequest, response: Response):
    try:
        row = authenticate(payload.email, payload.password)
        token, csrf, expires = create_session(str(row["id"]))
        set_session_cookie(response, token)
        return {"ok": True, "user": public_user(row), "csrf_token": csrf, "expires_at": expires}
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail="INVALID_EMAIL_OR_PASSWORD") from exc


@router.get("/session")
def session(request: Request):
    snapshot = session_snapshot(request.cookies.get(SESSION_COOKIE, ""))
    if not snapshot:
        return {"ok": True, "authenticated": False}
    user, csrf = snapshot
    return {
        "ok": True,
        "authenticated": True,
        "user": {"id": user.id, "email": user.email, "role": user.role, "email_verified": user.email_verified},
        "csrf_token": csrf,
    }


@router.post("/logout")
def logout(request: Request, response: Response):
    current_user_with_csrf(request)
    revoke_session(request.cookies.get(SESSION_COOKIE, ""))
    clear_session_cookie(response)
    return {"ok": True, "authenticated": False}


@router.get("/me")
def me(request: Request):
    user = current_user(request)
    return {"ok": True, "user": {"id": user.id, "email": user.email, "role": user.role, "email_verified": user.email_verified}}
