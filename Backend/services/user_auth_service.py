from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, Request, Response
from sqlalchemy import Boolean, Column, Float, MetaData, String, Table, Text, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from db import engine as default_engine

SESSION_COOKIE = "flowsignal_session"
SESSION_SECONDS = int(os.getenv("FLOWSIGNAL_SESSION_SECONDS", str(60 * 60 * 24 * 7)))
PBKDF2_ITERATIONS = max(600_000, int(os.getenv("FLOWSIGNAL_PBKDF2_ITERATIONS", "600000")))
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

metadata = MetaData()
users = Table(
    "flowsignal_users", metadata,
    Column("id", String(36), primary_key=True),
    Column("email", String(320), nullable=False, unique=True, index=True),
    Column("password_hash", Text, nullable=False),
    Column("role", String(16), nullable=False, default="user"),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("email_verified", Boolean, nullable=False, default=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Column("last_login_at", Float),
)
sessions = Table(
    "flowsignal_sessions", metadata,
    Column("token_hash", String(64), primary_key=True),
    Column("user_id", String(36), nullable=False, index=True),
    Column("csrf_token", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("expires_at", Float, nullable=False, index=True),
    Column("last_seen_at", Float, nullable=False),
    Column("revoked_at", Float),
)


@dataclass(frozen=True)
class CurrentUser:
    id: str
    email: str
    role: str
    email_verified: bool

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


def _engine(engine: Engine | None = None) -> Engine:
    chosen = engine or default_engine
    metadata.create_all(chosen)
    return chosen


def normalize_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if not EMAIL_RE.fullmatch(email):
        raise RuntimeError("INVALID_EMAIL")
    return email


def hash_password(password: str) -> str:
    password = str(password or "")
    if len(password) < 10 or len(password) > 1024:
        raise RuntimeError("PASSWORD_LENGTH_INVALID")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, rounds, salt_hex, digest_hex = str(encoded).split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(rounds)
        if iterations < 100_000:
            return False
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac("sha256", str(password).encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def public_user(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "email": row["email"],
        "role": row["role"],
        "email_verified": bool(row["email_verified"]),
    }


def signup(email: str, password: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    normalized = normalize_email(email)
    now = time.time()
    row = {
        "id": str(uuid.uuid4()),
        "email": normalized,
        "password_hash": hash_password(password),
        "role": "user",
        "is_active": True,
        "email_verified": False,
        "created_at": now,
        "updated_at": now,
    }
    try:
        with chosen.begin() as connection:
            connection.execute(users.insert().values(**row))
    except IntegrityError as exc:
        raise RuntimeError("EMAIL_ALREADY_REGISTERED") from exc
    return public_user(row)


def authenticate(email: str, password: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    normalized = normalize_email(email)
    with chosen.begin() as connection:
        row = connection.execute(select(users).where(users.c.email == normalized)).mappings().first()
        if not row or not row["is_active"] or not verify_password(password, row["password_hash"]):
            raise RuntimeError("INVALID_EMAIL_OR_PASSWORD")
        connection.execute(update(users).where(users.c.id == row["id"]).values(last_login_at=time.time(), updated_at=time.time()))
    return dict(row)


def create_session(user_id: str, *, engine: Engine | None = None) -> tuple[str, str, float]:
    chosen = _engine(engine)
    token = secrets.token_urlsafe(48)
    csrf = secrets.token_urlsafe(32)
    now = time.time()
    expires = now + SESSION_SECONDS
    with chosen.begin() as connection:
        connection.execute(sessions.insert().values(
            token_hash=_token_hash(token), user_id=user_id, csrf_token=csrf,
            created_at=now, expires_at=expires, last_seen_at=now, revoked_at=None,
        ))
    return token, csrf, expires


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        SESSION_COOKIE, token, max_age=SESSION_SECONDS, httponly=True, secure=True,
        samesite="none", path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE, path="/", secure=True, httponly=True, samesite="none")


def session_snapshot(token: str, *, engine: Engine | None = None) -> tuple[CurrentUser, str] | None:
    if not token:
        return None
    chosen = _engine(engine)
    now = time.time()
    with chosen.begin() as connection:
        session = connection.execute(select(sessions).where(sessions.c.token_hash == _token_hash(token))).mappings().first()
        if not session or session["revoked_at"] is not None or float(session["expires_at"]) <= now:
            return None
        user = connection.execute(select(users).where(users.c.id == session["user_id"])).mappings().first()
        if not user or not user["is_active"]:
            return None
        connection.execute(update(sessions).where(sessions.c.token_hash == session["token_hash"]).values(last_seen_at=now))
    return CurrentUser(str(user["id"]), str(user["email"]), str(user["role"]), bool(user["email_verified"])), str(session["csrf_token"])


def current_user(request: Request) -> CurrentUser:
    snapshot = session_snapshot(request.cookies.get(SESSION_COOKIE, ""))
    if not snapshot:
        raise HTTPException(status_code=401, detail="AUTHENTICATION_REQUIRED")
    return snapshot[0]


def current_user_with_csrf(request: Request) -> CurrentUser:
    snapshot = session_snapshot(request.cookies.get(SESSION_COOKIE, ""))
    if not snapshot:
        raise HTTPException(status_code=401, detail="AUTHENTICATION_REQUIRED")
    supplied = request.headers.get("X-FlowSignal-CSRF", "")
    if not supplied or not hmac.compare_digest(supplied, snapshot[1]):
        raise HTTPException(status_code=403, detail="CSRF_VALIDATION_FAILED")
    return snapshot[0]


def revoke_session(token: str, *, engine: Engine | None = None) -> None:
    if not token:
        return
    chosen = _engine(engine)
    with chosen.begin() as connection:
        connection.execute(update(sessions).where(sessions.c.token_hash == _token_hash(token)).values(revoked_at=time.time()))


def require_admin(request: Request) -> CurrentUser:
    user = current_user(request)
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")
    return user
