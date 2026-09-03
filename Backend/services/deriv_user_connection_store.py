from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import threading
import time
import weakref
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import Boolean, Column, Float, MetaData, String, Table, Text, select, update
from sqlalchemy.engine import Engine

from db import engine as default_engine

metadata = MetaData()
deriv_connections = Table(
    "flowsignal_deriv_connections", metadata,
    Column("connection_id", String(128), primary_key=True),
    Column("user_id", String(36), nullable=False, index=True),
    Column("encrypted_access_token", Text, nullable=False),
    Column("accounts_json", Text, nullable=False),
    Column("selected_account_id", String(255)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Column("expires_at", Float, nullable=False),
    Column("disconnected", Boolean, nullable=False, default=False),
)
deriv_oauth_states = Table(
    "flowsignal_deriv_oauth_states", metadata,
    Column("state_hash", String(64), primary_key=True),
    Column("user_id", String(36), nullable=False, index=True),
    Column("verifier_hash", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("expires_at", Float, nullable=False),
    Column("consumed_at", Float),
)

_METADATA_INIT_LOCK = threading.Lock()
_METADATA_READY_ENGINES: weakref.WeakSet[Engine] = weakref.WeakSet()


def _engine(engine: Engine | None = None) -> Engine:
    chosen = engine or default_engine
    if chosen not in _METADATA_READY_ENGINES:
        with _METADATA_INIT_LOCK:
            if chosen not in _METADATA_READY_ENGINES:
                metadata.create_all(chosen)
                _METADATA_READY_ENGINES.add(chosen)
    return chosen


def _fernet() -> Fernet:
    configured = os.getenv("FLOWSIGNAL_DERIV_TOKEN_KEY", "").strip()
    if not configured:
        raise RuntimeError("FLOWSIGNAL_DERIV_TOKEN_KEY_REQUIRED")
    try:
        raw = base64.urlsafe_b64decode(configured.encode("ascii"))
        if len(raw) == 32:
            return Fernet(configured.encode("ascii"))
    except Exception:
        pass
    derived = base64.urlsafe_b64encode(hashlib.sha256(configured.encode("utf-8")).digest())
    return Fernet(derived)


def _hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def register_oauth_state(user_id: str, state: str, code_verifier: str, *, engine: Engine | None = None) -> dict[str, Any]:
    if not user_id or not state or len(code_verifier) < 43:
        raise RuntimeError("DERIV_OAUTH_STATE_INVALID")
    chosen = _engine(engine)
    now = time.time()
    state_hash = _hash(state)
    with chosen.begin() as connection:
        existing = connection.execute(select(deriv_oauth_states).where(deriv_oauth_states.c.state_hash == state_hash)).mappings().first()
        if existing:
            raise RuntimeError("DERIV_OAUTH_STATE_ALREADY_REGISTERED")
        connection.execute(deriv_oauth_states.insert().values(
            state_hash=state_hash, user_id=user_id, verifier_hash=_hash(code_verifier),
            created_at=now, expires_at=now + 600, consumed_at=None,
        ))
    return {"ok": True, "expires_at": now + 600}


def consume_oauth_state(user_id: str, state: str, code_verifier: str, *, engine: Engine | None = None) -> None:
    chosen = _engine(engine)
    now = time.time()
    state_hash = _hash(state)
    with chosen.begin() as connection:
        row = connection.execute(select(deriv_oauth_states).where(deriv_oauth_states.c.state_hash == state_hash)).mappings().first()
        if not row or row["user_id"] != user_id:
            raise RuntimeError("DERIV_OAUTH_USER_MISMATCH")
        if row["consumed_at"] is not None:
            raise RuntimeError("DERIV_OAUTH_STATE_REPLAYED")
        if float(row["expires_at"]) <= now:
            raise RuntimeError("DERIV_OAUTH_STATE_EXPIRED")
        if _hash(code_verifier) != row["verifier_hash"]:
            raise RuntimeError("DERIV_OAUTH_VERIFIER_MISMATCH")
        connection.execute(update(deriv_oauth_states).where(deriv_oauth_states.c.state_hash == state_hash).values(consumed_at=now))


def latest_selected_account(user_id: str, authorized_account_ids: set[str], *, engine: Engine | None = None) -> str | None:
    """Return the user's last explicit selection only when it is still authorized."""
    if not user_id or not authorized_account_ids:
        return None
    chosen = _engine(engine)
    with chosen.begin() as connection:
        row = connection.execute(
            select(deriv_connections.c.selected_account_id)
            .where(
                (deriv_connections.c.user_id == user_id)
                & (deriv_connections.c.disconnected == False)  # noqa: E712
                & (deriv_connections.c.selected_account_id.is_not(None))
                & (deriv_connections.c.selected_account_id.in_(tuple(authorized_account_ids)))
            )
            .order_by(deriv_connections.c.updated_at.desc())
            .limit(1)
        ).first()
    return str(row[0] or "").strip() if row else None


def save_connection(user_id: str, access_token: str, accounts: list[dict[str, Any]], expires_at: float, *, selected_account_id: str | None = None, connection_id: str | None = None, engine: Engine | None = None) -> str:
    chosen = _engine(engine)
    cid = connection_id or secrets.token_urlsafe(32)
    now = time.time()
    encrypted = _fernet().encrypt(access_token.encode("utf-8")).decode("ascii")
    with chosen.begin() as connection:
        connection.execute(deriv_connections.insert().values(
            connection_id=cid, user_id=user_id, encrypted_access_token=encrypted,
            accounts_json=json.dumps(accounts, separators=(",", ":"), default=str),
            selected_account_id=selected_account_id, created_at=now, updated_at=now,
            expires_at=expires_at, disconnected=False,
        ))
    return cid


def load_connection(connection_id: str, user_id: str | None = None, *, engine: Engine | None = None) -> dict[str, Any] | None:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        row = connection.execute(select(deriv_connections).where(deriv_connections.c.connection_id == connection_id)).mappings().first()
    if not row or row["disconnected"] or float(row["expires_at"]) <= time.time():
        return None
    if user_id is not None and row["user_id"] != user_id:
        raise RuntimeError("DERIV_CONNECTION_USER_MISMATCH")
    try:
        token = _fernet().decrypt(str(row["encrypted_access_token"]).encode("ascii")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("DERIV_TOKEN_DECRYPT_FAILED") from exc
    return {
        "connection_id": row["connection_id"], "user_id": row["user_id"], "access_token": token,
        "accounts": json.loads(row["accounts_json"] or "[]"), "selected_account_id": row["selected_account_id"],
        "created_at": row["created_at"], "updated_at": row["updated_at"], "expires_at": row["expires_at"],
    }


def set_selected(connection_id: str, user_id: str, account_id: str, *, engine: Engine | None = None) -> None:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        result = connection.execute(update(deriv_connections).where(
            (deriv_connections.c.connection_id == connection_id) & (deriv_connections.c.user_id == user_id) &
            (deriv_connections.c.disconnected == False)  # noqa: E712
        ).values(selected_account_id=account_id, updated_at=time.time()))
        if result.rowcount != 1:
            raise RuntimeError("DERIV_CONNECTION_USER_MISMATCH")


def disconnect_connection(connection_id: str, user_id: str, *, engine: Engine | None = None) -> None:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        connection.execute(update(deriv_connections).where(
            (deriv_connections.c.connection_id == connection_id) & (deriv_connections.c.user_id == user_id)
        ).values(disconnected=True, updated_at=time.time()))
