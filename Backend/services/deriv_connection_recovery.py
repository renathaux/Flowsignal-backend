from __future__ import annotations

import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine

from db import engine as default_engine
from services.deriv_service import connection_snapshot
from services.deriv_user_connection_store import deriv_connections


def latest_connection_id(user_id: str, *, engine: Engine | None = None) -> str | None:
    """Return the newest still-active persisted Deriv connection for one user."""
    user_id = str(user_id or "").strip()
    if not user_id:
        return None
    chosen = engine or default_engine
    deriv_connections.metadata.create_all(chosen)
    now = time.time()
    with chosen.begin() as connection:
        row = connection.execute(
            select(deriv_connections.c.connection_id)
            .where(
                (deriv_connections.c.user_id == user_id)
                & (deriv_connections.c.disconnected == False)  # noqa: E712
                & (deriv_connections.c.expires_at > now)
            )
            .order_by(deriv_connections.c.updated_at.desc())
            .limit(1)
        ).first()
    return str(row[0]) if row and row[0] else None


def current_connection_snapshot(user_id: str, *, validate_token: bool = False) -> dict[str, Any]:
    """Rebind a logged-in FlowSignal user to their persisted Deriv connection."""
    connection_id = latest_connection_id(user_id)
    if not connection_id:
        return {"connected": False, "account_aware": True}
    return connection_snapshot(connection_id, user_id=user_id, validate_token=validate_token)
