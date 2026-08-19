from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine

from db import engine as default_engine
from services.deriv_binary_execution_service import account_settings, binary_executions, _engine as execution_engine


def execution_history(user_id: str, deriv_account_id: str, *, limit: int = 50, engine: Engine | None = None) -> dict[str, Any]:
    chosen = execution_engine(engine or default_engine)
    account_settings(user_id, deriv_account_id, engine=chosen)
    with chosen.begin() as connection:
        rows = connection.execute(
            select(binary_executions)
            .where(
                (binary_executions.c.user_id == user_id)
                & (binary_executions.c.deriv_account_id == deriv_account_id)
            )
            .order_by(binary_executions.c.created_at.desc())
            .limit(max(1, min(int(limit), 100)))
        ).mappings().all()
    return {"ok": True, "items": [dict(row) for row in rows], "count": len(rows)}
