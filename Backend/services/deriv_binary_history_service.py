from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from db import engine as default_engine
from services.deriv_binary_execution_service import account_settings, binary_executions, _engine as execution_engine


PUBLIC_HISTORY_FIELDS = (
    "id", "signal_id", "strategy_version", "direction", "contract_type",
    "symbol", "deriv_account_id", "account_type", "duration", "duration_unit",
    "stake", "currency",
    "proposal_id", "contract_id", "transaction_id", "purchase_timestamp",
    "expiry_timestamp", "buy_price", "potential_payout", "broker_status",
    "outcome", "profit_loss", "settlement_payout", "settlement_timestamp",
    "settlement_price", "created_at", "updated_at",
)


def _public_execution(row: Any) -> dict[str, Any]:
    return {field: row[field] for field in PUBLIC_HISTORY_FIELDS}


def execution_history(
    user_id: str,
    deriv_account_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
    engine: Engine | None = None,
) -> dict[str, Any]:
    chosen = execution_engine(engine or default_engine)
    account_settings(user_id, deriv_account_id, engine=chosen)
    bounded_limit = max(1, min(int(limit), 100))
    bounded_offset = max(0, int(offset))
    owned = (
        (binary_executions.c.user_id == user_id)
        & (binary_executions.c.deriv_account_id == deriv_account_id)
    )
    history_columns = [binary_executions.c[field] for field in PUBLIC_HISTORY_FIELDS]
    with chosen.begin() as connection:
        total = connection.execute(
            select(func.count()).select_from(binary_executions).where(owned)
        ).scalar_one()
        rows = connection.execute(
            select(*history_columns)
            .where(owned)
            .order_by(binary_executions.c.created_at.desc())
            .limit(bounded_limit)
            .offset(bounded_offset)
        ).mappings().all()
    return {
        "ok": True,
        "items": [_public_execution(row) for row in rows],
        "count": len(rows),
        "total": total,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "has_more": bounded_offset + len(rows) < total,
    }
