"""Lean read paths for Binary UI/status polling.

These helpers intentionally project only the columns returned to callers. Large
broker/relay payload JSON stays in Neon unless an execution path explicitly
needs it, which keeps frequent status polling from consuming database egress.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine

from db import engine as default_engine
from services.deriv_binary_execution_service import (
    SYMBOL,
    binary_accounts,
    binary_executions,
    genuine_signal_validation,
)
from services.deriv_v5_demo_relay_service import RULE_HASH, STRATEGY_VERSION, relay_signals

ACCOUNT_SETTING_FIELDS = (
    "user_id",
    "deriv_account_id",
    "account_type",
    "currency",
    "balance",
    "auth_state",
    "selected",
    "binary_auto_enabled",
    "binary_stake",
    "binary_duration_minutes",
)

EXECUTION_PUBLIC_FIELDS = (
    "id",
    "signal_id",
    "strategy_version",
    "direction",
    "contract_type",
    "symbol",
    "deriv_account_id",
    "account_type",
    "duration",
    "duration_unit",
    "stake",
    "currency",
    "proposal_id",
    "contract_id",
    "transaction_id",
    "purchase_timestamp",
    "expiry_timestamp",
    "buy_price",
    "potential_payout",
    "broker_status",
    "outcome",
    "profit_loss",
    "settlement_payout",
    "settlement_timestamp",
    "settlement_price",
    "recovery_status",
    "created_at",
    "updated_at",
)

_RELAY_READ_COLUMNS = (
    relay_signals.c.signal_id,
    relay_signals.c.strategy_version,
    relay_signals.c.rule_hash,
    relay_signals.c.direction,
    relay_signals.c.symbol,
    relay_signals.c.decision_timestamp,
    relay_signals.c.entry_timestamp,
)


def _chosen_engine(engine: Engine | None = None) -> Engine:
    # Production schema is created by Alembic at deploy time. Read-only polling
    # must not run metadata.create_all() on every request.
    return engine or default_engine


def account_settings(
    user_id: str,
    deriv_account_id: str,
    *,
    engine: Engine | None = None,
) -> dict[str, Any]:
    chosen = _chosen_engine(engine)
    columns = [binary_accounts.c[field] for field in ACCOUNT_SETTING_FIELDS]
    with chosen.begin() as connection:
        row = connection.execute(
            select(*columns).where(
                (binary_accounts.c.user_id == user_id)
                & (binary_accounts.c.deriv_account_id == deriv_account_id)
            )
        ).mappings().first()
    if not row:
        raise RuntimeError("DERIV_ACCOUNT_NOT_FOUND")
    return {field: row[field] for field in ACCOUNT_SETTING_FIELDS}


def latest_relay_signal(*, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _chosen_engine(engine)
    # The old path downloaded the complete relay inbox, including payload_json,
    # then filtered in Python. Keep the same newest-valid-signal semantics while
    # filtering immutable identity in SQL and transferring only tiny scalars.
    with chosen.begin() as connection:
        rows = connection.execute(
            select(*_RELAY_READ_COLUMNS)
            .where(
                (relay_signals.c.strategy_version == STRATEGY_VERSION)
                & (relay_signals.c.rule_hash == RULE_HASH)
                & (relay_signals.c.symbol == SYMBOL)
                & (relay_signals.c.direction.in_(("RISE", "FALL")))
                & (relay_signals.c.signal_id.like(f"{STRATEGY_VERSION}:{SYMBOL}:%"))
            )
            .order_by(relay_signals.c.decision_timestamp.desc())
        ).mappings().all()
    row = next(
        (candidate for candidate in rows if genuine_signal_validation(candidate)["valid"]),
        None,
    )
    if not row:
        return {
            "ok": True,
            "signal": "WAIT",
            "reason": "NO_RELAYED_V5_SIGNAL",
            "strategy_version": STRATEGY_VERSION,
            "rule_hash": RULE_HASH,
        }
    return {
        "ok": True,
        "signal": row["direction"],
        "signal_id": row["signal_id"],
        "symbol": row["symbol"],
        "decision_timestamp": row["decision_timestamp"],
        "strategy_version": row["strategy_version"],
        "rule_hash": row["rule_hash"],
    }


def execution_snapshot(
    user_id: str,
    deriv_account_id: str,
    *,
    engine: Engine | None = None,
) -> dict[str, Any]:
    chosen = _chosen_engine(engine)
    setting = account_settings(user_id, deriv_account_id, engine=chosen)
    execution_columns = [binary_executions.c[field] for field in EXECUTION_PUBLIC_FIELDS]
    relay_entry_quote = None
    with chosen.begin() as connection:
        row = connection.execute(
            select(*execution_columns)
            .where(
                (binary_executions.c.user_id == user_id)
                & (binary_executions.c.deriv_account_id == deriv_account_id)
            )
            .order_by(binary_executions.c.created_at.desc())
            .limit(1)
        ).mappings().first()
        if row:
            relay_row = connection.execute(
                select(relay_signals.c.entry_quote)
                .where(relay_signals.c.signal_id == row["signal_id"])
                .limit(1)
            ).first()
            if relay_row and relay_row[0] is not None:
                relay_entry_quote = float(relay_row[0])

    public = {field: row[field] for field in EXECUTION_PUBLIC_FIELDS} if row else None
    if public is not None:
        public["entry_quote"] = relay_entry_quote

    is_confirmed_live = bool(
        row
        and str(row.get("contract_id") or "").strip()
        and not row.get("settlement_timestamp")
    )
    return {
        "ok": True,
        "account": setting,
        "running_contract": public if is_confirmed_live else None,
        "last_execution": public,
    }
