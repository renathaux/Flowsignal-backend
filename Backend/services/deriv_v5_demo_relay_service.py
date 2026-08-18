"""Authenticated inbox for authoritative frozen-V5 signal relays.

Receiving a signal never purchases a contract. Execution remains a separate,
demo-account-only action gated by the persisted Binary Demo Auto setting.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any

from sqlalchemy import Boolean, Column, Float, Integer, MetaData, String, Table, Text, UniqueConstraint, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from db import engine as default_engine

STRATEGY_VERSION = "DERIV_BINARY_V5_NOISY_REVERSAL_FROZEN_1"
RULE_HASH = "fab52bb80f7f4dd9150adb2f90d7e090816915ff70e6b368518e7fb39444b249"
SYMBOL = "frxEURUSD"
SIGNATURE_MAX_AGE_SECONDS = 300

metadata = MetaData()
relay_signals = Table(
    "deriv_v5_relay_signals", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("signal_id", String(255), nullable=False, unique=True),
    Column("strategy_version", String(100), nullable=False),
    Column("rule_hash", String(64), nullable=False),
    Column("direction", String(8), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("decision_timestamp", Integer, nullable=False),
    Column("entry_timestamp", Integer, nullable=False),
    Column("entry_quote", Float, nullable=False),
    Column("entry_quote_epoch", Integer, nullable=False),
    Column("settlement_target_timestamp", Integer, nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("received_at", Float, nullable=False),
    Column("status", String(32), nullable=False, default="RECEIVED"),
)
demo_settings = Table(
    "deriv_binary_demo_settings", metadata,
    Column("user_id", String(255), primary_key=True),
    Column("enabled", Boolean, nullable=False, default=False),
    Column("stake", Float, nullable=False, default=1.0),
    Column("updated_at", Float, nullable=False),
)
executions = Table(
    "deriv_v5_demo_executions", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", String(255), nullable=False),
    Column("account_id", String(255), nullable=False),
    Column("strategy_version", String(100), nullable=False),
    Column("signal_id", String(255), nullable=False),
    Column("status", String(32), nullable=False),
    Column("created_at", Float, nullable=False),
    UniqueConstraint("user_id", "account_id", "strategy_version", "signal_id", name="uq_deriv_v5_demo_execution"),
)


def _engine(engine: Engine | None) -> Engine:
    chosen = engine or default_engine
    metadata.create_all(chosen)
    return chosen


def verify_signature(body: bytes, timestamp: str, signature: str, *, secret: str, now: int | None = None) -> None:
    if not secret:
        raise RuntimeError("V5_RELAY_AUTH_NOT_CONFIGURED")
    try:
        sent_at = int(timestamp)
    except (TypeError, ValueError):
        raise RuntimeError("V5_RELAY_UNAUTHENTICATED") from None
    if abs(int(now or time.time()) - sent_at) > SIGNATURE_MAX_AGE_SECONDS:
        raise RuntimeError("V5_RELAY_SIGNATURE_EXPIRED")
    expected = hmac.new(
        secret.encode("utf-8"), str(sent_at).encode("ascii") + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    if not signature or not hmac.compare_digest(expected, signature):
        raise RuntimeError("V5_RELAY_UNAUTHENTICATED")


def receive_signal(
    body: bytes, timestamp: str, signature: str, *,
    secret: str | None = None, now: int | None = None, engine: Engine | None = None,
) -> dict[str, Any]:
    verify_signature(body, timestamp, signature, secret=secret if secret is not None else os.getenv("BINARY_V5_RELAY_SECRET", ""), now=now)
    payload = json.loads(body)
    required = {"strategy_version", "rule_hash", "signal_id", "direction", "symbol", "decision_timestamp", "entry_timestamp", "entry_quote", "entry_quote_epoch", "settlement_target_timestamp"}
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise RuntimeError("V5_RELAY_INVALID_PAYLOAD")
    if payload["strategy_version"] != STRATEGY_VERSION or payload["rule_hash"] != RULE_HASH:
        raise RuntimeError("V5_RELAY_FROZEN_IDENTITY_MISMATCH")
    if payload["direction"] not in {"RISE", "FALL"} or payload["symbol"] != SYMBOL:
        raise RuntimeError("V5_RELAY_UNSUPPORTED_SIGNAL")
    chosen = _engine(engine)
    values = {key: payload[key] for key in required}
    values.update(payload_json=body.decode("utf-8"), received_at=float(now or time.time()), status="RECEIVED")
    try:
        with chosen.begin() as connection:
            connection.execute(relay_signals.insert().values(**values))
    except IntegrityError:
        return {"ok": True, "duplicate": True, "signal_id": payload["signal_id"], "broker_action": False}
    return {"ok": True, "duplicate": False, "signal_id": payload["signal_id"], "broker_action": False}


def binary_demo_auto(user_id: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        row = connection.execute(select(demo_settings).where(demo_settings.c.user_id == user_id)).mappings().first()
    return {"user_id": user_id, "enabled": bool(row["enabled"]) if row else False, "stake": float(row["stake"]) if row else 1.0, "demo_only": True}


def reserve_execution(user_id: str, account_id: str, signal_id: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    setting = binary_demo_auto(user_id, engine=chosen)
    if not setting["enabled"]:
        return {"ok": False, "reason": "BINARY_DEMO_AUTO_OFF", "broker_action": False}
    with chosen.begin() as connection:
        signal = connection.execute(select(relay_signals).where(relay_signals.c.signal_id == signal_id)).mappings().first()
        if not signal:
            return {"ok": False, "reason": "AUTHORITATIVE_V5_SIGNAL_REQUIRED", "broker_action": False}
        savepoint = connection.begin_nested()
        try:
            connection.execute(executions.insert().values(
                user_id=user_id, account_id=account_id, strategy_version=STRATEGY_VERSION,
                signal_id=signal_id, status="RESERVED", created_at=time.time(),
            ))
            savepoint.commit()
        except IntegrityError:
            savepoint.rollback()
            return {"ok": False, "reason": "SIGNAL_ALREADY_EXECUTED", "broker_action": False}
    return {"ok": True, "reserved": True, "broker_action": False}
