"""Authenticated inbox for authoritative frozen-V5 signal relays.

Receiving a signal never purchases a contract. Account-aware execution is a
separate service gated by persisted per-user/account Binary Auto settings.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, Text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from db import engine as default_engine

STRATEGY_VERSION = "DERIV_BINARY_SIMPLE_5M_1"
RULE_HASH = "755ea79159b49281b0671846d0883ec34702efa55c2cf397aafac47058f8e3cb"
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
