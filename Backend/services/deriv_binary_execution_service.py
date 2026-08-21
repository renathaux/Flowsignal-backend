"""Account-aware execution of authoritative relayed Binary V5 signals.

This module deliberately has no dependency on Forex or cTrader.  A durable
reservation is written before any broker call so a retry or restart cannot buy
the same signal twice for the same user/account.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from typing import Any, Callable

import requests
import websockets
from sqlalchemy import Boolean, Column, Float, Integer, MetaData, String, Table, Text, UniqueConstraint, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from db import engine as default_engine
from services.deriv_service import DERIV_API_BASE, _headers, private_selected_account
from services.deriv_v5_demo_relay_service import RULE_HASH, STRATEGY_VERSION, relay_signals

SYMBOL = "frxEURUSD"
DURATION = 5
DURATION_UNIT = "m"
DEFAULT_STAKE = 1.0
REAL_EXECUTION_FLAG = "BINARY_REAL_EXECUTION_ENABLED"
GENUINE_SIGNAL_ID = re.compile(
    rf"^{re.escape(STRATEGY_VERSION)}:{re.escape(SYMBOL)}:(?P<timestamp>[1-9][0-9]*):(?P<direction>RISE|FALL)$"
)

metadata = MetaData()
binary_accounts = Table(
    "deriv_binary_accounts", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", String(255), nullable=False),
    Column("deriv_account_id", String(255), nullable=False),
    Column("connection_id", String(255)),
    Column("account_type", String(16), nullable=False),
    Column("currency", String(20), nullable=False),
    Column("balance", Float),
    Column("auth_state", String(32), nullable=False),
    Column("selected", Boolean, nullable=False, default=False),
    Column("binary_auto_enabled", Boolean, nullable=False, default=False),
    Column("binary_stake", Float, nullable=False, default=DEFAULT_STAKE),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("user_id", "deriv_account_id", name="uq_deriv_binary_account"),
)
binary_executions = Table(
    "deriv_binary_executions", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", String(255), nullable=False),
    Column("deriv_account_id", String(255), nullable=False),
    Column("account_type", String(16), nullable=False),
    Column("strategy_version", String(100), nullable=False),
    Column("rule_hash", String(64), nullable=False),
    Column("signal_id", String(255), nullable=False),
    Column("direction", String(8), nullable=False),
    Column("contract_type", String(8), nullable=False),
    Column("symbol", String(32), nullable=False),
    Column("duration", Integer, nullable=False),
    Column("duration_unit", String(4), nullable=False),
    Column("stake", Float, nullable=False),
    Column("currency", String(20), nullable=False),
    Column("proposal_id", String(255)),
    Column("contract_id", String(255)),
    Column("transaction_id", String(255)),
    Column("buy_price", Float),
    Column("potential_payout", Float),
    Column("purchase_timestamp", Integer),
    Column("expiry_timestamp", Integer),
    Column("broker_status", String(32), nullable=False),
    Column("outcome", String(8)),
    Column("profit_loss", Float),
    Column("settlement_payout", Float),
    Column("settlement_timestamp", Integer),
    Column("settlement_price", Float),
    Column("broker_payload_json", Text),
    Column("recovery_status", String(32)),
    Column("recovery_lease_owner", String(128)),
    Column("recovery_lease_expires_at", Float),
    Column("recovery_attempt_count", Integer, nullable=False, default=0),
    Column("recovery_next_retry_at", Float),
    Column("recovery_error_code", String(64)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("user_id", "deriv_account_id", "strategy_version", "signal_id", name="uq_deriv_binary_execution"),
)
binary_signal_claims = Table(
    "deriv_binary_signal_claims", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("deriv_account_id", String(255), nullable=False),
    Column("strategy_version", String(100), nullable=False),
    Column("signal_id", String(255), nullable=False),
    Column("user_id", String(255), nullable=False),
    Column("created_at", Float, nullable=False),
    UniqueConstraint("deriv_account_id", "strategy_version", "signal_id", name="uq_deriv_binary_account_signal_claim"),
)

ACTIVE_CONTRACT_STATUSES = frozenset({"PURCHASED", "OPEN", "RUNNING", "SETTLEMENT_PENDING"})


def _engine(engine: Engine | None) -> Engine:
    chosen = engine or default_engine
    metadata.create_all(chosen)
    return chosen


def genuine_signal_validation(signal: dict[str, Any]) -> dict[str, Any]:
    """Positively identify a signal created by the frozen V5 worker.

    Inbox persistence is deliberately broader than execution eligibility so
    synthetic transport tests remain auditable without becoming actionable.
    """
    if signal.get("strategy_version") != STRATEGY_VERSION:
        return {"valid": False, "reason": "V5_STRATEGY_VERSION_MISMATCH"}
    if signal.get("rule_hash") != RULE_HASH:
        return {"valid": False, "reason": "V5_RULE_HASH_MISMATCH"}
    if signal.get("symbol") != SYMBOL:
        return {"valid": False, "reason": "V5_SYMBOL_MISMATCH"}
    direction = str(signal.get("direction") or "")
    if direction not in {"RISE", "FALL"}:
        return {"valid": False, "reason": "V5_DIRECTION_INVALID"}
    match = GENUINE_SIGNAL_ID.fullmatch(str(signal.get("signal_id") or ""))
    if not match:
        return {"valid": False, "reason": "V5_SIGNAL_ID_INVALID"}
    if match.group("direction") != direction:
        return {"valid": False, "reason": "V5_SIGNAL_ID_DIRECTION_MISMATCH"}
    try:
        timestamp = int(match.group("timestamp"))
        entry_timestamp = int(signal.get("entry_timestamp"))
    except (TypeError, ValueError):
        return {"valid": False, "reason": "V5_SIGNAL_TIMESTAMP_INVALID"}
    if timestamp <= 0 or timestamp != entry_timestamp:
        return {"valid": False, "reason": "V5_SIGNAL_TIMESTAMP_INVALID"}
    return {"valid": True, "timestamp": timestamp, "direction": direction}


def is_genuine_signal(signal: dict[str, Any]) -> bool:
    return bool(genuine_signal_validation(signal)["valid"])


def account_type(account: dict[str, Any]) -> str:
    """Use Deriv-provided account metadata; uncertainty is a hard block."""
    for key in ("is_demo", "is_virtual", "virtual", "demo"):
        value = account.get(key)
        if value is True or str(value or "").strip().lower() in {"true", "1", "yes"}:
            return "DEMO"
    values = [str(account.get(k) or "").strip().lower() for k in ("account_type", "type", "environment", "category")]
    if any(v in {"demo", "virtual", "practice"} or "demo" in v or "virtual" in v for v in values):
        return "DEMO"
    if any(v in {"real", "live"} or "real" in v for v in values):
        return "REAL"
    raise RuntimeError("DERIV_ACCOUNT_TYPE_UNCERTAIN")


def account_identifier(account: dict[str, Any]) -> str:
    value = str(account.get("account_id") or account.get("id") or account.get("loginid") or "").strip()
    if not value:
        raise RuntimeError("DERIV_ACCOUNT_ID_UNCERTAIN")
    return value


def sync_accounts(user_id: str, connection_id: str, accounts: list[dict[str, Any]], *, selected_account_id: str | None = None, engine: Engine | None = None) -> list[dict[str, Any]]:
    chosen = _engine(engine); now = time.time(); normalized = []
    for raw in accounts:
        aid = account_identifier(raw); kind = account_type(raw)
        currency = str(raw.get("currency") or "").strip().upper()
        if not currency:
            raise RuntimeError("DERIV_ACCOUNT_CURRENCY_UNCERTAIN")
        normalized.append({"account_id": aid, "account_type": kind, "currency": currency, "balance": raw.get("balance")})
    if selected_account_id and selected_account_id not in {x["account_id"] for x in normalized}:
        raise RuntimeError("DERIV_SELECTED_ACCOUNT_NOT_AUTHORIZED")
    selected = selected_account_id or (normalized[0]["account_id"] if len(normalized) == 1 else None)
    with chosen.begin() as connection:
        connection.execute(update(binary_accounts).where(binary_accounts.c.user_id == user_id).values(selected=False, auth_state="DISCONNECTED", updated_at=now))
        for item in normalized:
            existing = connection.execute(select(binary_accounts).where(
                (binary_accounts.c.user_id == user_id) & (binary_accounts.c.deriv_account_id == item["account_id"])
            )).mappings().first()
            values = dict(connection_id=connection_id, account_type=item["account_type"], currency=item["currency"],
                          balance=float(item["balance"]) if item["balance"] is not None else None,
                          auth_state="CONNECTED", selected=item["account_id"] == selected, updated_at=now)
            if existing:
                connection.execute(update(binary_accounts).where(binary_accounts.c.id == existing["id"]).values(**values))
            else:
                connection.execute(binary_accounts.insert().values(user_id=user_id, deriv_account_id=item["account_id"],
                    binary_auto_enabled=False, binary_stake=DEFAULT_STAKE, **values))
    return normalized


def select_account(user_id: str, connection_id: str, deriv_account_id: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine); now = time.time()
    with chosen.begin() as connection:
        row = connection.execute(select(binary_accounts).where((binary_accounts.c.user_id == user_id) &
            (binary_accounts.c.deriv_account_id == deriv_account_id) & (binary_accounts.c.connection_id == connection_id) &
            (binary_accounts.c.auth_state == "CONNECTED"))).mappings().first()
        if not row: raise RuntimeError("DERIV_SELECTED_ACCOUNT_NOT_AUTHORIZED")
        connection.execute(update(binary_accounts).where(binary_accounts.c.user_id == user_id).values(selected=False, updated_at=now))
        connection.execute(update(binary_accounts).where(binary_accounts.c.id == row["id"]).values(selected=True, updated_at=now))
    return account_settings(user_id, deriv_account_id, engine=chosen)


def disconnect_accounts(connection_id: str, *, engine: Engine | None = None) -> None:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        connection.execute(update(binary_accounts).where(binary_accounts.c.connection_id == connection_id).values(
            auth_state="DISCONNECTED", selected=False, updated_at=time.time()))


def account_settings(user_id: str, deriv_account_id: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        row = connection.execute(select(binary_accounts).where((binary_accounts.c.user_id == user_id) & (binary_accounts.c.deriv_account_id == deriv_account_id))).mappings().first()
    if not row: raise RuntimeError("DERIV_ACCOUNT_NOT_FOUND")
    return {k: row[k] for k in ("user_id", "deriv_account_id", "account_type", "currency", "balance", "auth_state", "selected", "binary_auto_enabled", "binary_stake")}


def save_account_settings(user_id: str, deriv_account_id: str, *, enabled: bool, stake: float, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    try: amount = round(float(stake), 2)
    except (TypeError, ValueError): raise RuntimeError("BINARY_STAKE_INVALID") from None
    if amount <= 0 or amount > 100000: raise RuntimeError("BINARY_STAKE_INVALID")
    with chosen.begin() as connection:
        result = connection.execute(update(binary_accounts).where((binary_accounts.c.user_id == user_id) &
            (binary_accounts.c.deriv_account_id == deriv_account_id)).values(binary_auto_enabled=bool(enabled), binary_stake=amount, updated_at=time.time()))
        if result.rowcount != 1: raise RuntimeError("DERIV_ACCOUNT_NOT_FOUND")
    return account_settings(user_id, deriv_account_id, engine=chosen)


def latest_relay_signal(*, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        rows = connection.execute(select(relay_signals).order_by(relay_signals.c.decision_timestamp.desc())).mappings().all()
    row = next((candidate for candidate in rows if is_genuine_signal(candidate)), None)
    if not row:
        return {"ok": True, "signal": "WAIT", "reason": "NO_RELAYED_V5_SIGNAL", "strategy_version": STRATEGY_VERSION, "rule_hash": RULE_HASH}
    return {"ok": True, "signal": row["direction"], "signal_id": row["signal_id"], "symbol": row["symbol"],
            "decision_timestamp": row["decision_timestamp"], "strategy_version": row["strategy_version"], "rule_hash": row["rule_hash"]}


def execution_snapshot(user_id: str, deriv_account_id: str, *, engine: Engine | None = None) -> dict[str, Any]:
    chosen = _engine(engine)
    setting = account_settings(user_id, deriv_account_id, engine=chosen)
    relay_entry_quote = None
    with chosen.begin() as connection:
        row = connection.execute(select(binary_executions).where((binary_executions.c.user_id == user_id) &
            (binary_executions.c.deriv_account_id == deriv_account_id)).order_by(binary_executions.c.created_at.desc()).limit(1)).mappings().first()
        if row:
            relay_row = connection.execute(
                select(relay_signals.c.entry_quote).where(relay_signals.c.signal_id == row["signal_id"]).limit(1)
            ).first()
            if relay_row and relay_row[0] is not None:
                relay_entry_quote = float(relay_row[0])
    public_fields = (
        "id", "signal_id", "strategy_version", "direction", "contract_type", "symbol",
        "deriv_account_id", "account_type", "duration", "duration_unit", "stake", "currency",
        "proposal_id", "contract_id", "transaction_id", "purchase_timestamp", "expiry_timestamp",
        "buy_price", "potential_payout", "broker_status", "outcome", "profit_loss",
        "settlement_payout", "settlement_timestamp", "settlement_price", "recovery_status", "created_at", "updated_at",
    )
    public = {field: row[field] for field in public_fields} if row else None
    if public is not None:
        # The relay entry quote is the underlying EURUSD decision price. It is safe
        # to expose only for this already user/account-scoped execution snapshot and
        # lets the mobile UI show whether a running CALL/PUT is currently above or
        # below its entry without exposing the relay payload itself.
        public["entry_quote"] = relay_entry_quote
    is_confirmed_live = bool(
        row
        and str(row.get("contract_id") or "").strip()
        and str(row.get("broker_status") or "").upper() in ACTIVE_CONTRACT_STATUSES
    )
    return {"ok": True, "account": setting,
            "running_contract": public if is_confirmed_live else None,
            "last_execution": public}


def _real_enabled() -> bool:
    return os.getenv(REAL_EXECUTION_FLAG, "false").strip().lower() in {"1", "true", "yes", "on"}


async def _recv(ws, req_id: int, *, terminal: bool = False) -> dict[str, Any]:
    deadline = time.monotonic() + 330
    while time.monotonic() < deadline:
        message = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(1, deadline-time.monotonic())))
        if message.get("error"):
            error = message.get("error") or {}
            code = re.sub(r"[^A-Za-z0-9_.-]", "_", str(error.get("code") or "WEBSOCKET_ERROR"))[:80]
            detail = str(error.get("message") or "Deriv websocket request failed").replace("\n", " ")[:240]
            raise RuntimeError(f"DERIV_{code}: {detail}")
        if message.get("req_id") != req_id: continue
        if not terminal: return message
        contract = message.get("proposal_open_contract") or {}
        if contract.get("is_sold") or str(contract.get("status") or "").lower() in {"won", "lost", "sold"}: return message
    raise RuntimeError("DERIV_CONTRACT_SETTLEMENT_TIMEOUT")


async def _broker_async(access_token: str, account: dict[str, Any], direction: str, stake: float, currency: str,
                        checkpoint: Callable[..., None] | None = None) -> dict[str, Any]:
    account_id = account_identifier(account)
    response = requests.post(f"{DERIV_API_BASE}/trading/v1/options/accounts/{account_id}/otp", headers=_headers(access_token), timeout=15)
    if not response.ok: raise RuntimeError(f"DERIV_OTP_FAILED_{response.status_code}")
    url = str(((response.json().get("data") or {}).get("url")) or "")
    kind = account_type(account); expected_path = "/ws/demo" if kind == "DEMO" else "/ws/real"
    if expected_path not in url: raise RuntimeError("DERIV_OTP_ACCOUNT_TYPE_MISMATCH")
    contract_type = "CALL" if direction == "RISE" else "PUT"
    async with websockets.connect(url, open_timeout=10, close_timeout=5) as ws:
        # The Options API rejects the relative 5/m representation for EURUSD
        # CALL/PUT with ContractBuyValidationError. Its proposal schema supports
        # the equivalent absolute expiry, so retain the exact five-minute rule
        # while avoiding any strategy/filter change.
        proposal_expiry = int(time.time()) + (DURATION * 60)
        await ws.send(json.dumps({"proposal":1,"amount":stake,"basis":"stake","contract_type":contract_type,
            "currency":currency,"date_expiry":proposal_expiry,"underlying_symbol":SYMBOL,"req_id":101}))
        proposal = (await _recv(ws,101)).get("proposal") or {}; proposal_id=str(proposal.get("id") or "")
        if not proposal_id: raise RuntimeError("DERIV_PROPOSAL_ID_MISSING")
        ask=float(proposal.get("ask_price") or stake)
        if checkpoint: checkpoint("PROPOSED", proposal_id=proposal_id, potential_payout=float(proposal.get("payout") or 0))
        await ws.send(json.dumps({"buy":proposal_id,"price":ask,"req_id":102}))
        if checkpoint: checkpoint("PURCHASE_REQUEST_SENT", proposal_id=proposal_id)
        buy=(await _recv(ws,102)).get("buy") or {}; contract_id=buy.get("contract_id")
        if contract_id in (None,""): raise RuntimeError("DERIV_CONTRACT_ID_MISSING")
        if checkpoint: checkpoint("PURCHASED", contract_id=str(contract_id), transaction_id=buy.get("transaction_id"),
            buy_price=float(buy.get("buy_price") or ask), purchase_timestamp=int(buy.get("purchase_time") or time.time()))
        await ws.send(json.dumps({"proposal_open_contract":1,"contract_id":contract_id,"subscribe":1,"req_id":103}))
        contract=(await _recv(ws,103,terminal=True)).get("proposal_open_contract") or {}
        profit=float(contract.get("profit") or 0); status=str(contract.get("status") or "").lower()
        return {"proposal_id":proposal_id,"contract_id":str(contract_id),"transaction_id":buy.get("transaction_id"),
            "buy_price":float(buy.get("buy_price") or ask),"potential_payout":float(proposal.get("payout") or buy.get("payout") or 0),
            "purchase_timestamp":int(buy.get("purchase_time") or time.time()),"expiry_timestamp":int(contract.get("date_expiry") or time.time()),
            "broker_status":status.upper() or "SETTLED","outcome":"WIN" if profit>0 else "LOSS",
            "profit_loss":profit,"settlement_payout":float(contract.get("payout") or 0),
            "settlement_timestamp":int(contract.get("exit_spot_time") or time.time()),
            "settlement_price":float(contract.get("exit_spot") or contract.get("current_spot") or 0),"raw":contract}


def deriv_broker(account_context: dict[str, Any], direction: str, stake: float, currency: str) -> dict[str, Any]:
    return asyncio.run(_broker_async(account_context["access_token"], account_context["account"], direction, stake, currency,
                                     account_context.get("checkpoint")))


def execute_relayed_signal(user_id: str, connection_id: str, signal_id: str, *, engine: Engine | None = None,
                           broker: Callable[[dict[str, Any], str, float, str], dict[str, Any]] = deriv_broker) -> dict[str, Any]:
    chosen = _engine(engine)
    with chosen.begin() as connection:
        signal = connection.execute(select(relay_signals).where(relay_signals.c.signal_id == signal_id)).mappings().first()
        if not signal: return {"ok":False,"reason":"AUTHORITATIVE_V5_SIGNAL_REQUIRED","broker_action":False}
        validation = genuine_signal_validation(signal)
        if not validation["valid"]:
            return {"ok":False,"reason":"NON_EXECUTABLE_V5_SIGNAL","validation_reason":validation["reason"],"broker_action":False}
    private = private_selected_account(connection_id, user_id)
    aid = private["account_id"]
    with chosen.begin() as connection:
        acct = connection.execute(select(binary_accounts).where((binary_accounts.c.user_id == user_id) &
            (binary_accounts.c.deriv_account_id == aid) & (binary_accounts.c.selected.is_(True)) &
            (binary_accounts.c.auth_state == "CONNECTED"))).mappings().first()
        if not acct: return {"ok":False,"reason":"DERIV_ACCOUNT_IDENTITY_UNCERTAIN","broker_action":False}
        if not acct["binary_auto_enabled"]: return {"ok":False,"reason":"BINARY_AUTO_OFF","broker_action":False}
        if acct["account_type"] == "REAL" and not _real_enabled(): return {"ok":False,"reason":"REAL_BINARY_EXECUTION_DISABLED","broker_action":False}
        direction=signal["direction"]; contract_type="CALL" if direction=="RISE" else "PUT"; now=time.time()
        try:
            connection.execute(binary_signal_claims.insert().values(
                deriv_account_id=aid, strategy_version=STRATEGY_VERSION,
                signal_id=signal_id, user_id=user_id, created_at=now,
            ))
            result=connection.execute(binary_executions.insert().values(user_id=user_id,deriv_account_id=aid,
                account_type=acct["account_type"],strategy_version=STRATEGY_VERSION,rule_hash=RULE_HASH,signal_id=signal_id,
                direction=direction,contract_type=contract_type,symbol=SYMBOL,duration=DURATION,duration_unit=DURATION_UNIT,
                stake=acct["binary_stake"],currency=acct["currency"],broker_status="RESERVED",created_at=now,updated_at=now))
            execution_id=result.inserted_primary_key[0]
        except IntegrityError:
            return {"ok":False,"duplicate":True,"reason":"SIGNAL_ALREADY_EXECUTED","broker_action":False}
    def checkpoint(status: str, **fields: Any) -> None:
        allowed = {key: value for key, value in fields.items() if key in {
            "proposal_id", "contract_id", "transaction_id", "buy_price", "potential_payout", "purchase_timestamp"
        }}
        allowed.update(broker_status=status, updated_at=time.time())
        with chosen.begin() as connection:
            connection.execute(update(binary_executions).where(binary_executions.c.id == execution_id).values(**allowed))

    broker_context = dict(private)
    broker_context["checkpoint"] = checkpoint
    try:
        result=broker(broker_context,direction,float(acct["binary_stake"]),acct["currency"])
    except Exception as exc:
        safe_error = str(exc).replace("\n", " ")
        access_token = str(private.get("access_token") or "")
        if access_token:
            safe_error = safe_error.replace(access_token, "[REDACTED]")
        safe_error = safe_error[:300]
        with chosen.begin() as connection:
            current = connection.execute(select(binary_executions.c.broker_status).where(binary_executions.c.id == execution_id)).scalar_one()
            safe_status = {
                "PURCHASE_REQUEST_SENT": "PURCHASE_AMBIGUOUS",
                "PURCHASED": "SETTLEMENT_PENDING",
            }.get(str(current), "PROPOSAL_FAILED_SAFE" if current == "PROPOSED" else "FAILED_SAFE")
            connection.execute(update(binary_executions).where(binary_executions.c.id==execution_id).values(
                broker_status=safe_status,broker_payload_json=json.dumps({
                    "error_code": type(exc).__name__, "error": safe_error,
                }),updated_at=time.time()))
        print(json.dumps({"event": "BINARY_BROKER_FAILED_SAFE", "execution_id": execution_id,
                          "account_id": aid, "account_type": acct["account_type"],
                          "signal_id": signal_id, "status": safe_status, "error": safe_error}), flush=True)
        raise
    values={k:result.get(k) for k in ("proposal_id","contract_id","transaction_id","buy_price","potential_payout","purchase_timestamp","expiry_timestamp","broker_status","outcome","profit_loss","settlement_payout","settlement_timestamp","settlement_price")}
    values.update(broker_payload_json=json.dumps(result.get("raw") or {},default=str),updated_at=time.time())
    with chosen.begin() as connection: connection.execute(update(binary_executions).where(binary_executions.c.id==execution_id).values(**values))
    return {"ok":True,"executed":True,"broker_action":True,"execution_id":execution_id,"account_id":aid,"account_type":acct["account_type"],"signal_id":signal_id,"contract_id":result.get("contract_id"),"outcome":result.get("outcome")}


def execute_signal_candidates(signal_id: str, *, engine: Engine | None = None,
                              broker: Callable[[dict[str, Any], str, float, str], dict[str, Any]] = deriv_broker) -> list[dict[str, Any]]:
    """Process every currently connected, selected account with Binary Auto ON.

    Each candidate is isolated: one account failure cannot alter the inbox,
    research records, or another account's reservation/execution.
    """
    chosen = _engine(engine)
    with chosen.begin() as connection:
        signal = connection.execute(select(relay_signals).where(relay_signals.c.signal_id == signal_id)).mappings().first()
        if not signal or not is_genuine_signal(signal):
            return [{"ok": False, "reason": "NON_EXECUTABLE_V5_SIGNAL", "broker_action": False}]
        rows = connection.execute(select(binary_accounts).where(
            binary_accounts.c.selected.is_(True) & binary_accounts.c.binary_auto_enabled.is_(True) &
            (binary_accounts.c.auth_state == "CONNECTED")
        ).order_by(binary_accounts.c.updated_at.desc())).mappings().all()
    # A reconnect can leave legacy user/session rows pointing at the same physical
    # Deriv login. Dispatch only the newest selected row for each exact account.
    # The durable account-level claim below still closes concurrent race windows.
    rows = list({row["deriv_account_id"]: row for row in reversed(rows)}.values())
    results = []
    for row in rows:
        try:
            results.append(execute_relayed_signal(row["user_id"], row["connection_id"], signal_id, engine=chosen, broker=broker))
        except Exception as exc:
            results.append({"ok": False, "user_id": row["user_id"], "account_id": row["deriv_account_id"],
                            "reason": "BROKER_EXECUTION_FAILED_SAFE", "error": str(exc)[:200], "broker_action": False})
    return results
