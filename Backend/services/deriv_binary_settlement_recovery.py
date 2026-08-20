"""Restart-safe monitor-only recovery for already-purchased Deriv contracts."""
from __future__ import annotations

import asyncio
import json
import secrets
import threading
import time
from typing import Any, Callable

import requests
import websockets
from sqlalchemy import and_, or_, select, update
from sqlalchemy.engine import Engine

from db import engine as default_engine
from services.deriv_binary_execution_service import (
    GENUINE_SIGNAL_ID, account_identifier, account_type, binary_accounts,
    binary_executions, _engine as execution_engine,
)
from services.deriv_service import DERIV_API_BASE, _headers, private_account
from services.deriv_v5_demo_relay_service import RULE_HASH, STRATEGY_VERSION

RECOVERABLE = {"PURCHASED", "SETTLEMENT_PENDING", "RECOVERING", "RECOVERY_RETRY", "RECONNECT_REQUIRED"}
FINAL = {"WON", "LOST", "SETTLED", "SOLD"}
LEASE_SECONDS = 420
POLL_SECONDS = 30
MAX_BATCH = 10
_START_LOCK = threading.Lock()
_STARTED = False


def _eligible(row: dict[str, Any]) -> bool:
    contract_id = str(row.get("contract_id") or "").strip()
    match = GENUINE_SIGNAL_ID.fullmatch(str(row.get("signal_id") or ""))
    return bool(
        contract_id and len(contract_id) <= 255 and match
        and row.get("strategy_version") == STRATEGY_VERSION
        and row.get("rule_hash") == RULE_HASH
        and row.get("symbol") == "frxEURUSD"
        and match.group("direction") == row.get("direction")
        and str(row.get("broker_status") or "") in RECOVERABLE
        and not row.get("settlement_timestamp")
    )


async def _monitor_async(context: dict[str, Any], contract_id: str) -> dict[str, Any]:
    account_id = account_identifier(context["account"])
    kind = account_type(context["account"])
    response = requests.post(f"{DERIV_API_BASE}/trading/v1/options/accounts/{account_id}/otp",
                             headers=_headers(context["access_token"]), timeout=15)
    if response.status_code in {401, 403}:
        raise RuntimeError("DERIV_RECOVERY_RECONNECT_REQUIRED")
    if not response.ok:
        raise RuntimeError("DERIV_RECOVERY_TEMPORARY_ERROR")
    url = str(((response.json().get("data") or {}).get("url")) or "")
    if ("/ws/demo" if kind == "DEMO" else "/ws/real") not in url:
        raise RuntimeError("DERIV_RECOVERY_ACCOUNT_TYPE_MISMATCH")
    async with websockets.connect(url, open_timeout=10, close_timeout=5) as ws:
        # Safety invariant: recovery sends only proposal_open_contract.
        await ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": contract_id, "subscribe": 1, "req_id": 701}))
        deadline = time.monotonic() + 330
        while time.monotonic() < deadline:
            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(1, deadline - time.monotonic())))
            if message.get("error"):
                raise RuntimeError("DERIV_RECOVERY_MONITOR_ERROR")
            if message.get("req_id") != 701:
                continue
            contract = message.get("proposal_open_contract") or {}
            status = str(contract.get("status") or "").upper()
            if contract.get("is_sold") or status in {"WON", "LOST", "SOLD"}:
                profit = float(contract.get("profit") or 0)
                return {
                    "broker_status": status or "SETTLED",
                    "outcome": "WIN" if profit > 0 else "LOSS" if profit < 0 else "TIE",
                    "profit_loss": profit,
                    "settlement_payout": float(contract.get("payout") or 0),
                    "settlement_timestamp": int(contract.get("exit_spot_time") or time.time()),
                    "settlement_price": float(contract.get("exit_spot") or contract.get("current_spot") or 0),
                }
        raise RuntimeError("DERIV_RECOVERY_STILL_PENDING")


def monitor_existing_contract(context: dict[str, Any], contract_id: str) -> dict[str, Any]:
    return asyncio.run(_monitor_async(context, contract_id))


def _claim(row_id: int, worker_id: str, now: float, engine: Engine) -> bool:
    with engine.begin() as connection:
        result = connection.execute(update(binary_executions).where(
            binary_executions.c.id == row_id,
            binary_executions.c.broker_status.in_(RECOVERABLE),
            binary_executions.c.settlement_timestamp.is_(None),
            or_(binary_executions.c.recovery_lease_expires_at.is_(None), binary_executions.c.recovery_lease_expires_at <= now),
        ).values(recovery_status="RECOVERING", recovery_lease_owner=worker_id,
                 recovery_lease_expires_at=now + LEASE_SECONDS, broker_status="RECOVERING", updated_at=now))
        return result.rowcount == 1


def recover_once(*, engine: Engine | None = None,
                 monitor: Callable[[dict[str, Any], str], dict[str, Any]] = monitor_existing_contract,
                 worker_id: str | None = None, now: float | None = None) -> dict[str, int]:
    chosen = execution_engine(engine or default_engine)
    stamp = float(now if now is not None else time.time())
    owner = worker_id or f"recovery-{secrets.token_urlsafe(12)}"
    with chosen.begin() as connection:
        rows = connection.execute(select(binary_executions).where(
            binary_executions.c.broker_status.in_(RECOVERABLE),
            binary_executions.c.settlement_timestamp.is_(None),
            or_(binary_executions.c.recovery_next_retry_at.is_(None), binary_executions.c.recovery_next_retry_at <= stamp),
        ).order_by(binary_executions.c.updated_at).limit(MAX_BATCH)).mappings().all()
    report = {"found": len(rows), "claimed": 0, "settled": 0, "deferred": 0, "ignored": 0}
    for raw in rows:
        row = dict(raw)
        if not _eligible(row):
            report["ignored"] += 1
            continue
        if not _claim(int(row["id"]), owner, stamp, chosen):
            continue
        report["claimed"] += 1
        try:
            with chosen.begin() as connection:
                account = connection.execute(select(binary_accounts).where(
                    binary_accounts.c.user_id == row["user_id"],
                    binary_accounts.c.deriv_account_id == row["deriv_account_id"],
                    binary_accounts.c.auth_state == "CONNECTED",
                )).mappings().first()
            if not account or not account.get("connection_id"):
                raise RuntimeError("DERIV_RECOVERY_RECONNECT_REQUIRED")
            context = private_account(str(account["connection_id"]), str(row["user_id"]), str(row["deriv_account_id"]))
            if account_type(context["account"]) != row["account_type"]:
                raise RuntimeError("DERIV_RECOVERY_ACCOUNT_TYPE_MISMATCH")
            result = monitor(context, str(row["contract_id"]))
            values = {key: result.get(key) for key in (
                "broker_status", "outcome", "profit_loss", "settlement_payout", "settlement_timestamp", "settlement_price"
            )}
            values.update(recovery_status="SETTLED", recovery_lease_owner=None, recovery_lease_expires_at=None,
                          recovery_next_retry_at=None, recovery_error_code=None, updated_at=time.time())
            with chosen.begin() as connection:
                connection.execute(update(binary_executions).where(
                    binary_executions.c.id == row["id"], binary_executions.c.recovery_lease_owner == owner
                ).values(**values))
            report["settled"] += 1
        except Exception as exc:
            code = str(exc) if str(exc).startswith("DERIV_RECOVERY_") else "DERIV_RECOVERY_TEMPORARY_ERROR"
            reconnect = code == "DERIV_RECOVERY_RECONNECT_REQUIRED"
            attempts = int(row.get("recovery_attempt_count") or 0) + 1
            delay = min(300, 15 * (2 ** min(attempts - 1, 4)))
            with chosen.begin() as connection:
                connection.execute(update(binary_executions).where(
                    binary_executions.c.id == row["id"], binary_executions.c.recovery_lease_owner == owner
                ).values(broker_status="RECONNECT_REQUIRED" if reconnect else "RECOVERY_RETRY",
                         recovery_status="RECONNECT_REQUIRED" if reconnect else "RETRY_PENDING",
                         recovery_attempt_count=attempts, recovery_next_retry_at=stamp + delay,
                         recovery_error_code=code, recovery_lease_owner=None, recovery_lease_expires_at=None,
                         updated_at=time.time()))
            report["deferred"] += 1
    return report


def _loop() -> None:
    time.sleep(5)
    while True:
        try:
            recover_once()
        except Exception as exc:
            print("DERIV_SETTLEMENT_RECOVERY_CYCLE_ERROR", type(exc).__name__)
        time.sleep(POLL_SECONDS)


def start_settlement_recovery_worker() -> None:
    global _STARTED
    with _START_LOCK:
        if _STARTED:
            return
        _STARTED = True
        threading.Thread(target=_loop, name="deriv-settlement-recovery", daemon=True).start()
