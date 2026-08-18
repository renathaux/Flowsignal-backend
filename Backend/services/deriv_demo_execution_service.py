"""Isolated Deriv demo-only Options execution for the Binary mini-app.

This module has no imports from cTrader/Forex execution code. It accepts a
FlowSignal observation (BUY/SELL) and, only after positively verifying a Deriv
demo Options account, purchases a $10 five-minute Rise/Fall contract.

Real-money Deriv execution is intentionally impossible here.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any

import requests
import websockets

from services.deriv_service import (
    DERIV_API_BASE,
    DERIV_CLIENT_ID,
    _CONNECTIONS,
    _LOCK,
    _headers,
    _is_demo_account,
    assert_demo_connection,
)

DEFAULT_STAKE_USD = 10.0
DEFAULT_DURATION_MINUTES = 5
UNDERLYING_SYMBOL = "frxEURUSD"

_EXECUTION_LOCK = threading.RLock()
_LAST_SIGNAL_BY_CONNECTION: dict[str, str] = {}
_LAST_TRADE_BY_CONNECTION: dict[str, dict[str, Any]] = {}


def _private_connection(connection_id: str) -> dict[str, Any]:
    assert_demo_connection(connection_id)
    with _LOCK:
        record = _CONNECTIONS.get(connection_id)
        if not isinstance(record, dict):
            raise RuntimeError("Deriv connection expired")
        access_token = str(record.get("access_token") or "").strip()
        accounts = list(record.get("accounts") or [])
    if not access_token:
        raise RuntimeError("Deriv access token unavailable")
    demo = next((item for item in accounts if isinstance(item, dict) and _is_demo_account(item)), None)
    if not demo:
        raise RuntimeError("No positively verified Deriv demo Options account is available")
    account_id = str(demo.get("account_id") or demo.get("id") or demo.get("loginid") or "").strip()
    if not account_id:
        raise RuntimeError("Verified Deriv demo account has no account ID")
    return {"access_token": access_token, "account": demo, "account_id": account_id}


def _otp_websocket_url(access_token: str, account_id: str) -> str:
    response = requests.post(
        f"{DERIV_API_BASE}/trading/v1/options/accounts/{account_id}/otp",
        headers=_headers(access_token),
        timeout=15,
    )
    if not response.ok:
        detail = response.text[:300]
        raise RuntimeError(f"Deriv demo OTP failed ({response.status_code}): {detail}")
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    url = str((data or {}).get("url") or "").strip()
    if not url:
        raise RuntimeError("Deriv OTP response did not include a WebSocket URL")
    if "/ws/demo" not in url:
        raise RuntimeError("Deriv execution blocked: OTP did not return a demo WebSocket")
    return url


async def _recv_for_req(ws, req_id: int, timeout: float = 12.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Timed out waiting for Deriv WebSocket response")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        message = json.loads(raw)
        if not isinstance(message, dict):
            continue
        if message.get("error"):
            error = message.get("error") or {}
            raise RuntimeError(str(error.get("message") or error.get("code") or "Deriv WebSocket error"))
        if message.get("req_id") == req_id:
            return message


async def _purchase_demo_contract_async(websocket_url: str, side: str, stake: float, duration_minutes: int) -> dict[str, Any]:
    contract_type = "CALL" if side == "BUY" else "PUT"
    async with websockets.connect(websocket_url, open_timeout=10, close_timeout=5) as ws:
        proposal_req_id = 101
        await ws.send(json.dumps({
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "duration": duration_minutes,
            "duration_unit": "m",
            "underlying_symbol": UNDERLYING_SYMBOL,
            "req_id": proposal_req_id,
        }))
        proposal_message = await _recv_for_req(ws, proposal_req_id)
        proposal = proposal_message.get("proposal") or {}
        proposal_id = str(proposal.get("id") or "").strip()
        if not proposal_id:
            raise RuntimeError("Deriv proposal did not return an ID")
        try:
            ask_price = float(proposal.get("ask_price") or stake)
        except (TypeError, ValueError):
            ask_price = stake
        buy_req_id = 102
        await ws.send(json.dumps({"buy": proposal_id, "price": max(ask_price, stake), "req_id": buy_req_id}))
        buy_message = await _recv_for_req(ws, buy_req_id)
        buy = buy_message.get("buy") or {}
        contract_id = buy.get("contract_id")
        if contract_id in (None, ""):
            raise RuntimeError("Deriv buy response did not return a contract ID")
        return {
            "ok": True,
            "demo_only": True,
            "side": side,
            "contract_type": contract_type,
            "underlying_symbol": UNDERLYING_SYMBOL,
            "duration_minutes": duration_minutes,
            "stake": stake,
            "contract_id": contract_id,
            "transaction_id": buy.get("transaction_id"),
            "buy_price": buy.get("buy_price"),
            "payout": buy.get("payout"),
            "balance_after": buy.get("balance_after"),
            "purchase_time": buy.get("purchase_time"),
            "start_time": buy.get("start_time"),
            "longcode": buy.get("longcode"),
        }


def execute_demo_signal(connection_id: str, signal: str, signal_id: str, *, stake: float = DEFAULT_STAKE_USD, duration_minutes: int = DEFAULT_DURATION_MINUTES) -> dict[str, Any]:
    side = str(signal or "").strip().upper()
    normalized_signal_id = str(signal_id or "").strip()
    if side not in {"BUY", "SELL"}:
        return {"ok": False, "executed": False, "reason": "WAIT", "demo_only": True}
    if not normalized_signal_id:
        raise RuntimeError("Binary signal ID is required")
    stake = round(float(stake), 2)
    duration_minutes = int(duration_minutes)
    if stake != DEFAULT_STAKE_USD:
        raise RuntimeError("Binary demo stake is locked to $10.00")
    if duration_minutes != DEFAULT_DURATION_MINUTES:
        raise RuntimeError("Binary demo expiry is locked to 5 minutes")

    private = _private_connection(connection_id)
    with _EXECUTION_LOCK:
        if _LAST_SIGNAL_BY_CONNECTION.get(connection_id) == normalized_signal_id:
            return {
                "ok": True,
                "executed": False,
                "duplicate": True,
                "demo_only": True,
                "reason": "SIGNAL_ALREADY_EXECUTED",
                "last_trade": _LAST_TRADE_BY_CONNECTION.get(connection_id),
            }
        ws_url = _otp_websocket_url(private["access_token"], private["account_id"])
        result = asyncio.run(_purchase_demo_contract_async(ws_url, side, stake, duration_minutes))
        result.update({
            "executed": True,
            "signal_id": normalized_signal_id,
            "account_id": private["account_id"],
            "executed_at": time.time(),
        })
        _LAST_SIGNAL_BY_CONNECTION[connection_id] = normalized_signal_id
        _LAST_TRADE_BY_CONNECTION[connection_id] = dict(result)
        return result


def execution_snapshot(connection_id: str) -> dict[str, Any]:
    assert_demo_connection(connection_id)
    with _EXECUTION_LOCK:
        return {
            "ok": True,
            "demo_only": True,
            "stake": DEFAULT_STAKE_USD,
            "duration_minutes": DEFAULT_DURATION_MINUTES,
            "underlying_symbol": UNDERLYING_SYMBOL,
            "last_signal_id": _LAST_SIGNAL_BY_CONNECTION.get(connection_id),
            "last_trade": _LAST_TRADE_BY_CONNECTION.get(connection_id),
            "real_money_enabled": False,
        }
