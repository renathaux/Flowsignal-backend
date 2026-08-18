import os
import secrets
import threading
import time
from typing import Any

import requests

# Binary is an isolated mini-app inside FlowSignal. Use the exact OAuth client
# registered for FlowSignal on developers.deriv.com so stale Render environment
# variables cannot silently point this integration at an old/legacy client.
DERIV_CLIENT_ID = "348ZidIsn7poIwqP8A0tg"
DERIV_REDIRECT_URI = "https://flowsignalfx.com/deriv/callback"
DERIV_AUTH_URL = "https://auth.deriv.com/oauth2/auth"
DERIV_TOKEN_URL = "https://auth.deriv.com/oauth2/token"
DERIV_API_BASE = "https://api.derivws.com"

_CONNECTIONS: dict[str, dict[str, Any]] = {}
_LOCK = threading.RLock()


def public_config() -> dict[str, Any]:
    return {
        "configured": bool(DERIV_CLIENT_ID),
        "client_id": DERIV_CLIENT_ID,
        "redirect_uri": DERIV_REDIRECT_URI,
        "authorization_url": DERIV_AUTH_URL,
        "scope": "trade",
        "demo_only": True,
    }


def _headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Deriv-App-ID": DERIV_CLIENT_ID,
        "Accept": "application/json",
    }


def exchange_authorization_code(code: str, code_verifier: str) -> dict[str, Any]:
    response = requests.post(
        DERIV_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "client_id": DERIV_CLIENT_ID,
            "code": code,
            "code_verifier": code_verifier,
            "redirect_uri": DERIV_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    if not response.ok:
        detail = ""
        try:
            body = response.json()
            detail = str(body.get("error_description") or body.get("error") or "").strip()
        except Exception:
            detail = response.text[:200].strip()
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"Deriv token exchange failed ({response.status_code}){suffix}")
    payload = response.json()
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Deriv token response did not include an access token")

    accounts = fetch_options_accounts(access_token)
    connection_id = secrets.token_urlsafe(32)
    expires_in = int(payload.get("expires_in") or 3600)
    now = time.time()
    with _LOCK:
        _CONNECTIONS[connection_id] = {
            "access_token": access_token,
            "created_at": now,
            "expires_at": now + max(60, expires_in),
            "accounts": accounts,
        }
    return connection_snapshot(connection_id)


def fetch_options_accounts(access_token: str) -> list[dict[str, Any]]:
    response = requests.get(
        f"{DERIV_API_BASE}/trading/v1/options/accounts",
        headers=_headers(access_token),
        timeout=15,
    )
    if not response.ok:
        raise RuntimeError(f"Deriv account lookup failed ({response.status_code})")
    payload = response.json()
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "accounts", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _is_demo_account(account: dict[str, Any]) -> bool:
    # Never infer an unknown account as demo. Execution remains blocked unless
    # Deriv explicitly identifies the account as demo/virtual/practice.
    for key in ("is_demo", "is_virtual", "virtual", "demo"):
        if account.get(key) is True or str(account.get(key) or "").lower() in {"true", "1", "yes"}:
            return True
    for key in ("account_type", "type", "environment", "category"):
        value = str(account.get(key) or "").strip().lower()
        if value in {"demo", "virtual", "practice"} or "demo" in value or "virtual" in value:
            return True
    return False


def _public_account(account: dict[str, Any]) -> dict[str, Any]:
    safe_keys = (
        "id", "account_id", "loginid", "currency", "balance", "account_type",
        "type", "environment", "is_demo", "is_virtual", "status", "display_name",
    )
    result = {key: account.get(key) for key in safe_keys if key in account}
    result["demo_verified"] = _is_demo_account(account)
    return result


def connection_snapshot(connection_id: str) -> dict[str, Any]:
    with _LOCK:
        record = _CONNECTIONS.get(connection_id)
        if not record:
            return {"connected": False, "demo_only": True}
        if float(record.get("expires_at") or 0) <= time.time():
            _CONNECTIONS.pop(connection_id, None)
            return {"connected": False, "expired": True, "demo_only": True}
        accounts = list(record.get("accounts") or [])
    public_accounts = [_public_account(item) for item in accounts]
    demos = [item for item in public_accounts if item.get("demo_verified")]
    return {
        "connected": True,
        "connection_id": connection_id,
        "demo_only": True,
        "accounts": public_accounts,
        "demo_accounts": demos,
        "demo_account_verified": bool(demos),
        "expires_at": record.get("expires_at"),
    }


def disconnect(connection_id: str) -> None:
    with _LOCK:
        _CONNECTIONS.pop(connection_id, None)


def assert_demo_connection(connection_id: str) -> dict[str, Any]:
    snapshot = connection_snapshot(connection_id)
    if not snapshot.get("connected"):
        raise RuntimeError("Deriv is not connected")
    if not snapshot.get("demo_account_verified"):
        raise RuntimeError("No Deriv demo Options account has been positively verified. Real-account execution is blocked.")
    return snapshot
