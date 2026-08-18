import os
import secrets
import threading
import time
from typing import Any

import requests

# Binary is an isolated mini-app inside FlowSignal. Use dedicated environment
# keys so Deriv configuration can be changed/re-provisioned without touching
# cTrader/Forex settings or any legacy DERIV_* variables elsewhere.
DERIV_CLIENT_ID = str(
    os.getenv("BINARY_DERIV_CLIENT_ID")
    or "348ZidIsn7poIwqP8AOtg"
).strip()
DERIV_REDIRECT_URI = str(
    os.getenv("BINARY_DERIV_REDIRECT_URI")
    or "https://flowsignalfx.com/deriv/callback"
).strip()
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
        "account_aware": True,
        "real_execution_enabled": str(os.getenv("BINARY_REAL_EXECUTION_ENABLED", "false")).lower() in {"1", "true", "yes", "on"},
        "config_source": (
            "BINARY_DERIV_CLIENT_ID"
            if os.getenv("BINARY_DERIV_CLIENT_ID")
            else "flowsignal_default"
        ),
    }


def _headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Deriv-App-ID": DERIV_CLIENT_ID,
        "Accept": "application/json",
    }


def exchange_authorization_code(code: str, code_verifier: str, *, user_id: str, selected_account_id: str | None = None) -> dict[str, Any]:
    if not str(user_id or "").strip():
        raise RuntimeError("FlowSignal user ID is required")
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
            "user_id": str(user_id).strip(),
            "selected_account_id": selected_account_id,
        }
    from services.deriv_binary_execution_service import sync_accounts
    normalized = sync_accounts(str(user_id).strip(), connection_id, accounts, selected_account_id=selected_account_id)
    if not selected_account_id and len(normalized) == 1:
        with _LOCK:
            _CONNECTIONS[connection_id]["selected_account_id"] = normalized[0]["account_id"]
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
    try:
        from services.deriv_binary_execution_service import account_type
        result["account_type_normalized"] = account_type(account)
    except RuntimeError:
        result["account_type_normalized"] = "UNKNOWN"
    return result


def connection_snapshot(connection_id: str) -> dict[str, Any]:
    with _LOCK:
        record = _CONNECTIONS.get(connection_id)
        if not record:
            return {"connected": False, "account_aware": True}
        if float(record.get("expires_at") or 0) <= time.time():
            _CONNECTIONS.pop(connection_id, None)
            return {"connected": False, "expired": True, "account_aware": True}
        accounts = list(record.get("accounts") or [])
    public_accounts = [_public_account(item) for item in accounts]
    demos = [item for item in public_accounts if item.get("demo_verified")]
    return {
        "connected": True,
        "connection_id": connection_id,
        "account_aware": True,
        "user_id": record.get("user_id"),
        "selected_account_id": record.get("selected_account_id"),
        "accounts": public_accounts,
        "demo_accounts": demos,
        "demo_account_verified": bool(demos),
        "expires_at": record.get("expires_at"),
    }


def disconnect(connection_id: str) -> None:
    with _LOCK:
        _CONNECTIONS.pop(connection_id, None)
    from services.deriv_binary_execution_service import disconnect_accounts
    disconnect_accounts(connection_id)


def set_selected_account(connection_id: str, user_id: str, account_id: str) -> dict[str, Any]:
    with _LOCK:
        record = _CONNECTIONS.get(connection_id)
        if not record or record.get("user_id") != user_id:
            raise RuntimeError("DERIV_CONNECTION_USER_MISMATCH")
        accounts = list(record.get("accounts") or [])
        ids = {str(a.get("account_id") or a.get("id") or a.get("loginid") or "").strip() for a in accounts}
        if account_id not in ids:
            raise RuntimeError("DERIV_SELECTED_ACCOUNT_NOT_AUTHORIZED")
        record["selected_account_id"] = account_id
    from services.deriv_binary_execution_service import select_account
    select_account(user_id, connection_id, account_id)
    return connection_snapshot(connection_id)


def private_selected_account(connection_id: str, user_id: str) -> dict[str, Any]:
    snapshot = connection_snapshot(connection_id)
    if not snapshot.get("connected"):
        raise RuntimeError("DERIV_NOT_CONNECTED")
    with _LOCK:
        record = _CONNECTIONS.get(connection_id)
        if not record or record.get("user_id") != user_id:
            raise RuntimeError("DERIV_CONNECTION_USER_MISMATCH")
        selected = str(record.get("selected_account_id") or "").strip()
        if not selected:
            raise RuntimeError("DERIV_ACCOUNT_SELECTION_REQUIRED")
        account = next((a for a in record.get("accounts") or [] if str(a.get("account_id") or a.get("id") or a.get("loginid") or "").strip() == selected), None)
        token = str(record.get("access_token") or "").strip()
    if not account or not token:
        raise RuntimeError("DERIV_ACCOUNT_IDENTITY_UNCERTAIN")
    return {"access_token": token, "account": account, "account_id": selected}


def assert_demo_connection(connection_id: str) -> dict[str, Any]:
    snapshot = connection_snapshot(connection_id)
    if not snapshot.get("connected"):
        raise RuntimeError("Deriv is not connected")
    if not snapshot.get("demo_account_verified"):
        raise RuntimeError("No Deriv demo Options account has been positively verified. Real-account execution is blocked.")
    return snapshot
