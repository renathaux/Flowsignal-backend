from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from services.deriv_service import (
    bind_oauth_state,
    connection_snapshot,
    disconnect,
    exchange_authorization_code,
    public_config,
    set_selected_account,
)
from services.deriv_binary_execution_service import (
    account_settings,
    execute_relayed_signal,
    execution_snapshot as account_execution_snapshot,
    latest_relay_signal,
    save_account_settings,
)
from services.deriv_binary_history_service import execution_history
from services.user_auth_service import current_user, current_user_with_csrf

router = APIRouter(prefix="/user/deriv", tags=["user-deriv"])


class OAuthStateRequest(BaseModel):
    state: str
    code_verifier: str


class OAuthExchangeRequest(BaseModel):
    code: str
    code_verifier: str
    state: str
    selected_account_id: str | None = None


class ConnectionRequest(BaseModel):
    connection_id: str


class AccountSelectionRequest(BaseModel):
    connection_id: str
    deriv_account_id: str


class AccountSettingsRequest(BaseModel):
    deriv_account_id: str
    enabled: bool
    stake: float


class V5ExecutionRequest(BaseModel):
    connection_id: str
    signal_id: str


@router.get("/config")
def config(request: Request):
    current_user(request)
    return public_config()


@router.get("/binary/v5/signal")
def signal(request: Request):
    current_user(request)
    return latest_relay_signal()


@router.post("/oauth/state")
def oauth_state(payload: OAuthStateRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return bind_oauth_state(user.id, payload.state.strip(), payload.code_verifier.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/oauth/exchange")
def oauth_exchange(payload: OAuthExchangeRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return exchange_authorization_code(
            payload.code.strip(), payload.code_verifier.strip(), user_id=user.id,
            oauth_state=payload.state.strip(), selected_account_id=payload.selected_account_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/status")
def status(payload: ConnectionRequest, request: Request):
    user = current_user(request)
    try:
        return connection_snapshot(payload.connection_id.strip(), user_id=user.id)
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/disconnect")
def user_disconnect(payload: ConnectionRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        disconnect(payload.connection_id.strip(), user.id)
        return {"ok": True, "connected": False}
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/account/select")
def account_select(payload: AccountSelectionRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return set_selected_account(payload.connection_id.strip(), user.id, payload.deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.get("/binary/account-settings/{deriv_account_id}")
def get_account_settings(deriv_account_id: str, request: Request):
    user = current_user(request)
    try:
        return account_settings(user.id, deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/binary/account-settings")
def update_account_settings(payload: AccountSettingsRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return save_account_settings(user.id, payload.deriv_account_id.strip(), enabled=payload.enabled, stake=payload.stake)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/binary/execution-status/{deriv_account_id}")
def execution_status(deriv_account_id: str, request: Request):
    user = current_user(request)
    try:
        return account_execution_snapshot(user.id, deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/binary/history/{deriv_account_id}")
def history(deriv_account_id: str, request: Request, limit: int = 50, offset: int = 0):
    user = current_user(request)
    try:
        return execution_history(
            user.id,
            deriv_account_id.strip(),
            limit=max(1, min(int(limit), 100)),
            offset=max(0, int(offset)),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/binary/v5/execute")
def execute(payload: V5ExecutionRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return execute_relayed_signal(user.id, payload.connection_id.strip(), payload.signal_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Deriv Binary execution failed: {exc}") from exc
