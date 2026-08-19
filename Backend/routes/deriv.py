from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
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
    execute_signal_candidates,
    execution_snapshot as account_execution_snapshot,
    latest_relay_signal,
    save_account_settings,
)
from services.deriv_binary_history_service import execution_history
from services.deriv_binary_strategy_service import binary_signal_snapshot
from services.deriv_v5_demo_relay_service import receive_signal
from services.user_auth_service import current_user, current_user_with_csrf

router = APIRouter(prefix="/deriv", tags=["deriv"])


@router.post("/v5/demo/relay")
async def receive_v5_demo_relay(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    try:
        result = receive_signal(
            body,
            request.headers.get("X-FlowSignal-Relay-Timestamp", ""),
            request.headers.get("X-FlowSignal-Relay-Signature", ""),
        )
        background_tasks.add_task(execute_signal_candidates, result["signal_id"])
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


class DerivOAuthStateRequest(BaseModel):
    state: str
    code_verifier: str


class DerivExchangeRequest(BaseModel):
    code: str
    code_verifier: str
    state: str
    selected_account_id: str | None = None


class DerivConnectionRequest(BaseModel):
    connection_id: str


class DerivAccountSelectionRequest(BaseModel):
    connection_id: str
    deriv_account_id: str


class BinaryAccountSettingsRequest(BaseModel):
    deriv_account_id: str
    enabled: bool
    stake: float


class BinaryV5ExecutionRequest(BaseModel):
    connection_id: str
    signal_id: str


@router.get("/config")
def get_deriv_config():
    return public_config()


@router.get("/binary/signal")
def get_deriv_binary_signal():
    return binary_signal_snapshot()


@router.get("/binary/v5/signal")
def get_authoritative_v5_signal():
    return latest_relay_signal()


@router.post("/oauth/state")
def register_deriv_oauth_state(payload: DerivOAuthStateRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return bind_oauth_state(user.id, payload.state.strip(), payload.code_verifier.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/oauth/exchange")
def exchange_deriv_code(payload: DerivExchangeRequest, request: Request):
    user = current_user_with_csrf(request)
    code = payload.code.strip()
    verifier = payload.code_verifier.strip()
    state = payload.state.strip()
    if not code or not state or len(verifier) < 43:
        raise HTTPException(status_code=400, detail="Invalid OAuth code, state, or PKCE verifier")
    try:
        return exchange_authorization_code(
            code, verifier, user_id=user.id, oauth_state=state,
            selected_account_id=payload.selected_account_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/status")
def get_deriv_status(payload: DerivConnectionRequest, request: Request):
    user = current_user(request)
    try:
        return connection_snapshot(payload.connection_id.strip(), user_id=user.id)
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/disconnect")
def disconnect_deriv(payload: DerivConnectionRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        disconnect(payload.connection_id.strip(), user.id)
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {"ok": True, "connected": False}


@router.post("/account/select")
def select_deriv_account(payload: DerivAccountSelectionRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return set_selected_account(payload.connection_id.strip(), user.id, payload.deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/binary/account-settings")
def update_binary_account_settings(payload: BinaryAccountSettingsRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return save_account_settings(user.id, payload.deriv_account_id.strip(), enabled=payload.enabled, stake=payload.stake)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/binary/account-settings/{deriv_account_id}")
def get_binary_account_settings(deriv_account_id: str, request: Request):
    user = current_user(request)
    try:
        return account_settings(user.id, deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/binary/v5/execute")
def execute_authoritative_v5(payload: BinaryV5ExecutionRequest, request: Request):
    user = current_user_with_csrf(request)
    try:
        return execute_relayed_signal(user.id, payload.connection_id.strip(), payload.signal_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Deriv Binary execution failed: {exc}") from exc


@router.get("/binary/execution-status/{deriv_account_id}")
def get_account_execution_status(deriv_account_id: str, request: Request):
    user = current_user(request)
    try:
        return account_execution_snapshot(user.id, deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/binary/history/{deriv_account_id}")
def get_binary_history(deriv_account_id: str, request: Request, limit: int = 50):
    user = current_user(request)
    try:
        return execution_history(user.id, deriv_account_id.strip(), limit=max(1, min(int(limit), 100)))
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
