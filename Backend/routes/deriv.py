from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel, conint

from services.deriv_service import (
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
        print("BINARY_RELAY_REJECTED =", {
            "reason": str(exc),
            "body_preview": body.decode("utf-8", errors="replace")[:500],
            "has_timestamp": bool(request.headers.get("X-FlowSignal-Relay-Timestamp", "")),
            "has_signature": bool(request.headers.get("X-FlowSignal-Relay-Signature", "")),
        })
        raise HTTPException(status_code=403, detail=str(exc)) from exc


class DerivExchangeRequest(BaseModel):
    code: str
    code_verifier: str
    user_id: str
    selected_account_id: str | None = None


class DerivConnectionRequest(BaseModel):
    connection_id: str


class DerivAccountSelectionRequest(BaseModel):
    connection_id: str
    user_id: str
    deriv_account_id: str


class BinaryAccountSettingsRequest(BaseModel):
    user_id: str
    deriv_account_id: str
    enabled: bool
    stake: float
    duration_minutes: conint(strict=True, ge=1, le=60) | None = None


class BinaryV5ExecutionRequest(BaseModel):
    connection_id: str
    user_id: str
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


@router.post("/oauth/exchange")
def exchange_deriv_code(request: DerivExchangeRequest):
    code = request.code.strip()
    verifier = request.code_verifier.strip()
    if not code or len(verifier) < 43:
        raise HTTPException(status_code=400, detail="Invalid OAuth code or PKCE verifier")
    try:
        return exchange_authorization_code(code, verifier, user_id=request.user_id.strip(), selected_account_id=request.selected_account_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/status")
def get_deriv_status(request: DerivConnectionRequest):
    return connection_snapshot(request.connection_id.strip())


@router.post("/disconnect")
def disconnect_deriv(request: DerivConnectionRequest):
    disconnect(request.connection_id.strip())
    return {"ok": True, "connected": False, "demo_only": True}


@router.post("/account/select")
def select_deriv_account(request: DerivAccountSelectionRequest):
    try:
        return set_selected_account(request.connection_id.strip(), request.user_id.strip(), request.deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/binary/account-settings")
def update_binary_account_settings(request: BinaryAccountSettingsRequest):
    try:
        return save_account_settings(
            request.user_id.strip(), request.deriv_account_id.strip(),
            enabled=request.enabled, stake=request.stake,
            duration_minutes=request.duration_minutes,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/binary/account-settings/{user_id}/{deriv_account_id}")
def get_binary_account_settings(user_id: str, deriv_account_id: str):
    try:
        return account_settings(user_id.strip(), deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/binary/v5/execute")
def execute_authoritative_v5(request: BinaryV5ExecutionRequest):
    try:
        return execute_relayed_signal(request.user_id.strip(), request.connection_id.strip(), request.signal_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Deriv Binary execution failed: {exc}") from exc


@router.get("/binary/execution-status/{user_id}/{deriv_account_id}")
def get_account_execution_status(user_id: str, deriv_account_id: str):
    try:
        return account_execution_snapshot(user_id.strip(), deriv_account_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/binary/history/{user_id}/{deriv_account_id}")
def get_binary_history(user_id: str, deriv_account_id: str, limit: int = 50, offset: int = 0):
    try:
        return execution_history(
            user_id.strip(),
            deriv_account_id.strip(),
            limit=max(1, min(int(limit), 100)),
            offset=max(0, int(offset)),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
