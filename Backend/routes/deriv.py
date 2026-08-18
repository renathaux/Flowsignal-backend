from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.deriv_service import (
    assert_demo_connection,
    connection_snapshot,
    disconnect,
    exchange_authorization_code,
    public_config,
)
from services.deriv_demo_execution_service import (
    execute_demo_signal,
    execution_snapshot,
)
from services.deriv_binary_strategy_service import binary_signal_snapshot

router = APIRouter(prefix="/deriv", tags=["deriv"])


class DerivExchangeRequest(BaseModel):
    code: str
    code_verifier: str


class DerivConnectionRequest(BaseModel):
    connection_id: str


class DerivDemoSignalRequest(BaseModel):
    connection_id: str
    signal: str
    signal_id: str


@router.get("/config")
def get_deriv_config():
    return public_config()


@router.get("/binary/signal")
def get_deriv_binary_signal():
    """Return the isolated Deriv-native 5m RISE/FALL/WAIT decision."""
    return binary_signal_snapshot()


@router.post("/oauth/exchange")
def exchange_deriv_code(request: DerivExchangeRequest):
    code = request.code.strip()
    verifier = request.code_verifier.strip()
    if not code or len(verifier) < 43:
        raise HTTPException(status_code=400, detail="Invalid OAuth code or PKCE verifier")
    try:
        return exchange_authorization_code(code, verifier)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/status")
def get_deriv_status(request: DerivConnectionRequest):
    return connection_snapshot(request.connection_id.strip())


@router.post("/disconnect")
def disconnect_deriv(request: DerivConnectionRequest):
    disconnect(request.connection_id.strip())
    return {"ok": True, "connected": False, "demo_only": True}


@router.post("/demo/guard")
def verify_demo_guard(request: DerivConnectionRequest):
    try:
        snapshot = assert_demo_connection(request.connection_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {
        "ok": True,
        "demo_only": True,
        "execution_enabled": True,
        "real_money_enabled": False,
        "message": "Demo account verified. Binary execution is demo-only.",
        "demo_accounts": snapshot.get("demo_accounts", []),
    }


@router.post("/demo/execute-signal")
def execute_binary_demo_signal(request: DerivDemoSignalRequest):
    """Execute one isolated EURUSD Rise/Fall contract on Deriv demo only."""
    try:
        return execute_demo_signal(
            request.connection_id.strip(),
            request.signal,
            request.signal_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Deriv demo execution failed: {exc}") from exc


@router.post("/demo/execution-status")
def get_binary_demo_execution_status(request: DerivConnectionRequest):
    try:
        return execution_snapshot(request.connection_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
