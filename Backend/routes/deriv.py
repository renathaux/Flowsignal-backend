from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.deriv_service import (
    assert_demo_connection,
    connection_snapshot,
    disconnect,
    exchange_authorization_code,
    public_config,
)

router = APIRouter(prefix="/deriv", tags=["deriv"])


class DerivExchangeRequest(BaseModel):
    code: str
    code_verifier: str


class DerivConnectionRequest(BaseModel):
    connection_id: str


@router.get("/config")
def get_deriv_config():
    return public_config()


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
    """Safety preflight used before any future Deriv purchase endpoint.

    This deliberately does not place a contract. It proves that the currently
    connected account is explicitly identified as demo before execution code is
    allowed to be added/enabled.
    """
    try:
        snapshot = assert_demo_connection(request.connection_id.strip())
    except RuntimeError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {
        "ok": True,
        "demo_only": True,
        "execution_enabled": False,
        "message": "Demo account verified. Contract purchase endpoint is not enabled yet.",
        "demo_accounts": snapshot.get("demo_accounts", []),
    }
