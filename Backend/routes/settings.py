from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.deriv_service import (
    assert_demo_connection,
    connection_snapshot,
    disconnect,
    exchange_authorization_code,
    public_config,
)
from services.risk_service import get_risk_settings, update_risk_settings
from services.settings_service import load_feature_flags, save_feature_flags

router = APIRouter()


class DerivExchangeRequest(BaseModel):
    code: str
    code_verifier: str


class DerivConnectionRequest(BaseModel):
    connection_id: str


# Binary/Deriv mini-app endpoints live here only because this router is already
# mounted by api.py. They do not touch Forex/cTrader execution state.
@router.get("/deriv/config")
def deriv_config_get():
    return public_config()


@router.post("/deriv/oauth/exchange")
def deriv_oauth_exchange(request: DerivExchangeRequest):
    code = request.code.strip()
    verifier = request.code_verifier.strip()
    if not code or len(verifier) < 43:
        raise HTTPException(status_code=400, detail="Invalid OAuth code or PKCE verifier")
    try:
        return exchange_authorization_code(code, verifier)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/deriv/status")
def deriv_status(request: DerivConnectionRequest):
    return connection_snapshot(request.connection_id.strip())


@router.post("/deriv/disconnect")
def deriv_disconnect(request: DerivConnectionRequest):
    disconnect(request.connection_id.strip())
    return {"ok": True, "connected": False, "demo_only": True}


@router.post("/deriv/demo/guard")
def deriv_demo_guard(request: DerivConnectionRequest):
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


@router.get("/settings/risk")
def settings_risk_get():
    return {
        "ok": True,
        "risk": get_risk_settings(),
    }


@router.post("/settings/risk")
def settings_risk_post(payload: dict):
    try:
        risk = update_risk_settings(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "ok": True,
        "risk": risk,
    }


@router.get("/feature-flags")
def feature_flags_get():
    return {
        "ok": True,
        "flags": load_feature_flags(),
    }


@router.post("/feature-flags")
def feature_flags_post(payload: dict):
    return {
        "ok": True,
        "flags": save_feature_flags(payload),
    }
