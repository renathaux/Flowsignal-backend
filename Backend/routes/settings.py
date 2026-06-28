from fastapi import APIRouter, HTTPException

from services.risk_service import get_risk_settings, update_risk_settings
from services.settings_service import load_feature_flags, save_feature_flags

router = APIRouter()


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
