from fastapi import APIRouter, HTTPException
from services.risk_service import get_risk_settings, update_risk_settings
from services.settings_service import load_feature_flags, save_feature_flags
from routes.user_auth import router as user_auth_router
from routes.password_reset import router as password_reset_router

router = APIRouter()
# api.py already mounts this router. Include the database-backed auth routers
# here so customer signup/login/verification/session/password reset are exposed.
router.include_router(user_auth_router)
router.include_router(password_reset_router)


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
