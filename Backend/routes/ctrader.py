from fastapi import APIRouter

from services.ctrader_service import get_health_snapshot

router = APIRouter()


@router.get("/health/ctrader")
def ctrader_health():
    return {
        "ok": True,
        **get_health_snapshot(),
    }
