from fastapi import APIRouter, HTTPException, Query

from fundamentals.insight_service import get_fundamental_insight
from fundamentals.repositories.observations import provider_health


router = APIRouter(prefix="/fundamentals", tags=["fundamentals"])


@router.get("/insight")
def fundamental_insight(symbol: str = Query(default="EURUSD")):
    normalized = str(symbol or "").upper().replace("/", "")
    if normalized != "EURUSD":
        raise HTTPException(
            status_code=422,
            detail="Fundamental Insight currently supports EURUSD only.",
        )
    return get_fundamental_insight(normalized)


@router.get("/health")
def fundamental_health():
    try:
        return provider_health()
    except Exception as exc:
        return {
            "engine_readiness": "NOT_READY",
            "status": "UNAVAILABLE",
            "error": str(exc),
        }
