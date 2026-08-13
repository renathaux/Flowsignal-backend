from fastapi import APIRouter, HTTPException, Query

from services.v2_shadow_service import (
    get_shadow_history,
    get_shadow_summary,
)


router = APIRouter(prefix="/shadow/v2", tags=["strategy-v2-shadow"])


def _symbol(value):
    normalized = str(value or "").upper()
    if normalized not in {"EURUSD", "XAUUSD"}:
        raise HTTPException(status_code=422, detail="symbol must be EURUSD or XAUUSD")
    return normalized


@router.get("/summary")
def summary(symbol: str = "XAUUSD"):
    return get_shadow_summary(_symbol(symbol))


@router.get("/history")
def history(
    symbol: str | None = None,
    decision: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    normalized = _symbol(symbol) if symbol else None
    items = get_shadow_history(normalized, decision, limit, offset)
    return {
        "items": items,
        "count": len(items),
        "limit": limit,
        "offset": offset,
        "read_only": True,
        "warning": "SHADOW — DOES NOT PLACE ORDERS",
    }
