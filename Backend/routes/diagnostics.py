from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from services.strategy_diagnostics_service import query_cycles


router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])


@router.get("/strategy-cycles")
def strategy_cycles(
    symbol: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    decision: str | None = None,
    block_reason: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    normalized_symbol = str(symbol or "").upper() or None
    if normalized_symbol and normalized_symbol not in {"EURUSD", "XAUUSD"}:
        raise HTTPException(status_code=422, detail="symbol must be EURUSD or XAUUSD")
    rows = query_cycles(
        symbol=normalized_symbol,
        start=start,
        end=end,
        decision=decision,
        block_reason=block_reason,
        limit=limit,
        offset=offset,
    )
    return {
        "items": rows,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "read_only": True,
    }
