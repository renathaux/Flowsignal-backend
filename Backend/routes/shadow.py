from fastapi import APIRouter, HTTPException, Query

from services.shadow_retention_service import cleanup_shadow_history_safely
from services.v1_outcome_reconciliation_service import get_v1_actual_metrics
from services.v1_outcome_reconciliation_hotfix_service import reconcile_v1_outcomes_safely
from services.v2_shadow_service import (
    get_shadow_history,
    get_shadow_summary,
)
from services.v2_risk_alignment import install_v2_risk_alignment


# V2 remains shadow-only. This aligns its hypothetical risk plan with the same
# configured minimum SL distance and RR window used by V1/live validation.
install_v2_risk_alignment()


router = APIRouter(prefix="/shadow/v2", tags=["strategy-v2-shadow"])


def _symbol(value):
    normalized = str(value or "").upper()
    if normalized not in {"EURUSD", "XAUUSD"}:
        raise HTTPException(status_code=422, detail="symbol must be EURUSD or XAUUSD")
    return normalized


@router.get("/summary")
def summary(symbol: str = "XAUUSD"):
    normalized = _symbol(symbol)
    retention = cleanup_shadow_history_safely()
    reconciliation = reconcile_v1_outcomes_safely()
    payload = get_shadow_summary(normalized)
    payload["v1"].update(get_v1_actual_metrics(normalized))
    payload["v1"]["reconciliation"] = reconciliation
    payload["retention"] = retention
    return payload


@router.get("/history")
def history(
    symbol: str | None = None,
    decision: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    normalized = _symbol(symbol) if symbol else None
    retention = cleanup_shadow_history_safely()
    reconciliation = reconcile_v1_outcomes_safely()
    items = get_shadow_history(normalized, decision, limit, offset)
    return {
        "items": items,
        "count": len(items),
        "limit": limit,
        "offset": offset,
        "read_only": True,
        "warning": "SHADOW — DOES NOT PLACE ORDERS",
        "retention": retention,
        "v1_actual": get_v1_actual_metrics(normalized),
        "v1_reconciliation": reconciliation,
    }
