from fastapi import APIRouter

from ctrader_connector import get_live_prices, start_ctrader_live_price_stream
from services.ctrader_service import get_health_snapshot

router = APIRouter()


@router.get("/health/ctrader")
def ctrader_health():
    return {
        "ok": True,
        **get_health_snapshot(),
    }


@router.get("/chart/live-ticks")
def live_chart_ticks():
    """Latest cTrader spot snapshots for visual candle updates only."""
    start_ctrader_live_price_stream()
    status = get_live_prices() or {}
    return {
        "ok": True,
        "source": "ctrader",
        "live_prices": status.get("live_prices", {}),
        "live_price_health": status.get("live_price_health"),
        "live_price_stale_symbols": status.get("live_price_stale_symbols", []),
        "live_price_last_update": status.get("live_price_last_update"),
    }
