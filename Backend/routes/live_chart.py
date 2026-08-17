"""Read-only cTrader tick snapshot endpoint for the visual chart.

This route deliberately has no trading/execution actions.  It exposes the
latest prices already maintained by the cTrader live-price thread so the web
chart can update its forming candle without waiting for the 15-second panel
refresh.
"""

from fastapi import APIRouter

from ctrader_connector import get_live_prices, start_ctrader_live_price_stream

router = APIRouter(prefix="/chart", tags=["chart"])


@router.get("/live-ticks")
def live_chart_ticks():
    # Idempotent: ensures the existing cTrader spot subscription is running.
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
