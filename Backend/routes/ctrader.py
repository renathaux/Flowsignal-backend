from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query

from services.ctrader_transport_guard import install_ctrader_transport_guard

# Install before importing/using cTrader market-data functions. The guard only
# bounds read/auth requests; broker order/amend/close payloads are untouched.
install_ctrader_transport_guard()

from ctrader_connector import (
    get_ctrader_market_data,
    get_live_prices,
    start_ctrader_live_price_stream,
)
from indicators.smc import analyze_structure
from services.ctrader_service import get_health_snapshot

router = APIRouter()

_ALLOWED_SYMBOLS = {"EURUSD", "XAUUSD"}
_TIMEFRAME_MINUTES = {"5m": 5, "15m": 15, "1h": 60}


@router.get("/health/ctrader")
def ctrader_health():
    return {
        "ok": True,
        **get_health_snapshot(),
    }


@router.get("/panel-data", include_in_schema=False)
def nonblocking_panel_data(force: int = 0):
    """Return the dashboard cache immediately during broker recovery.

    The original panel endpoint can wait up to 55 seconds for an invalid/cold
    cache. Browser requests should not be held open that long. Normal dashboard
    reads use a zero initial-wait timeout while the existing background refresh
    continues. Explicit force=1 keeps the original synchronous diagnostic path.

    No strategy, signal, broker execution, risk, SL/TP, or Binary behavior is
    changed here; this is only an HTTP delivery guard for the dashboard cache.
    """
    import api

    if int(force or 0) == 1:
        return api.panel_data(force=1)

    original_wait = api.PANEL_INITIAL_DATA_WAIT_SECONDS
    try:
        api.PANEL_INITIAL_DATA_WAIT_SECONDS = 0
        return api.panel_data(force=0)
    finally:
        api.PANEL_INITIAL_DATA_WAIT_SECONDS = original_wait


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


def _closed_only(frame, timeframe):
    """Remove the currently forming bar before SMC analysis."""
    if frame is None or frame.empty:
        return frame
    minutes = _TIMEFRAME_MINUTES[timeframe]
    cutoff = datetime.now(timezone.utc)
    data = frame.copy()
    data.index = data.index.map(
        lambda value: value if getattr(value, "tzinfo", None) else value.tz_localize("UTC")
    )
    return data[
        data.index.map(lambda value: value.to_pydatetime() + timedelta(minutes=minutes) <= cutoff)
    ]


@router.get("/chart/smc-structure")
def chart_smc_structure(
    symbol: str = Query(default="EURUSD"),
    timeframe: str = Query(default="15m"),
    limit: int = Query(default=250, ge=50, le=500),
):
    """Read-only, closed-candle SMC structure for the visual chart overlay.

    This endpoint never places, modifies, closes, or authorizes broker orders.
    It deliberately removes the forming candle before calculating swings,
    BOS, and CHoCH so the visual indicator cannot repaint from live ticks.
    """
    normalized_symbol = str(symbol or "").upper()
    normalized_timeframe = str(timeframe or "").lower()
    if normalized_symbol not in _ALLOWED_SYMBOLS:
        raise HTTPException(status_code=422, detail="symbol must be EURUSD or XAUUSD")
    if normalized_timeframe not in _TIMEFRAME_MINUTES:
        raise HTTPException(status_code=422, detail="timeframe must be 5m, 15m, or 1h")

    frame = get_ctrader_market_data(
        normalized_symbol,
        normalized_timeframe,
        limit=limit,
    )
    closed = _closed_only(frame, normalized_timeframe)
    structure = analyze_structure(closed, left_bars=2, right_bars=2)
    structure.update({
        "symbol": normalized_symbol,
        "timeframe": normalized_timeframe,
        "source": "ctrader_closed_candles",
        "closed_candle_count": int(len(closed)) if closed is not None else 0,
        "observation_only": True,
        "affects_strategy": False,
    })
    return structure
