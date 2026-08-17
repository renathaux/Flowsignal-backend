from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query

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
