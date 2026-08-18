from datetime import datetime, timedelta, timezone
import copy
import time

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
    """Serve the dashboard from in-memory cache only.

    This route intentionally does not call api.panel_data(). The heavy endpoint
    can perform broker/database/meta refresh work that may block during cTrader
    recovery. The trading engine continues refreshing the authoritative cache in
    its background thread; the browser only reads the latest confirmed snapshot.

    No strategy, signal, execution, risk, SL/TP, or Binary logic is changed.
    """
    import api

    cached = api.PANEL_CACHE.get("data")
    if not isinstance(cached, dict):
        cached = api.default_panel()
    data = copy.deepcopy(cached)

    now = time.time()
    last_update = float(api.PANEL_CACHE.get("last_update") or 0)
    age = max(now - last_update, 0) if last_update else 0
    refresh_state = dict(api.PANEL_REFRESH_STATE or {})
    live_meta = api.LIVE_PANEL_META_CACHE or {}
    live_pl = dict(live_meta.get("live_pl_sync") or {})

    # Preserve the top-level P/L fields expected by the existing frontend, but
    # read them only from the already-populated metadata cache.
    for key in (
        "weekly_realized_pl",
        "daily_realized_pl",
        "daily_total_pl",
        "monthly_realized_pl",
        "floating_live_pl",
        "weekly_total_pl",
    ):
        data[key] = live_pl.get(key, data.get(key, 0))

    paper_enabled = bool(getattr(api, "AUTO_TRADE_ENABLED", {}).get("enabled", False))
    live_enabled = bool(getattr(api, "LIVE_AUTO_TRADE_ENABLED", {}).get("enabled", False))
    live_account = copy.deepcopy(getattr(api, "LIVE_ACCOUNT_STATE", {}))
    live_orders = copy.deepcopy(getattr(api, "LIVE_ACTIVE_ORDERS", {}))
    live_positions = copy.deepcopy(live_meta.get("live_positions") or [])

    data["_meta"] = {
        "source": "shared_cache_nonblocking",
        "cache_age_seconds": round(age, 1),
        "stale_data": bool(refresh_state.get("last_error") or not last_update),
        "last_successful_refresh": refresh_state.get("last_success"),
        "refresh_seconds": getattr(api, "CACHE_SECONDS", 15),
        "error": refresh_state.get("last_error"),
        "brain_refresh": refresh_state,
        "live_meta_last_update": live_meta.get("last_update"),
        "live_meta_error": live_meta.get("last_error"),
        "paper_auto_enabled": paper_enabled,
        "live_auto_enabled": live_enabled,
        "auto_trade_state": {
            "paper_enabled": paper_enabled,
            "live_enabled": live_enabled,
            "source": "memory_cache",
        },
        "live_account": live_account,
        "live_active_orders": live_orders,
        "broker_open_positions_count": len(live_positions),
        "live_trade_history": copy.deepcopy(live_meta.get("live_recent_history") or []),
        "live_trade_stats": {
            **copy.deepcopy(live_meta.get("live_trade_stats") or {}),
            **live_pl,
        },
        "weekly_realized_pl": live_pl.get("weekly_realized_pl", 0),
        "daily_realized_pl": live_pl.get("daily_realized_pl", 0),
        "daily_total_pl": live_pl.get("daily_total_pl", 0),
        "monthly_realized_pl": live_pl.get("monthly_realized_pl", 0),
        "floating_live_pl": live_pl.get("floating_live_pl", 0),
        "weekly_total_pl": live_pl.get("weekly_total_pl", 0),
        "live_price_status": copy.deepcopy(live_meta.get("live_price_status") or {}),
        "nonblocking_cache_only": True,
        "force_requested": bool(force),
    }

    # Keep response serialization defensive, exactly like the original endpoint.
    safe = getattr(api, "_json_safe_panel_value", None)
    return safe(data) if callable(safe) else data


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
