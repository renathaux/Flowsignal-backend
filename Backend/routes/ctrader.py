from datetime import datetime, timedelta, timezone
import time

from fastapi import APIRouter, HTTPException, Query

from services.ctrader_transport_guard import install_ctrader_transport_guard
from services.trade_signal_lifecycle_guard import (
    clone_panel_for_transport,
    install_trade_signal_lifecycle_guard,
)

# Install before importing/using cTrader market-data functions. The guard only
# bounds read/auth requests; broker order/amend/close payloads are untouched.
install_ctrader_transport_guard()

from ctrader_connector import (
    get_ctrader_market_data,
    get_live_prices,
    get_symbol_risk_fallback,
    start_ctrader_live_price_stream,
)
from indicators.smc import analyze_structure as analyze_xauusd_structure
from indicators.smc.legacy_engine import analyze_structure as analyze_legacy_structure
from services.ctrader_service import get_health_snapshot

router = APIRouter()

_ALLOWED_SYMBOLS = {"EURUSD", "XAUUSD"}
_TIMEFRAME_MINUTES = {"5m": 5, "15m": 15, "1h": 60}


@router.on_event("startup")
def _install_trade_signal_lifecycle_guard():
    # api.py is fully imported by startup time, so the lifecycle can be wrapped
    # without a circular import during module initialization.
    install_trade_signal_lifecycle_guard()


@router.get("/health/ctrader")
def ctrader_health():
    return {
        "ok": True,
        **get_health_snapshot(),
    }


@router.get("/panel-data", include_in_schema=False)
@router.get("/dashboard-feed", include_in_schema=False)
def nonblocking_dashboard_feed(force: int = 0):
    """Serve browser dashboard reads from in-memory cache only.

    Both URLs intentionally use this lightweight route so old cached browsers
    cannot fall through to the heavyweight legacy handler. All cloning here is
    bounded/cycle-safe, so a recursive diagnostic object can never turn the
    dashboard into NO DATA.

    No strategy parameters, signal rules, broker execution, risk, SL/TP, or
    Binary logic are changed.
    """
    import api

    try:
        cached = api.PANEL_CACHE.get("data")
        if not isinstance(cached, dict):
            cached = api.default_panel()
        data = clone_panel_for_transport(cached)
        if not isinstance(data, dict):
            data = api.default_panel()

        now = time.time()
        last_update = float(api.PANEL_CACHE.get("last_update") or 0)
        age = max(now - last_update, 0) if last_update else 0
        refresh_state = clone_panel_for_transport(api.PANEL_REFRESH_STATE or {})
        if not isinstance(refresh_state, dict):
            refresh_state = {}
        live_meta = api.LIVE_PANEL_META_CACHE or {}
        live_pl = clone_panel_for_transport(live_meta.get("live_pl_sync") or {})
        if not isinstance(live_pl, dict):
            live_pl = {}

        for key in (
            "weekly_realized_pl",
            "daily_realized_pl",
            "daily_total_pl",
            "monthly_realized_pl",
            "floating_live_pl",
            "weekly_total_pl",
        ):
            data[key] = live_pl.get(key, data.get(key, 0))

        def _enabled(value):
            if isinstance(value, dict):
                return bool(value.get("enabled", False))
            return bool(value)

        paper_enabled = _enabled(getattr(api, "AUTO_TRADE_ENABLED", False))
        live_enabled = _enabled(getattr(api, "LIVE_AUTO_TRADE_ENABLED", False))
        live_account = clone_panel_for_transport(
            getattr(api, "LIVE_ACCOUNT_STATE", {}) or {}
        )
        live_orders = clone_panel_for_transport(
            getattr(api, "LIVE_ACTIVE_ORDERS", {}) or {}
        )
        live_positions = clone_panel_for_transport(
            live_meta.get("live_positions") or []
        )
        live_recent_history = clone_panel_for_transport(
            live_meta.get("live_recent_history") or []
        )
        live_trade_stats = clone_panel_for_transport(
            live_meta.get("live_trade_stats") or {}
        )
        live_price_status = clone_panel_for_transport(
            live_meta.get("live_price_status") or {}
        )

        if not isinstance(live_positions, list):
            live_positions = []
        if not isinstance(live_recent_history, list):
            live_recent_history = []
        if not isinstance(live_trade_stats, dict):
            live_trade_stats = {}
        if not isinstance(live_price_status, dict):
            live_price_status = {}

        data["_meta"] = {
            "source": "dashboard_feed_cache_only_cycle_safe",
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
            "live_account": live_account if isinstance(live_account, dict) else {},
            "live_active_orders": live_orders if isinstance(live_orders, dict) else {},
            "broker_open_positions_count": len(live_positions),
            "live_trade_history": live_recent_history,
            "live_trade_stats": {
                **live_trade_stats,
                **live_pl,
            },
            "weekly_realized_pl": live_pl.get("weekly_realized_pl", 0),
            "daily_realized_pl": live_pl.get("daily_realized_pl", 0),
            "daily_total_pl": live_pl.get("daily_total_pl", 0),
            "monthly_realized_pl": live_pl.get("monthly_realized_pl", 0),
            "floating_live_pl": live_pl.get("floating_live_pl", 0),
            "weekly_total_pl": live_pl.get("weekly_total_pl", 0),
            "live_price_status": live_price_status,
            "nonblocking_cache_only": True,
            "cycle_safe_transport": True,
            "legacy_panel_alias": True,
            "force_requested": bool(force),
        }

        safe = getattr(api, "_json_safe_panel_value", None)
        return safe(data) if callable(safe) else data
    except Exception as exc:
        fallback = api.default_panel()
        fallback["_meta"] = {
            "source": "dashboard_feed_failsafe",
            "stale_data": True,
            "error": f"cache read failed: {type(exc).__name__}",
            "nonblocking_cache_only": True,
            "cycle_safe_transport": True,
            "legacy_panel_alias": True,
        }
        return fallback


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
    structure_analyzer = (
        analyze_xauusd_structure
        if normalized_symbol == "XAUUSD"
        else analyze_legacy_structure
    )
    point_size = get_symbol_risk_fallback(normalized_symbol).get("tick_size")
    structure = structure_analyzer(
        closed,
        left_bars=2,
        right_bars=2,
        timeframe=normalized_timeframe,
        point_size=point_size,
    )
    structure.update({
        "symbol": normalized_symbol,
        "timeframe": normalized_timeframe,
        "source": "ctrader_closed_candles",
        "closed_candle_count": int(len(closed)) if closed is not None else 0,
        "observation_only": True,
        "affects_strategy": False,
    })
    return structure
