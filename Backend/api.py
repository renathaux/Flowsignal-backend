from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from email.mime.text import MIMEText
import smtplib
import time
import threading
import json
import copy
import os
import hashlib
import uuid
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from ctrader_connector import (
    CTRADER_PAYLOAD_VOLUME_SCALE,
    build_ctrader_authorization_url,
    clear_ctrader_saved_accounts,
    close_position,
    connect_account,
    convert_ctrader_volume_to_lots,
    convert_lots_to_ctrader_volume,
    disconnect_account,
    exchange_ctrader_authorization_code,
    fetch_ctrader_accounts,
    forget_ctrader_account,
    get_connection_state,
    get_ctrader_account_snapshot,
    get_closed_deals_for_current_week,
    get_closed_deals_for_current_month,
    get_ctrader_diagnostics,
    get_ctrader_redirect_uri_debug,
    get_ctrader_refresh_token_status,
    get_live_prices,
    get_ctrader_position_fetch_error,
    get_ctrader_symbol_risk_metadata,
    get_open_positions,
    modify_position_sltp,
    modify_position_stop_loss,
    normalize_symbol,
    normalize_trade_levels,
    parse_ctrader_money,
    place_market_order,
    remove_debug_open_position,
    set_active_ctrader_account,
    set_debug_open_positions,
    start_ctrader_live_price_stream
)
from routes.ctrader import router as ctrader_router
from routes.performance import (
    configure_performance_data_provider,
    router as performance_router,
)
from routes.settings import router as settings_router
from routes.trading import router as trading_router
from news_service import get_news_impact
from services.settings_service import load_feature_flags, load_risk_settings

FINAL_SIGNAL_HOLD_FILE = os.path.join(
    os.path.dirname(__file__),
    "final_signal_hold.json"
)

def clear_persisted_final_signal_hold(symbol, reason="trade executed"):
    try:
        if not os.path.exists(FINAL_SIGNAL_HOLD_FILE):
            return

        with open(FINAL_SIGNAL_HOLD_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return

        hold_key = normalize_symbol(symbol)

        if hold_key in data:
            data.pop(hold_key, None)

            with open(FINAL_SIGNAL_HOLD_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)

            try:
                import brain

                brain.FINAL_SIGNAL_HOLD.pop(hold_key, None)
            except Exception as brain_exc:
                print("FINAL_SIGNAL_HOLD_MEMORY_RELEASE_WARNING =", {
                    "symbol": hold_key,
                    "error": str(brain_exc),
                })

            print("FINAL_SIGNAL_HOLD_CONSUMED =", {
                "symbol": hold_key,
                "reason": reason,
            })
    except Exception as exc:
        print("FINAL_SIGNAL_HOLD_RELEASE_ERROR =", {
            "symbol": symbol,
            "error": str(exc),
        })

app = FastAPI()
app.include_router(settings_router)
app.include_router(performance_router)
app.include_router(ctrader_router)
app.include_router(trading_router)

@app.on_event("startup")
def start_background_task():
    print("Startup OK - warming panel cache")
    warm_panel_cache_from_persisted_candles()
    try:
        start_ctrader_live_price_stream()
    except Exception as exc:
        print("CTRADER_LIVE_STREAM_START_ERROR =", str(exc))
    thread = threading.Thread(target=background_fetch)
    thread.daemon = True
    thread.start()
    
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TradeRequest(BaseModel):
    symbol: str
    action: str
    token: str

class FeedbackRequest(BaseModel):
    message: str
    user: str | None = None
    time: str | None = None

class SignupRequest(BaseModel):
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

class DebugBrokerPositionsRequest(BaseModel):
    positions: list

# =========================
# DEFAULT PANEL DATA
# =========================
def default_panel():
    return {
        "EURUSD": {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (startup)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "UNKNOWN",
            "entry_quality": "WEAK"
        },
        "XAUUSD": {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (startup)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "UNKNOWN",
            "entry_quality": "WEAK"
        }
    }


# =========================
# CACHE
# =========================
PANEL_CACHE = {
    "data": default_panel(),
    "last_update": 0
}
PANEL_REFRESH_LOCK = threading.Lock()
PANEL_REFRESH_STATE = {
    "running": False,
    "last_started": None,
    "last_success": None,
    "last_error": None,
    "last_duration_seconds": None,
    "reason": None,
    "last_source": None,
}
LIVE_PANEL_META_CACHE = {
    "live_positions": [],
    "live_price_status": {
        "live_prices": {},
        "live_price_health": "starting",
        "live_price_last_update": None,
    },
        "live_pl_sync": {
        "daily_realized_pl": 0,
        "daily_total_pl": 0,
        "weekly_realized_pl": 0,
        "monthly_realized_pl": 0,
        "floating_live_pl": 0,
        "weekly_total_pl": 0,
        "open_positions_count": 0,
    },
    "live_recent_history": [],
    "live_trade_stats": {},
    "last_execution_time": 0,
    "last_update": None,
    "last_error": None,
}
LIVE_MONTHLY_HISTORY_CACHE = {
    "history": [],
    "updated_at": 0,
    "month_key": None,
}
LIVE_MONTHLY_HISTORY_FILE = os.path.join(
    os.path.dirname(__file__),
    "live_monthly_history.json",
)


def load_live_monthly_history_cache():
    try:
        if not os.path.exists(LIVE_MONTHLY_HISTORY_FILE):
            return

        with open(LIVE_MONTHLY_HISTORY_FILE, "r", encoding="utf-8") as file:
            saved = json.load(file)

        current_month = datetime.now(LIVE_MARKET_TIMEZONE).strftime("%Y-%m")
        if saved.get("month_key") != current_month:
            return

        history = saved.get("history")
        if not isinstance(history, list):
            return

        LIVE_MONTHLY_HISTORY_CACHE.update({
            "history": history,
            "updated_at": float(saved.get("updated_at") or 0),
            "month_key": current_month,
        })
        print("LIVE_MONTHLY_HISTORY_CACHE_LOADED =", {
            "month_key": current_month,
            "closed_trades": len(history),
        })
    except Exception as exc:
        print("LIVE_MONTHLY_HISTORY_CACHE_LOAD_ERROR:", exc)


def save_live_monthly_history_cache():
    try:
        with open(LIVE_MONTHLY_HISTORY_FILE, "w", encoding="utf-8") as file:
            json.dump(LIVE_MONTHLY_HISTORY_CACHE, file, indent=2)
    except Exception as exc:
        print("LIVE_MONTHLY_HISTORY_CACHE_SAVE_ERROR:", exc)

CACHE_SECONDS = 15
PANEL_REFRESH_STUCK_SECONDS = 45
ADMIN_TOKEN = "N2415"
FEEDBACK_EMAIL = "flowsignal.contact@gmail.com"
FEEDBACK_APP_PASSWORD = "wwro vjjg grzt vpcp"
USERS_FILE = "users.json"
VISITS_FILE = "visits.json"
SESSIONS = {}

def load_users():
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_users(users):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2)
def load_visits():
    if not os.path.exists(VISITS_FILE):
        return []

    try:
        with open(VISITS_FILE, "r") as f:
            return json.load(f)
    except:
        return []


def save_visits(visits):
    with open(VISITS_FILE, "w") as f:
        json.dump(visits, f, indent=2)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()
def _is_valid_panel_payload(data):
    return (
        isinstance(data, dict)
        and isinstance(data.get("EURUSD"), dict)
        and isinstance(data.get("XAUUSD"), dict)
    )

def _panel_candle_counts(data):
    candles = data.get("candles") if isinstance(data, dict) else {}
    counts = {}

    for symbol in ["EURUSD", "XAUUSD"]:
        symbol_candles = (
            candles.get(symbol)
            if isinstance(candles, dict)
            else {}
        )
        counts[symbol] = {
            timeframe: len(symbol_candles.get(timeframe) or [])
            if isinstance(symbol_candles, dict)
            else 0
            for timeframe in ["5m", "15m", "1h"]
        }

    return counts

def _panel_cache_validity(data):
    if not _is_valid_panel_payload(data):
        return {
            "valid": False,
            "reason": "panel payload shape invalid",
            "candle_counts": _panel_candle_counts(data),
        }

    candle_counts = _panel_candle_counts(data)
    problems = []

    for symbol in ["EURUSD", "XAUUSD"]:
        plan = data.get(symbol) or {}
        try:
            price = float(plan.get("price"))
            has_price = math.isfinite(price) and price > 0
        except (TypeError, ValueError):
            has_price = False

        market_condition = str(
            plan.get("market_condition") or "UNKNOWN"
        ).upper()
        has_market_condition = market_condition not in [
            "",
            "UNKNOWN",
            "CTRADER_UNAVAILABLE",
        ]
        scores = [
            plan.get("buy_pct", plan.get("buy_percentage", 0)),
            plan.get("sell_pct", plan.get("sell_percentage", 0)),
            plan.get("confidence", 0),
        ]
        try:
            has_scores = any(float(value or 0) > 0 for value in scores)
        except (TypeError, ValueError):
            has_scores = False
        has_candles = any(candle_counts[symbol].values())

        if not has_price:
            problems.append(f"{symbol} price missing")
        if not has_market_condition:
            problems.append(f"{symbol} market condition unavailable")
        if not has_scores:
            problems.append(f"{symbol} scores are all zero")
        if not has_candles:
            problems.append(f"{symbol} candles missing")

    return {
        "valid": not problems,
        "reason": "; ".join(problems) if problems else None,
        "candle_counts": candle_counts,
    }

def _is_startup_panel_payload(data):
    if not _is_valid_panel_payload(data):
        return True

    return all(
        str((data.get(symbol) or {}).get("signal_text") or "").endswith(
            "(startup)"
        )
        for symbol in ["EURUSD", "XAUUSD"]
    )

def update_panel_cache(data, source):
    validity = _panel_cache_validity(data)
    if not validity["valid"]:
        raise ValueError(
            f"Brain returned unusable panel data: {validity['reason']}"
        )

    updated_at = time.time()
    PANEL_CACHE["data"] = data
    PANEL_CACHE["last_update"] = updated_at
    PANEL_REFRESH_STATE["last_success"] = updated_at
    PANEL_REFRESH_STATE["last_source"] = source
    PANEL_REFRESH_STATE["last_error"] = None
    print("PANEL_CACHE_UPDATE_SUCCESS =", {
        "source": source,
        "updated_at": updated_at,
        "eurusd_signal": (data.get("EURUSD") or {}).get("signal"),
        "xauusd_signal": (data.get("XAUUSD") or {}).get("signal"),
        "candle_counts": validity["candle_counts"],
    })
    return updated_at

def calculate_fresh_panel_data(reason, force_refresh=False):
    from brain import get_panel_data

    print("PANEL_REFRESH_START =", {
        "reason": reason,
        "force_refresh": bool(force_refresh),
    })
    return get_panel_data(force_refresh=force_refresh)

def refresh_panel_cache(reason="background", force_refresh=False):
    if not PANEL_REFRESH_LOCK.acquire(blocking=False):
        print("PANEL_REFRESH_SKIPPED =", {
            "reason": reason,
            "active_reason": PANEL_REFRESH_STATE.get("reason"),
        })
        return False

    started_at = time.time()
    PANEL_REFRESH_STATE.update({
        "running": True,
        "last_started": started_at,
        "last_error": None,
        "reason": reason,
    })

    try:
        data = calculate_fresh_panel_data(
            reason,
            force_refresh=force_refresh,
        )
        update_panel_cache(data, reason)
        print("PANEL_REFRESH_SUCCESS =", {
            "reason": reason,
            "seconds": round(time.time() - started_at, 2),
        })
        return True
    except Exception as exc:
        PANEL_REFRESH_STATE["last_error"] = str(exc)
        print("PANEL_REFRESH_ERROR =", {
            "reason": reason,
            "error": str(exc),
        })
        return False
    finally:
        PANEL_REFRESH_STATE["running"] = False
        PANEL_REFRESH_STATE["last_duration_seconds"] = round(
            time.time() - started_at,
            2,
        )
        PANEL_REFRESH_LOCK.release()

def refresh_panel_cache_direct(reason, force_refresh=True):
    started_at = time.time()
    PANEL_REFRESH_STATE.update({
        "last_started": started_at,
        "last_error": None,
        "reason": reason,
    })

    try:
        data = calculate_fresh_panel_data(
            reason,
            force_refresh=force_refresh,
        )
        refresh_live_panel_meta(data)
        validity = _panel_cache_validity(data)
        if validity["valid"]:
            update_panel_cache(data, reason)
        else:
            PANEL_REFRESH_STATE["last_error"] = (
                f"Fresh brain output has no usable market data: "
                f"{validity['reason']}"
            )
            print("PANEL_CACHE_RETURN_STALE =", {
                "reason": reason,
                "fresh_output_unusable": True,
                "validation_error": validity["reason"],
                "candle_counts": validity["candle_counts"],
            })
        PANEL_REFRESH_STATE["last_duration_seconds"] = round(
            time.time() - started_at,
            2,
        )
        print("PANEL_REFRESH_SUCCESS =", {
            "reason": reason,
            "seconds": PANEL_REFRESH_STATE["last_duration_seconds"],
            "direct": True,
            "cache_updated": validity["valid"],
        })
        return data
    except Exception as exc:
        PANEL_REFRESH_STATE["last_error"] = str(exc)
        PANEL_REFRESH_STATE["last_duration_seconds"] = round(
            time.time() - started_at,
            2,
        )
        print("PANEL_REFRESH_ERROR =", {
            "reason": reason,
            "error": str(exc),
            "direct": True,
        })
        return None


def warm_panel_cache_from_persisted_candles():
    try:
        from brain import hydrate_market_data_cache_from_disk

        if not hydrate_market_data_cache_from_disk():
            print("PANEL_STARTUP_WARM_SKIPPED = persisted candles incomplete")
            return False

        return refresh_panel_cache(
            reason="startup_disk_cache",
            force_refresh=True,
        )
    except Exception as exc:
        PANEL_REFRESH_STATE["last_error"] = str(exc)
        print("PANEL_STARTUP_WARM_ERROR =", str(exc))
        return False


def schedule_panel_refresh(reason="api_cache_stale"):
    if PANEL_REFRESH_STATE.get("running"):
        return False

    thread = threading.Thread(
        target=refresh_panel_cache,
        kwargs={"reason": reason},
        daemon=True,
    )
    thread.start()
    return True


def get_signal_setup_id(plan, side=None):
    if not isinstance(plan, dict):
        return None

    signal_side = str(side or plan.get("signal") or "").upper()
    if signal_side not in ["BUY", "SELL"]:
        return None

    setup_parts = {
        "side": signal_side,
        "strategy_setup_type": plan.get("strategy_setup_type"),
        "setup_candle_time": (
            plan.get("setup_candle_time")
            or plan.get("five_m_closed_candle_time")
            or plan.get("fifteen_m_breakout_candle_time")
        ),
        "entry": plan.get("entry_price") or plan.get("entry"),
        "sl": plan.get("stop_loss") or plan.get("sl"),
        "tp2": plan.get("tp2"),
    }
    encoded = json.dumps(setup_parts, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def get_latest_consumed_signal_setup(symbol):
    normalized_symbol = normalize_symbol(symbol)

    for trade in LIVE_TRADE_HISTORY:
        if not isinstance(trade, dict):
            continue
        if normalize_symbol(trade.get("symbol")) != normalized_symbol:
            continue
        if get_live_trade_status(trade) in ["RUNNING", "OPEN", "TP1 HIT"]:
            continue

        setup_id = trade.get("signal_setup_id")
        side = str(trade.get("side") or trade.get("action") or "").upper()
        if setup_id and side in ["BUY", "SELL"]:
            return {
                "signal_setup_id": setup_id,
                "side": side,
                "closed_at": trade.get("closed_at"),
            }

    return None


def apply_trade_signal_lifecycle(panel_data):
    if not isinstance(panel_data, dict):
        return panel_data

    def apply_active_trade_display(plan, active_trade, side, status):
        original_signal = str(plan.get("signal") or "WAIT").upper()
        setup_still_confirmed = original_signal in ["BUY", "SELL"]
        display_signal = original_signal if setup_still_confirmed else "WAIT"

        plan["market_signal"] = original_signal
        plan["signal"] = "WAIT"
        plan["final_signal"] = "WAIT"
        plan["signal_display_state"] = display_signal
        plan["display_signal"] = display_signal
        plan["history_signal"] = display_signal
        plan["setup_still_confirmed"] = setup_still_confirmed
        plan["active_trade_side"] = side
        plan["active_trade_status"] = status
        plan["fresh_entry_available"] = False
        plan["trade_already_running"] = True
        plan["blocked_by"] = "active_trade_running"
        plan["blocked_reason"] = (
            f"{side} trade is already running. Waiting for a new 15m swing break."
        )
        plan["blocker_rule_name"] = "active_trade_running"
        plan["signal_text"] = (
            f"{display_signal} {'🟢' if display_signal == 'BUY' else '🔴'} (setup confirmed, trade running)"
            if display_signal in ["BUY", "SELL"]
            else "WAIT ⚪ (trade running; setup no longer fresh)"
        )
        plan["plan_type"] = "WAIT - TRADE RUNNING"
        plan["plan_bias"] = display_signal
        plan["entry_timing"] = "WAIT FOR NEW 15M SWING BREAK"
        plan["strategy_setup_complete"] = False

        level_map = {
            "entry_price": ["entry", "entry_price", "current_price"],
            "stop_loss": ["sl", "stop_loss", "planned_sl"],
            "tp1": ["tp1", "planned_tp1"],
            "tp2": ["tp2", "planned_tp2"],
        }
        for plan_key, trade_keys in level_map.items():
            if plan.get(plan_key) not in [None, "", "--"]:
                continue
            for trade_key in trade_keys:
                value = active_trade.get(trade_key)
                if value not in [None, "", "--"]:
                    plan[plan_key] = value
                    break

        return display_signal

    for symbol in ["EURUSD", "XAUUSD"]:
        plan = panel_data.get(symbol)
        active_trade = LIVE_ACTIVE_ORDERS.get(symbol)

        if not isinstance(plan, dict):
            continue

        current_signal = str(plan.get("signal") or "WAIT").upper()
        current_setup_id = get_signal_setup_id(plan, current_signal)
        plan.setdefault("fresh_entry_available", current_signal in ["BUY", "SELL"])
        plan.setdefault("trade_already_running", False)
        plan.setdefault("signal_display_state", current_signal)

        if isinstance(active_trade, dict):
            trade_status = get_live_trade_status(active_trade)
            trade_side = str(
                active_trade.get("side")
                or active_trade.get("action")
                or ""
            ).upper()

            if trade_status in ["RUNNING", "OPEN", "TP1 HIT"]:
                if not active_trade.get("signal_setup_id") and current_setup_id:
                    active_trade["signal_setup_id"] = current_setup_id

                display_signal = apply_active_trade_display(
                    plan,
                    active_trade,
                    trade_side,
                    trade_status,
                )
                plan["active_trade_side"] = trade_side
                plan["active_trade_status"] = trade_status
                print("ACTIVE_TRADE_DISPLAY_DEBUG =", {
                    "symbol": symbol,
                    "side": trade_side,
                    "broker_status": trade_status,
                    "market_signal": current_signal,
                    "display_signal": display_signal,
                    "fresh_entry_available": plan.get("fresh_entry_available"),
                    "trade_already_running": plan.get("trade_already_running"),
                    "reason": "active trade display replaces fresh signal",
                })
                continue

        consumed = get_latest_consumed_signal_setup(symbol)
        if (
            current_signal not in ["BUY", "SELL"]
            or not current_setup_id
            or not consumed
            or consumed.get("side") != current_signal
            or consumed.get("signal_setup_id") != current_setup_id
        ):
            continue

        plan["consumed_market_signal"] = current_signal
        plan["signal"] = "WAIT"
        plan["final_signal"] = "WAIT"
        plan["signal_display_state"] = "WAIT"
        plan["fresh_entry_available"] = False
        plan["trade_already_running"] = False
        plan["signal_text"] = (
            f"WAIT ⚪ ({current_signal} trade closed; waiting for a fresh setup)"
        )
        plan["plan_bias"] = "WAIT"
        plan["plan_type"] = "WAIT FOR NEW SIGNAL"
        plan["entry_timing"] = "WAIT FOR FRESH SETUP"
        plan["strategy_setup_complete"] = False
        plan["blocked_by"] = "consumed_trade_setup"
        plan["blocked_reason"] = (
            f"The {current_signal} setup was already used. "
            "Waiting for a new 15m swing break."
        )
        plan["blocker_rule_name"] = "fresh_setup_required_after_trade_close"
        plan["trade_setup_consumed"] = True
        print("CLOSED_TRADE_SIGNAL_RESET_DEBUG =", {
            "symbol": symbol,
            "side": current_signal,
            "signal_setup_id": current_setup_id,
            "display_signal": "WAIT",
            "reason": "closed trade consumed the current setup",
        })

    return panel_data


def overlay_live_forming_candles(panel_data, live_price_status, now=None):
    if not isinstance(panel_data, dict):
        return panel_data

    live_price_status = (
        live_price_status
        if isinstance(live_price_status, dict)
        else {}
    )
    prices = live_price_status.get("live_prices") or {}
    now_timestamp = float(now if now is not None else time.time())
    timeframe_seconds = {
        "5m": 5 * 60,
        "15m": 15 * 60,
        "1h": 60 * 60,
    }

    for symbol in ["EURUSD", "XAUUSD"]:
        tick = prices.get(symbol) or {}
        tick_timestamp = tick.get("timestamp")

        try:
            tick_age = now_timestamp - float(tick_timestamp)
            tick_price = float(
                tick.get("mid")
                or tick.get("bid")
                or tick.get("ask")
            )
        except (TypeError, ValueError):
            continue

        if tick_price <= 0 or tick_age < 0 or tick_age > 10:
            continue

        symbol_candles = (
            (panel_data.get("candles") or {}).get(symbol)
        )
        if not isinstance(symbol_candles, dict):
            continue

        for timeframe, seconds in timeframe_seconds.items():
            candles = symbol_candles.get(timeframe)
            if not isinstance(candles, list) or not candles:
                continue

            bucket_time = int(now_timestamp // seconds) * seconds
            last = candles[-1]

            try:
                last_time = int(float(last.get("time")))
                previous_close = float(last.get("close"))
            except (TypeError, ValueError, AttributeError):
                continue

            if last_time < bucket_time:
                gap_seconds = bucket_time - last_time
                synthetic_open = (
                    tick_price
                    if gap_seconds > seconds * 2
                    else previous_close
                )
                candles.append({
                    "time": bucket_time,
                    "open": synthetic_open,
                    "high": max(synthetic_open, tick_price),
                    "low": min(synthetic_open, tick_price),
                    "close": tick_price,
                })
            elif last_time == bucket_time:
                try:
                    last["high"] = max(
                        float(last.get("high")),
                        tick_price,
                    )
                    last["low"] = min(
                        float(last.get("low")),
                        tick_price,
                    )
                except (TypeError, ValueError):
                    last["high"] = max(previous_close, tick_price)
                    last["low"] = min(previous_close, tick_price)
                last["close"] = tick_price

            if timeframe == "5m":
                plan = panel_data.get(symbol)
                if isinstance(plan, dict):
                    source = plan.setdefault("signal_data_source", {})
                    source.setdefault(
                        "latest_closed_5m_time",
                        source.get("latest_5m_time"),
                    )
                    source["latest_5m_time"] = datetime.fromtimestamp(
                        bucket_time,
                        tz=timezone.utc,
                    ).isoformat()
                    source["candle_source"] = "ctrader_live_tick"
                    source["display_tick_age_seconds"] = round(tick_age, 2)

        print("LIVE_FORMING_CANDLE_DEBUG =", {
            "symbol": symbol,
            "bucket_time": datetime.fromtimestamp(
                int(now_timestamp // 300) * 300,
                tz=timezone.utc,
            ).isoformat(),
            "tick_price": tick_price,
            "tick_age_seconds": round(tick_age, 2),
        })

    return panel_data


def refresh_live_panel_meta(panel_data):
    try:
        live_positions = sync_live_positions()
        apply_trade_signal_lifecycle(panel_data)
        apply_broker_closed_to_panel_signal_history(panel_data)
        run_ctrader_auto_trade_checks(panel_data)
        update_live_trade_exit_states(panel_data)
        live_price_status = get_live_prices()
        live_pl_sync = calculate_live_pl_sync()
        live_recent_history = get_live_recent_history_for_panel()
        live_trade_stats = calculate_live_trade_stats()

        LIVE_PANEL_META_CACHE.update({
            "live_positions": live_positions or [],
            "live_price_status": live_price_status or {},
            "live_pl_sync": live_pl_sync or {},
            "live_recent_history": live_recent_history or [],
            "live_trade_stats": live_trade_stats or {},
            "last_execution_time": get_last_execution_time(),
            "last_update": time.time(),
            "last_error": None,
        })
        print("LIVE_PANEL_META_REFRESH_SUCCESS =", {
            "positions": len(live_positions or []),
            "history": len(live_recent_history or []),
        })
        return True
    except Exception as exc:
        LIVE_PANEL_META_CACHE["last_error"] = str(exc)
        print("LIVE_PANEL_META_REFRESH_ERROR =", str(exc))
        return False


def background_fetch():
    while True:
        try:
            print("🔄 BACKGROUND FETCH (once for all users)")
            if refresh_panel_cache(reason="background"):
                refresh_live_panel_meta(PANEL_CACHE["data"])
                print("✅ Cache updated globally")

        except Exception as e:
            print("❌ Background fetch error:", e)

        time.sleep(CACHE_SECONDS)

@app.get("/")
def root():
    return {"message": "FlowSignal backend is running"}

@app.get("/news-impact")
def news_impact(symbol: str = "EURUSD"):
    return get_news_impact(symbol)

@app.get("/health-check")
def health_check():
    account_state = sync_ctrader_account_state(force=True)
    candle_status = {}

    try:
        from ctrader_connector import get_ctrader_candle_cache_status
        candle_status = get_ctrader_candle_cache_status()
    except Exception as exc:
        candle_status = {
            "ctrader_last_error": str(exc),
        }

    return {
        "ok": True,
        "backend_online": True,
        "ctrader_auth_status": "connected" if account_state.get("connected") else "disconnected",
        "active_account": account_state.get("account_id"),
        "data_source_status": os.getenv("MARKET_DATA_SOURCE", "ctrader"),
        "last_candle_time": candle_status.get("ctrader_last_success"),
        "last_candle_error": candle_status.get("ctrader_last_error"),
        "auto_trade_status": {
            "paper": AUTO_TRADE_ENABLED.get("enabled"),
            "live": LIVE_AUTO_TRADE_ENABLED.get("enabled"),
        },
        "feature_flags": load_feature_flags(),
    }

@app.get("/ctrader-diagnostics")
def ctrader_diagnostics():
    return get_ctrader_diagnostics()

def format_status_time(timestamp):
    if not timestamp:
        return None

    try:
        return time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(float(timestamp))
        )
    except (TypeError, ValueError):
        return None

LIVE_POSITION_SYNC_STATUS = {
    "last_success": None,
    "last_error": None,
}

@app.get("/ctrader/status")
@app.get("/ctrader-status")
def ctrader_status():
    account_state = sync_ctrader_account_state(force=False)
    refresh_token_status = get_ctrader_refresh_token_status()
    connected = bool(account_state.get("connected"))
    positions = []
    last_error = LIVE_POSITION_SYNC_STATUS.get("last_error")

    if connected:
        try:
            positions = get_open_positions()
            position_error = get_ctrader_position_fetch_error()

            if position_error:
                last_error = position_error
                LIVE_POSITION_SYNC_STATUS["last_error"] = position_error
            else:
                LIVE_POSITION_SYNC_STATUS["last_success"] = time.time()
                LIVE_POSITION_SYNC_STATUS["last_error"] = None
                last_error = None
        except Exception as e:
            last_error = str(e)
            LIVE_POSITION_SYNC_STATUS["last_error"] = last_error
            positions = []
    else:
        last_error = LIVE_POSITION_SYNC_STATUS.get("last_error")

    if connected:
        reason = account_state.get("reason") or "authenticated"
    else:
        reason = account_state.get("reason") or "broker disconnected"

    return {
        "connected": connected,
        "auth_ok": bool(account_state.get("auth_ok")),
        "account_found": bool(account_state.get("account_found")),
        "execution_ready": bool(account_state.get("execution_ready")),
        "degraded": bool(account_state.get("degraded")),
        "consecutive_failures": account_state.get("consecutive_failures", 0),
        "has_refresh_token": bool(refresh_token_status.get("has_refresh_token")),
        "refresh_token_loaded": bool(refresh_token_status.get("has_refresh_token")),
        "reason": reason,
        "account_id": account_state.get("account_id"),
        "live_positions_count": len(positions or []),
        "last_success": format_status_time(
            LIVE_POSITION_SYNC_STATUS.get("last_success")
        ),
        "last_error": last_error,
    }

@app.get("/market-data-source")
def market_data_source():
    from brain import get_market_data_source_status

    return get_market_data_source_status()

@app.get("/live-prices")
def live_prices():
    start_ctrader_live_price_stream()
    return get_live_prices()

@app.get("/auto-trade-status")
def auto_trade_status():
    return {
        **AUTO_TRADE_LAST_STATUS,
        "live_auto_status_by_symbol": LIVE_AUTO_STATUS_BY_SYMBOL,
    }

@app.post("/market-data-source")
def set_market_data_source_endpoint(payload: dict):
    from brain import get_panel_data, set_market_data_source

    result = set_market_data_source(payload.get("source"))

    if not result.get("ok"):
        return result

    print("MARKET DATA SOURCE SWITCHED TO:", result.get("source"))

    try:
        data = get_panel_data(force_refresh=True)

        if isinstance(data, dict) and "EURUSD" in data and "XAUUSD" in data:
            PANEL_CACHE["data"] = data
            PANEL_CACHE["last_update"] = time.time()
            result["panel_refreshed"] = True
        else:
            result["panel_refreshed"] = False
            result["refresh_error"] = "Forced refresh returned invalid panel data"

    except Exception as e:
        result["panel_refreshed"] = False
        result["refresh_error"] = str(e)
        print("MARKET DATA SOURCE FORCE REFRESH ERROR:", e)

    return result

@app.get("/panel-data")
def panel_data(force: int = 0):
    age = time.time() - PANEL_CACHE["last_update"]

    cached_data = PANEL_CACHE.get("data")

    if not isinstance(cached_data, dict):
        cached_data = default_panel()

    force_requested = str(force).lower() in ["1", "true", "yes"]
    cache_stale = age >= CACHE_SECONDS
    startup_cache = _is_startup_panel_payload(cached_data)
    cache_validity = _panel_cache_validity(cached_data)
    cache_valid = cache_validity["valid"]
    running_since = PANEL_REFRESH_STATE.get("last_started")
    running_seconds = (
        time.time() - float(running_since)
        if PANEL_REFRESH_STATE.get("running") and running_since
        else 0
    )
    refresh_stuck = running_seconds >= PANEL_REFRESH_STUCK_SECONDS
    direct_reason = None

    if force_requested:
        direct_reason = "panel_force"
    elif not cache_valid:
        direct_reason = "api_cache_invalid"
    elif cache_stale:
        direct_reason = (
            "api_cache_stale_startup"
            if startup_cache
            else "api_cache_stale_direct"
        )

    if direct_reason:
        fresh_data = refresh_panel_cache_direct(
            direct_reason,
            force_refresh=True,
        )
        if fresh_data is not None:
            cached_data = fresh_data
            age = 0
            fresh_validity = _panel_cache_validity(fresh_data)
            cache_valid = fresh_validity["valid"]
            cache_validity = fresh_validity
        else:
            print("PANEL_CACHE_RETURN_STALE =", {
                "reason": direct_reason,
                "cache_age_seconds": round(age, 1),
                "startup_cache": startup_cache,
                "refresh_running": PANEL_REFRESH_STATE.get("running"),
                "refresh_running_seconds": round(running_seconds, 1),
                "refresh_stuck": refresh_stuck,
                "error": PANEL_REFRESH_STATE.get("last_error"),
            })

    data = copy.deepcopy(cached_data)
    data = apply_trade_signal_lifecycle(data)
    current_live_price_status = get_live_prices()
    data = overlay_live_forming_candles(
        data,
        current_live_price_status,
    )

    live_positions = LIVE_PANEL_META_CACHE.get("live_positions") or []
    live_price_status = current_live_price_status or (
        LIVE_PANEL_META_CACHE.get("live_price_status") or {}
    )
    paper_trades = data.get("paper_trades") if isinstance(data, dict) else {}
    paper_history = data.get("paper_trade_history") if isinstance(data, dict) else []
    paper_stats = data.get("paper_trade_stats") if isinstance(data, dict) else {}
    live_pl_sync = dict(LIVE_PANEL_META_CACHE.get("live_pl_sync") or {})
    expected_weekly_reset = get_live_week_start_ts()
    expected_daily_reset = get_live_daily_reset_ts()
    expected_month_start = get_live_month_start_ts()

    if live_pl_sync.get("weekly_reset_ts") != expected_weekly_reset:
        if live_pl_sync.get("weekly_realized_pl"):
            print("stale weekly P/L cache ignored")
        live_pl_sync["weekly_realized_pl"] = 0
        live_pl_sync["weekly_total_pl"] = live_pl_sync.get("floating_live_pl", 0)
        live_pl_sync["weekly_reset_ts"] = expected_weekly_reset

    if live_pl_sync.get("daily_reset_ts") != expected_daily_reset:
        if live_pl_sync.get("daily_realized_pl"):
            print("stale daily P/L cache ignored")
        live_pl_sync["daily_realized_pl"] = 0
        live_pl_sync["daily_reset_ts"] = expected_daily_reset

    if live_pl_sync.get("monthly_start_ts") != expected_month_start:
        live_pl_sync["monthly_realized_pl"] = 0
        live_pl_sync["monthly_start_ts"] = expected_month_start

    live_recent_history = LIVE_PANEL_META_CACHE.get("live_recent_history") or []

    if not live_pl_sync.get("pl_calculation_version"):
        live_pl_sync = calculate_live_pl_sync()
        live_recent_history = get_live_recent_history_for_panel()
        LIVE_PANEL_META_CACHE.update({
            "live_pl_sync": live_pl_sync or {},
            "live_recent_history": live_recent_history or [],
            "live_trade_stats": calculate_live_trade_stats() or {},
            "last_update": time.time(),
        })

    print("LIVE_PL_SYNC:", live_pl_sync)

    print("PAPER_STATE_REFRESH:", {
        "paper_auto_enabled": AUTO_TRADE_ENABLED["enabled"],
        "active_symbols": [
            symbol
            for symbol, trade in (paper_trades or {}).items()
            if trade
        ],
        "history_count": len(paper_history or []),
        "stats": paper_stats or {}
    })
    print("AUTO_PANEL_STATE_REFRESH:", {
        "paper_auto_enabled": AUTO_TRADE_ENABLED["enabled"],
        "live_auto_enabled": LIVE_AUTO_TRADE_ENABLED["enabled"],
        "live_broker_connected": LIVE_ACCOUNT_STATE.get("connected"),
        "live_positions_count": len(live_positions or []),
        "live_active_symbols": [
            symbol
            for symbol, trade in LIVE_ACTIVE_ORDERS.items()
            if trade
        ],
    })

    data["weekly_realized_pl"] = live_pl_sync.get("weekly_realized_pl", 0)
    data["daily_realized_pl"] = live_pl_sync.get("daily_realized_pl", 0)
    data["daily_total_pl"] = live_pl_sync.get("daily_total_pl", 0)
    data["monthly_realized_pl"] = live_pl_sync.get("monthly_realized_pl", 0)
    data["floating_live_pl"] = live_pl_sync.get("floating_live_pl", 0)
    data["weekly_total_pl"] = live_pl_sync.get("weekly_total_pl", 0)

    data["_meta"] = {
        "source": (
            "direct_fresh"
            if direct_reason and age == 0 and cache_valid
            else "direct_fresh_no_data"
            if direct_reason and age == 0 and not cache_valid
            else "shared_cache_refreshing"
            if (
                age >= CACHE_SECONDS
                and PANEL_REFRESH_STATE.get("running")
                and not refresh_stuck
            )
            else "shared_cache"
            if cache_valid
            else "no_valid_cache"
        ),
        "cache_age_seconds": round(age, 1),
        "refresh_seconds": CACHE_SECONDS,
        "error": PANEL_REFRESH_STATE.get("last_error"),
        "cache_valid": cache_valid,
        "cache_validation_error": cache_validity.get("reason"),
        "candle_counts": cache_validity.get("candle_counts"),
        "brain_refresh": dict(PANEL_REFRESH_STATE),
        "live_meta_last_update": LIVE_PANEL_META_CACHE.get("last_update"),
        "live_meta_error": LIVE_PANEL_META_CACHE.get("last_error"),

        "paper_auto_enabled":
            AUTO_TRADE_ENABLED["enabled"],

        "live_auto_enabled":
            LIVE_AUTO_TRADE_ENABLED["enabled"],

        "live_account":
            LIVE_ACCOUNT_STATE,

        "live_active_orders":
            LIVE_ACTIVE_ORDERS,

        "broker_open_positions_count":
            len(live_positions or []),

        "live_trade_history":
            live_recent_history,

        "live_trade_stats":
            {
                **(LIVE_PANEL_META_CACHE.get("live_trade_stats") or {}),
                "pl_calculation_version":
                    live_pl_sync.get("pl_calculation_version"),
                "daily_realized_pl":
                    live_pl_sync.get("daily_realized_pl", 0),
                "daily_total_pl":
                    live_pl_sync.get("daily_total_pl", 0),
                "weekly_realized_pl":
                    live_pl_sync.get("weekly_realized_pl", 0),
                "monthly_realized_pl":
                    live_pl_sync.get("monthly_realized_pl", 0),
                "floating_live_pl":
                    live_pl_sync.get("floating_live_pl", 0),
                "weekly_total_pl":
                    live_pl_sync.get("weekly_total_pl", 0),
            },

        "weekly_realized_pl":
            live_pl_sync.get("weekly_realized_pl", 0),

        "daily_realized_pl":
            live_pl_sync.get("daily_realized_pl", 0),

        "daily_total_pl":
            live_pl_sync.get("daily_total_pl", 0),

        "monthly_realized_pl":
            live_pl_sync.get("monthly_realized_pl", 0),

        "floating_live_pl":
            live_pl_sync.get("floating_live_pl", 0),

        "weekly_total_pl":
            live_pl_sync.get("weekly_total_pl", 0),

        "pl_calculation_version":
            live_pl_sync.get("pl_calculation_version"),

        "auto_trade_status":
            AUTO_TRADE_LAST_STATUS,

        "live_auto_status_by_symbol":
            LIVE_AUTO_STATUS_BY_SYMBOL,

        "live_prices":
            live_price_status.get("live_prices", {}),

        "live_price_health":
            live_price_status.get("live_price_health"),

        "live_price_last_update":
            live_price_status.get("live_price_last_update"),

        "last_execution_time":
            LIVE_PANEL_META_CACHE.get("last_execution_time", 0)
    }

    return data


@app.get("/brain-status")
def brain_status():
    return {
        "ok": PANEL_REFRESH_STATE.get("last_error") is None,
        "cache_ready": bool(PANEL_CACHE.get("last_update")),
        "cache_age_seconds": (
            round(time.time() - PANEL_CACHE["last_update"], 1)
            if PANEL_CACHE.get("last_update")
            else None
        ),
        **PANEL_REFRESH_STATE,
    }
@app.post("/feedback")
def send_feedback(request: FeedbackRequest):
    try:
        msg_body = f"""
FlowSignal Feedback

User: {request.user or "anonymous"}
Time: {request.time or "unknown"}

Message:
{request.message}
""".strip()

        msg = MIMEText(msg_body)
        msg["Subject"] = "FlowSignal Feedback"
        msg["From"] = FEEDBACK_EMAIL
        msg["To"] = FEEDBACK_EMAIL

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(FEEDBACK_EMAIL, FEEDBACK_APP_PASSWORD)
            server.send_message(msg)

        print("✅ Feedback email sent")
        return {"status": "sent"}

    except Exception as e:
        print("❌ Feedback error:", e)
        return {"status": "error", "message": str(e)}
    
@app.post("/signup")
def signup(request: SignupRequest):
    users = load_users()
    email = request.email.strip().lower()

    if not email or not request.password.strip():
        return {"ok": False, "message": "Email and password required"}

    if email in users:
        return {"ok": False, "message": "Account already exists"}

    role = "user"

    if email == "flowsignal.contact@gmail.com":
        role = "admin"

    users[email] = {
        "password": hash_password(request.password),
        "role": role
    }
    save_users(users)

    return {"ok": True, "message": "Account created"}

@app.post("/login")
def login(request: LoginRequest):
    users = load_users()
    email = request.email.strip().lower()
    hashed = hash_password(request.password)
    
    if email == "flowsignal.contact@gmail.com" and request.password == "@Renathaux509.":
        token = str(uuid.uuid4())
        SESSIONS[token] = {
            "email": email,
            "role": "admin"
        }

        return {
            "ok": True,
            "message": "Admin login success",
            "token": token,
            "email": email,
            "role": "admin"
        }

    if email not in users:
        return {"ok": False, "message": "Account not found"}

    if users[email]["password"] != hashed:
        return {"ok": False, "message": "Wrong password"}

    role = "admin" if email == "flowsignal.contact@gmail.com" else users[email].get("role", "user")

    token = str(uuid.uuid4())
    SESSIONS[token] = {
        "email": email,
        "role": role
    }

    return {
        "ok": True,
        "message": "Login success",
        "token": token,
        "email": email,
        "role": role
    }

@app.post("/track-visit")
def track_visit(data: dict, request: Request):
    try:
        visits = load_visits()

        visitor_id = data.get("visitor_id")
        
        ip = request.headers.get("x-forwarded-for", request.client.host)
        if ip and "," in ip:
            ip = ip.split(",")[0].strip()

        country = "Unknown"

        try:
            if ip not in ["127.0.0.1", "localhost"]:
                geo = requests.get(f"http://ip-api.com/json/{ip}", timeout=2).json()
                country = geo.get("country", "Unknown")
            else:
                country = "Local"
        except Exception:
            country = "Unknown"

        if not visitor_id:
            visitor_id = str(uuid.uuid4())

        visit_data = {
            "time": time.time(),
            "visitor_id": visitor_id,
            "country": country
        }

        visits.append(visit_data)

        thirty_days_ago = time.time() - (30 * 86400)

        visits = [
            v for v in visits
            if v.get("time", 0) >= thirty_days_ago
        ]

        save_visits(visits)

        unique_visitors = len(
            set(v.get("visitor_id") for v in visits if v.get("visitor_id"))
        )

        return {
            "ok": True,
            "visitor_id": visitor_id,
            "total_visits": len(visits),
            "unique_visitors": unique_visitors
        }

    except Exception as e:
        print("TRACK VISIT ERROR:", e)

        return {
            "ok": False,
            "message": str(e)
        }
@app.get("/admin-stats")
def admin_stats():
    visits = load_visits()

    total_visits = len(visits)

    unique_visitors = len(
        set(v.get("visitor_id") for v in visits if v.get("visitor_id"))
    )

    today_start = time.time() - 86400

    today_visits = len([
        v for v in visits
        if v.get("time", 0) >= today_start
    ])

    last_visit = None
    if visits:
        last_visit = max(v["time"] for v in visits)

    countries = list(
    set(
        v.get("country")
        for v in visits
        if v.get("country")
    )
    )

    return {
        "total_visits": total_visits,
        "unique_visitors": unique_visitors,
        "today_visits": today_visits,
        "last_visit": last_visit,
        "countries": countries
    }

@app.post("/execute-trade")
def execute_trade(request: TradeRequest):
    try:
        if request.token != ADMIN_TOKEN:
            print("❌ UNAUTHORIZED TRADE ATTEMPT")
            return {
                "ok": False,
                "message": "Unauthorized"
            }

        print(f"TRADE REQUEST RECEIVED -> {request.symbol} {request.action}")
        return {
            "ok": True,
            "message": f"Trade request received for {request.symbol} {request.action}",
            "symbol": request.symbol,
            "action": request.action
        }
    except Exception as e:
        print("TRADE ERROR:", e)
        return {
            "ok": False,
            "message": str(e)
        }


AUTO_TRADE_ENABLED = {
    "enabled": False
}
AUTO_TRADE_STATE_FILE = os.path.join(
    os.path.dirname(__file__),
    "auto_trade_state.json"
)

LAST_EXECUTION_TIME = 0

def load_auto_trade_state():
    if not os.path.exists(AUTO_TRADE_STATE_FILE):
        return

    try:
        with open(AUTO_TRADE_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        AUTO_TRADE_ENABLED["enabled"] = bool(state.get("paper_auto_enabled", False))
        LIVE_AUTO_TRADE_ENABLED["enabled"] = bool(state.get("live_auto_enabled", False))
        print("AUTO TRADE STATE LOADED:", state)
    except Exception as e:
        print("AUTO TRADE STATE LOAD ERROR:", e)

def save_auto_trade_state():
    try:
        with open(AUTO_TRADE_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "paper_auto_enabled": AUTO_TRADE_ENABLED["enabled"],
                "live_auto_enabled": LIVE_AUTO_TRADE_ENABLED["enabled"],
            }, f, indent=2)
    except Exception as e:
        print("AUTO TRADE STATE SAVE ERROR:", e)

def get_last_execution_time():
    live_times = [
        timestamp
        for timestamp in LIVE_LAST_EXECUTION_TIME.values()
        if timestamp
    ] if "LIVE_LAST_EXECUTION_TIME" in globals() else []

    return max([LAST_EXECUTION_TIME, *live_times] or [0])

@app.post("/paper-auto-toggle")
def paper_auto_toggle(payload: dict):

    enabled = bool(
        payload.get("enabled", False)
    )

    AUTO_TRADE_ENABLED["enabled"] = enabled
    save_auto_trade_state()

    print(
        "AUTO TRADE STATE:",
        AUTO_TRADE_ENABLED["enabled"]
    )

    return {
        "status": "ok",
        "enabled": AUTO_TRADE_ENABLED["enabled"]
    }

LIVE_AUTO_TRADE_ENABLED = {
    "enabled": False
}

load_auto_trade_state()

LIVE_ACCOUNT_STATE = {
    "connected": False,
    "mode": "demo",   # demo/live
    "broker": "ctrader",
    "account_id": None,
    "execution_ready": False
}

LIVE_ACTIVE_ORDERS = {
    "EURUSD": None,
    "XAUUSD": None
}

LIVE_TRADE_HISTORY = []
LIVE_BROKER_CLOSED_HISTORY = []
LIVE_BROKER_HISTORY_CACHE = {
    "updated_at": 0,
    "history": [],
}
LIVE_RESET_KEY = "last_live_reset"
LIVE_MARKET_TIMEZONE = ZoneInfo("America/New_York")
load_live_monthly_history_cache()
LAST_LIVE_RESET = 0
LIVE_ORDER_LOCK = threading.RLock()
LIVE_ORDER_IN_FLIGHT = set()
MAX_LIVE_TRADE_HISTORY = 50
LIVE_EXECUTION_COOLDOWN_SECONDS = 30
BROKER_SYNC_GRACE_SECONDS = 10
LIVE_RISK_PERCENT = 1.0
MAX_LIVE_RISK_PERCENT = 1.05
RISK_TOLERANCE_PERCENT = 0.10
MIN_LOT = 0.01
LIVE_LAST_EXECUTION_TIME = {
    "EURUSD": 0,
    "XAUUSD": 0
}

def get_configured_live_risk_percent():
    try:
        configured = float(
            load_risk_settings().get("riskPerTradePct", LIVE_RISK_PERCENT)
        )
    except (TypeError, ValueError):
        configured = LIVE_RISK_PERCENT

    return max(0.05, min(1.0, configured))

def get_maximum_allowed_live_risk_percent(risk_percent=None):
    target = (
        get_configured_live_risk_percent()
        if risk_percent is None
        else float(risk_percent)
    )
    return round(target + 0.05, 4)
AUTO_TRADE_LAST_STATUS = {
    "symbol": None,
    "signal": None,
    "action": None,
    "status": "WAIT",
    "reason": "Waiting for BUY/SELL signal",
    "timestamp": None,
}
LIVE_AUTO_STATUS_BY_SYMBOL = {
    "EURUSD": {
        "symbol": "EURUSD",
        "signal": None,
        "action": None,
        "status": "WAIT",
        "reason": "Waiting for BUY/SELL signal",
        "checked_at": None,
        "active_trade": None,
    },
    "XAUUSD": {
        "symbol": "XAUUSD",
        "signal": None,
        "action": None,
        "status": "WAIT",
        "reason": "Waiting for BUY/SELL signal",
        "checked_at": None,
        "active_trade": None,
    },
}
LIVE_BACKUP_FILE = os.path.join(
    os.path.dirname(__file__),
    "live_backup.json"
)

def normalize_live_auto_status(status):
    state = str(status or "WAIT").upper()

    if state == "WAITING":
        return "WAIT"
    if state == "ORDER_SENT":
        return "EXECUTED"
    if state == "ORDER_REJECTED":
        return "BLOCKED"

    return state

def set_auto_trade_status(symbol=None, signal=None, action=None, status="WAITING", reason=None, details=None):
    timestamp = time.time()
    normalized_symbol = normalize_symbol(symbol) if symbol else None
    normalized_status = normalize_live_auto_status(status)

    AUTO_TRADE_LAST_STATUS.update({
        "symbol": normalized_symbol,
        "signal": signal,
        "action": action,
        "status": normalized_status,
        "reason": reason,
        "timestamp": timestamp,
        "details": details,
    })

    if normalized_symbol in LIVE_AUTO_STATUS_BY_SYMBOL:
        LIVE_AUTO_STATUS_BY_SYMBOL[normalized_symbol] = {
            "symbol": normalized_symbol,
            "signal": signal,
            "action": action,
            "status": normalized_status,
            "reason": reason,
            "details": details,
            "checked_at": timestamp,
            "active_trade": LIVE_ACTIVE_ORDERS.get(normalized_symbol),
        }
    elif not normalized_symbol:
        for status_symbol in LIVE_AUTO_STATUS_BY_SYMBOL:
            LIVE_AUTO_STATUS_BY_SYMBOL[status_symbol] = {
                **LIVE_AUTO_STATUS_BY_SYMBOL[status_symbol],
                "status": normalized_status,
                "reason": reason,
                "details": details,
                "checked_at": timestamp,
                "active_trade": LIVE_ACTIVE_ORDERS.get(status_symbol),
            }

    print("AUTO TRADE STATUS:", AUTO_TRADE_LAST_STATUS)
    print("LIVE AUTO STATUS BY SYMBOL:", LIVE_AUTO_STATUS_BY_SYMBOL)
    log_live_trade_audit(
        "auto_status",
        LIVE_ACTIVE_ORDERS.get(normalized_symbol) if normalized_symbol else {},
        symbol=normalized_symbol,
        reason=f"{normalized_status}: {reason}"
    )
    return AUTO_TRADE_LAST_STATUS

def get_persistable_live_active_orders():
    return {
        symbol: trade
        for symbol, trade in LIVE_ACTIVE_ORDERS.items()
        if trade and trade.get("source") == "broker"
    }

def get_live_trade_identity(trade):
    if not isinstance(trade, dict):
        return None

    return (
        trade.get("trade_id")
        or trade.get("broker_position_id")
        or trade.get("position_id")
        or trade.get("broker_order_id")
        or trade.get("order_id")
    )

def get_live_trade_match_key(trade):
    if not isinstance(trade, dict):
        return None

    broker_position_id = trade.get("broker_position_id") or trade.get("position_id")

    if broker_position_id:
        return f"position:{broker_position_id}"

    broker_order_id = trade.get("broker_order_id") or trade.get("order_id")

    if broker_order_id:
        return f"order:{broker_order_id}"

    trade_id = trade.get("trade_id")

    if trade_id:
        return f"trade:{trade_id}"

    return None

def ensure_live_trade_identity(trade, symbol=None):
    if not isinstance(trade, dict):
        return trade

    broker_position_id = (
        trade.get("broker_position_id")
        or trade.get("position_id")
    )
    trade_id = get_live_trade_identity(trade)

    if broker_position_id:
        trade_id = f"ctrader-pos-{broker_position_id}"
    elif not trade_id:
        trade_id = f"flowsignal-{normalize_symbol(symbol or trade.get('symbol'))}-{uuid.uuid4()}"

    trade["trade_id"] = str(trade_id)

    if broker_position_id:
        trade["broker_position_id"] = broker_position_id

    return trade

def enrich_broker_closed_trade_levels(trade):
    if not isinstance(trade, dict):
        return trade

    broker_trade = dict(trade)
    raw = broker_trade.get("raw") if isinstance(broker_trade.get("raw"), dict) else {}
    close_detail = (
        raw.get("closePositionDetail")
        if isinstance(raw.get("closePositionDetail"), dict)
        else {}
    )

    raw_tp1 = (
        broker_trade.get("tp1")
        or broker_trade.get("tp_price")
        or raw.get("tp1")
        or raw.get("takeProfit")
        or close_detail.get("tp1")
        or close_detail.get("takeProfit")
    )
    raw_tp2 = (
        broker_trade.get("tp2")
        or broker_trade.get("tp2_price")
        or raw.get("tp2")
        or close_detail.get("tp2")
    )

    if raw_tp1 is not None:
        broker_trade["tp1"] = raw_tp1

    if raw_tp2 is not None:
        broker_trade["tp2"] = raw_tp2

    broker_identifiers = get_trade_identifier_set(broker_trade)
    local_trades = [
        item
        for item in [
            *LIVE_ACTIVE_ORDERS.values(),
            *LIVE_TRADE_HISTORY,
        ]
        if isinstance(item, dict)
    ]
    local_trade = next(
        (
            item
            for item in local_trades
            if (
                broker_identifiers
                and broker_identifiers.intersection(
                    get_trade_identifier_set(item)
                )
            )
        ),
        None,
    )

    if not local_trade:
        return broker_trade

    merged = {
        **broker_trade,
        **local_trade,
    }
    preserved_fields = [
        "sl",
        "original_sl",
        "tp1",
        "tp2",
        "protected_sl_price",
        "hit_tp1",
        "profit_protected",
        "current_price",
        "current_high",
        "current_low",
    ]

    for field in preserved_fields:
        if local_trade.get(field) is not None:
            merged[field] = local_trade.get(field)
        elif broker_trade.get(field) is not None:
            merged[field] = broker_trade.get(field)

    broker_override_fields = [
        "status",
        "result",
        "pnl",
        "profit",
        "broker_pnl",
        "broker_realized_profit",
        "broker_realized_source",
        "broker_pnl_source",
        "closed_at",
        "close_price",
        "deal_id",
        "order_id",
        "broker_order_id",
        "position_id",
        "broker_position_id",
        "trade_id",
        "source",
        "history_source",
        "note",
        "raw",
    ]

    for field in broker_override_fields:
        if field in broker_trade:
            merged[field] = broker_trade.get(field)

    return merged

def get_live_broker_closed_history(force=False):
    run_weekly_live_reset()

    now = time.time()

    if (
        not force
        and LIVE_BROKER_HISTORY_CACHE.get("history")
        and now - LIVE_BROKER_HISTORY_CACHE.get("updated_at", 0) < 20
    ):
        return [
            enrich_broker_closed_trade_levels(trade)
            for trade in LIVE_BROKER_HISTORY_CACHE.get("history") or []
        ]

    try:
        broker_history = [
            enrich_broker_closed_trade_levels(trade)
            for trade in get_closed_deals_for_current_week(max_rows=100)
        ]
    except Exception as e:
        print("LIVE_BROKER_HISTORY_SYNC_ERROR:", e)
        broker_history = []

    LIVE_BROKER_CLOSED_HISTORY[:] = broker_history[:MAX_LIVE_TRADE_HISTORY]
    LIVE_BROKER_HISTORY_CACHE["history"] = list(LIVE_BROKER_CLOSED_HISTORY)
    LIVE_BROKER_HISTORY_CACHE["updated_at"] = now
    print("LIVE_BROKER_HISTORY_SYNC =", {
        "closed_trades": len(LIVE_BROKER_CLOSED_HISTORY),
        "realized_pl": round(sum(item.get("broker_realized_profit") or 0 for item in LIVE_BROKER_CLOSED_HISTORY), 2),
    })

    return list(LIVE_BROKER_CLOSED_HISTORY)

def get_live_broker_monthly_history(force=False):
    now = time.time()
    month_key = datetime.now(LIVE_MARKET_TIMEZONE).strftime("%Y-%m")

    if (
        not force
        and LIVE_MONTHLY_HISTORY_CACHE.get("month_key") == month_key
        and now - LIVE_MONTHLY_HISTORY_CACHE.get("updated_at", 0) < 60
    ):
        return list(LIVE_MONTHLY_HISTORY_CACHE.get("history") or [])

    previous_history = list(LIVE_MONTHLY_HISTORY_CACHE.get("history") or [])
    previous_month_key = LIVE_MONTHLY_HISTORY_CACHE.get("month_key")

    try:
        monthly_history = get_closed_deals_for_current_month(max_rows=500)
    except Exception as exc:
        print("LIVE_MONTHLY_HISTORY_SYNC_ERROR:", exc)
        return previous_history

    if (
        not monthly_history
        and previous_history
        and previous_month_key == month_key
    ):
        print("LIVE_MONTHLY_HISTORY_EMPTY_REFRESH_IGNORED =", {
            "month_key": month_key,
            "preserved_closed_trades": len(previous_history),
        })
        return previous_history

    LIVE_MONTHLY_HISTORY_CACHE["history"] = list(monthly_history or [])
    LIVE_MONTHLY_HISTORY_CACHE["updated_at"] = now
    LIVE_MONTHLY_HISTORY_CACHE["month_key"] = month_key
    save_live_monthly_history_cache()
    return list(LIVE_MONTHLY_HISTORY_CACHE["history"])

def get_live_recent_history_for_panel():
    run_weekly_live_reset()

    broker_history = get_live_broker_closed_history()

    if broker_history:
        return broker_history[:MAX_LIVE_TRADE_HISTORY]

    active_ids = {
        str(get_live_trade_match_key(trade))
        for trade in LIVE_ACTIVE_ORDERS.values()
        if trade and get_live_trade_match_key(trade)
    }
    cleaned = []

    for trade in LIVE_TRADE_HISTORY:
        if str(get_live_trade_match_key(trade)) in active_ids:
            continue

        if not is_usable_local_live_history_trade(trade):
            continue

        if not trade_is_current_week(trade):
            continue

        cleaned.append(trade)

    return cleaned[:MAX_LIVE_TRADE_HISTORY]

def is_usable_local_live_history_trade(trade):
    if not isinstance(trade, dict):
        return False

    status = get_live_trade_status(trade)

    if status in ["RUNNING", "OPEN", "WAIT", "BLOCKED", "REJECTED", "NO_SIGNAL"]:
        return False

    broker_pl, broker_pl_source = extract_broker_realized_pl(trade)
    stored_pl = trade.get("pnl") if trade.get("pnl") is not None else trade.get("profit")

    try:
        stored_value = float(stored_pl)
    except (TypeError, ValueError):
        stored_value = 0

    if status in ["BROKER_CLOSED", "DISCONNECTED"] and broker_pl_source is None and abs(stored_value) > 500:
        print("LIVE_HISTORY_CORRUPT_ROW_IGNORED =", {
            "symbol": trade.get("symbol"),
            "position_id": trade.get("position_id") or trade.get("broker_position_id"),
            "status": status,
            "stored_profit": stored_value,
        })
        return False

    return True

def log_live_trade_audit(
    event,
    trade=None,
    symbol=None,
    reason=None,
    weekly_realized_pl=None,
    floating_live_pl=None,
    weekly_total_pl=None,
):
    trade = trade if isinstance(trade, dict) else {}
    trade_id = get_live_trade_identity(trade)

    print("LIVE_TRADE_AUDIT_DEBUG =", {
        "event": event,
        "symbol": normalize_symbol(symbol or trade.get("symbol")) if (symbol or trade.get("symbol")) else None,
        "trade_id": str(trade_id) if trade_id else None,
        "broker_position_id": trade.get("broker_position_id") or trade.get("position_id"),
        "side": trade.get("side") or trade.get("action"),
        "entry": trade.get("entry"),
        "current_price": trade.get("current_price") or trade.get("currentPrice"),
        "sl": trade.get("sl"),
        "tp1": trade.get("tp1"),
        "tp2": trade.get("tp2"),
        "status": trade.get("status"),
        "result": trade.get("result"),
        "pips": trade.get("pips"),
        "profit": (
            trade.get("broker_realized_profit")
            if trade.get("broker_realized_profit") is not None
            else trade.get("broker_pnl")
            if trade.get("broker_pnl") is not None
            else trade.get("pnl")
            if trade.get("pnl") is not None
            else trade.get("profit")
        ),
        "weekly_realized_pl": weekly_realized_pl,
        "floating_live_pl": floating_live_pl,
        "weekly_total_pl": weekly_total_pl,
        "source": trade.get("source"),
        "reason": reason or trade.get("note") or trade.get("exit_reason"),
    })

def save_live_backup():
    try:
        with open(LIVE_BACKUP_FILE, "w") as f:
            json.dump({
                "live_active_orders":
                    get_persistable_live_active_orders(),
                "live_trade_history":
                    LIVE_TRADE_HISTORY[:MAX_LIVE_TRADE_HISTORY],
                "live_last_execution_time":
                    LIVE_LAST_EXECUTION_TIME,
                LIVE_RESET_KEY:
                    LAST_LIVE_RESET,
            }, f, indent=2)
    except Exception as e:
        print("LIVE BACKUP SAVE ERROR:", e)

def get_live_week_start_ts(now=None):
    if now is None:
        now = datetime.now(LIVE_MARKET_TIMEZONE)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=LIVE_MARKET_TIMEZONE)
    else:
        now = now.astimezone(LIVE_MARKET_TIMEZONE)

    reset = now.replace(hour=17, minute=0, second=0, microsecond=0)
    days_since_sunday = (now.weekday() - 6) % 7
    reset = reset - timedelta(days=days_since_sunday)

    if now.weekday() == 6 and now < reset:
        reset = reset - timedelta(days=7)

    return reset.timestamp()

def run_weekly_live_reset(force=False):
    global LAST_LIVE_RESET

    reset_ts = get_live_week_start_ts()

    if not force and reset_ts <= LAST_LIVE_RESET:
        return False

    active_ids = {
        str(get_live_trade_match_key(trade))
        for trade in LIVE_ACTIVE_ORDERS.values()
        if trade and get_live_trade_match_key(trade)
    }
    before_count = len(LIVE_TRADE_HISTORY)
    kept_history = []

    for trade in LIVE_TRADE_HISTORY:
        status = str(
            trade.get("status")
            or trade.get("result")
            or ""
        ).upper()
        match_key = str(get_live_trade_match_key(trade))

        if match_key in active_ids or status in ["RUNNING", "OPEN", "TP1 HIT"]:
            kept_history.append(trade)
            continue

        if trade_is_current_week(trade, reset_ts=reset_ts):
            kept_history.append(trade)

    LIVE_TRADE_HISTORY[:] = kept_history[:MAX_LIVE_TRADE_HISTORY]

    for symbol, timestamp in list(LIVE_LAST_EXECUTION_TIME.items()):
        try:
            if float(timestamp or 0) < reset_ts:
                LIVE_LAST_EXECUTION_TIME[symbol] = 0
        except (TypeError, ValueError):
            LIVE_LAST_EXECUTION_TIME[symbol] = 0

    LIVE_BROKER_CLOSED_HISTORY.clear()
    LIVE_BROKER_HISTORY_CACHE["history"] = []
    LIVE_BROKER_HISTORY_CACHE["updated_at"] = 0

    for symbol in LIVE_AUTO_STATUS_BY_SYMBOL:
        if LIVE_ACTIVE_ORDERS.get(symbol):
            continue

        LIVE_AUTO_STATUS_BY_SYMBOL[symbol] = {
            **LIVE_AUTO_STATUS_BY_SYMBOL[symbol],
            "signal": None,
            "action": None,
            "status": "WAIT",
            "reason": "Waiting for BUY/SELL signal",
            "checked_at": time.time(),
            "active_trade": None,
        }

    if not any(LIVE_ACTIVE_ORDERS.values()):
        AUTO_TRADE_LAST_STATUS.update({
            "symbol": None,
            "signal": None,
            "action": None,
            "status": "WAIT",
            "reason": "Waiting for BUY/SELL signal",
            "timestamp": time.time(),
        })

    LAST_LIVE_RESET = reset_ts

    removed_count = before_count - len(LIVE_TRADE_HISTORY)

    print("LIVE WEEKLY RESET:", {
        "reset_time": datetime.fromtimestamp(reset_ts).isoformat(),
        "removed_closed_trades": removed_count,
        "kept_trades": len(LIVE_TRADE_HISTORY),
    })

    save_live_backup()
    return True

def move_live_trade_to_history_once(trade):
    if not isinstance(trade, dict):
        return

    ensure_live_trade_identity(trade)

    identifiers = [
        trade.get("trade_id"),
        trade.get("order_id"),
        trade.get("broker_order_id"),
        trade.get("position_id"),
        trade.get("broker_position_id"),
    ]
    identifiers = [str(item) for item in identifiers if item]

    if identifiers:
        LIVE_TRADE_HISTORY[:] = [
            item for item in LIVE_TRADE_HISTORY
            if not any(
                str(item.get(key)) in identifiers
                for key in [
                    "order_id",
                    "broker_order_id",
                    "position_id",
                    "broker_position_id",
                    "trade_id",
                ]
                if item.get(key)
            )
        ]

    LIVE_TRADE_HISTORY.insert(0, trade)
    del LIVE_TRADE_HISTORY[MAX_LIVE_TRADE_HISTORY:]
    log_live_trade_audit("history_upsert", trade)

def get_trade_timestamp(trade):
    if not isinstance(trade, dict):
        return None

    for key in ["closed_at", "opened_at", "time", "timestamp"]:
        value = trade.get(key)

        if value:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue

    return None

def trade_is_today(trade):
    timestamp = get_trade_timestamp(trade)

    if not timestamp:
        return False

    if timestamp > 10000000000:
        timestamp = timestamp / 1000

    trade_time = time.localtime(timestamp)
    current_time = time.localtime()

    return (
        trade_time.tm_year == current_time.tm_year
        and trade_time.tm_yday == current_time.tm_yday
    )

def trade_is_current_week(trade, reset_ts=None):
    timestamp = get_trade_timestamp(trade)

    if not timestamp:
        return False

    if timestamp > 10000000000:
        timestamp = timestamp / 1000

    if reset_ts is None:
        reset_ts = get_live_week_start_ts()

    return timestamp >= reset_ts

def get_live_daily_reset_ts(now=None):
    if now is None:
        now = datetime.now(LIVE_MARKET_TIMEZONE)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=LIVE_MARKET_TIMEZONE)
    else:
        now = now.astimezone(LIVE_MARKET_TIMEZONE)

    reset = now.replace(hour=17, minute=0, second=0, microsecond=0)
    if now < reset:
        reset -= timedelta(days=1)
    return reset.timestamp()

def get_live_month_start_ts(now=None):
    if now is None:
        now = datetime.now(LIVE_MARKET_TIMEZONE)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=LIVE_MARKET_TIMEZONE)
    else:
        now = now.astimezone(LIVE_MARKET_TIMEZONE)

    return now.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ).timestamp()

def get_closed_trade_dedupe_key(trade):
    if not isinstance(trade, dict):
        return None

    deal_id = trade.get("deal_id")
    if deal_id not in [None, ""]:
        return f"deal:{deal_id}"

    trade_id = trade.get("trade_id")
    if trade_id not in [None, ""]:
        return f"trade:{trade_id}"

    timestamp = get_trade_timestamp(trade)
    broker_pl, _source = extract_broker_realized_pl(trade)
    return "|".join([
        str(normalize_symbol(trade.get("symbol")) or ""),
        str(trade.get("order_id") or trade.get("broker_order_id") or ""),
        str(trade.get("position_id") or trade.get("broker_position_id") or ""),
        str(timestamp or ""),
        str(broker_pl if broker_pl is not None else ""),
    ])

def calculate_closed_pl_windows(raw_closed_trades, now=None):
    current = now or datetime.now(LIVE_MARKET_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=LIVE_MARKET_TIMEZONE)
    else:
        current = current.astimezone(LIVE_MARKET_TIMEZONE)

    weekly_reset_ts = get_live_week_start_ts(current)
    daily_reset_ts = get_live_daily_reset_ts(current)
    monthly_start_ts = get_live_month_start_ts(current)
    unique_trades = []
    seen = set()
    ignored_duplicate_count = 0

    for trade in raw_closed_trades or []:
        if not isinstance(trade, dict):
            continue

        source = str(trade.get("source") or "").lower()
        history_source = str(trade.get("history_source") or "").lower()
        if source not in ["broker", "ctrader"] and "ctrader" not in history_source:
            continue

        key = get_closed_trade_dedupe_key(trade)
        if not key or key in seen:
            ignored_duplicate_count += 1
            continue

        timestamp = get_trade_timestamp(trade)
        if timestamp is None:
            continue
        if timestamp > 10_000_000_000:
            timestamp /= 1000

        broker_pl, broker_pl_source = extract_broker_realized_pl(trade)
        if broker_pl is None:
            continue

        seen.add(key)
        unique_trades.append({
            "trade": trade,
            "timestamp": timestamp,
            "profit": broker_pl,
            "profit_source": broker_pl_source,
        })

    day_trades = [
        item for item in unique_trades
        if item["timestamp"] >= daily_reset_ts
    ]
    week_trades = [
        item for item in unique_trades
        if item["timestamp"] >= weekly_reset_ts
    ]
    month_trades = [
        item for item in unique_trades
        if item["timestamp"] >= monthly_start_ts
    ]
    ignored_old_trades_count = sum(
        item["timestamp"] < weekly_reset_ts
        for item in unique_trades
    )
    weekly_losing_trades = [
        item for item in week_trades
        if item["profit"] < 0
    ]
    daily_losing_trades = [
        item for item in day_trades
        if item["profit"] < 0
    ]

    invalid_week_trades = [
        item for item in week_trades
        if item["timestamp"] < weekly_reset_ts
    ]
    if invalid_week_trades:
        print("WEEKLY_PL_VALIDATION_ERROR =", {
            "weekly_reset_time": datetime.fromtimestamp(
                weekly_reset_ts,
                LIVE_MARKET_TIMEZONE,
            ).isoformat(),
            "invalid_trade_count": len(invalid_week_trades),
        })
        week_trades = [
            item for item in unique_trades
            if item["timestamp"] >= weekly_reset_ts
        ]

    weekly_realized_pl = round(
        sum(item["profit"] for item in week_trades),
        2,
    )
    daily_realized_pl = round(
        sum(item["profit"] for item in day_trades),
        2,
    )

    if weekly_realized_pl < 0 and not weekly_losing_trades:
        print("stale weekly P/L cache ignored")
        weekly_realized_pl = 0

    if daily_realized_pl < 0 and not daily_losing_trades:
        print("stale daily P/L cache ignored")
        daily_realized_pl = 0

    debug = {
        "now_ny": current.isoformat(),
        "current_week_start_ny": datetime.fromtimestamp(
            weekly_reset_ts,
            LIVE_MARKET_TIMEZONE,
        ).isoformat(),
        "daily_start_ny": datetime.fromtimestamp(
            daily_reset_ts,
            LIVE_MARKET_TIMEZONE,
        ).isoformat(),
        "weekly_reset_time": datetime.fromtimestamp(
            weekly_reset_ts,
            LIVE_MARKET_TIMEZONE,
        ).isoformat(),
        "daily_reset_time": datetime.fromtimestamp(
            daily_reset_ts,
            LIVE_MARKET_TIMEZONE,
        ).isoformat(),
        "monthly_start_time": datetime.fromtimestamp(
            monthly_start_ts,
            LIVE_MARKET_TIMEZONE,
        ).isoformat(),
        "closed_trades_count_week": len(week_trades),
        "closed_trades_count_day": len(day_trades),
        "closed_trades_count_month": len(month_trades),
        "closed_trades_after_week_start": len(week_trades),
        "closed_trades_after_daily_start": len(day_trades),
        "ignored_old_trades_count": ignored_old_trades_count,
        "ignored_old_closed_trades": ignored_old_trades_count,
        "ignored_duplicate_count": ignored_duplicate_count,
        "weekly_losing_trades_count": len(weekly_losing_trades),
        "daily_losing_trades_count": len(daily_losing_trades),
        "weekly_pl_source": "raw_ctrader_closed_deals",
        "daily_pl_source": "raw_ctrader_closed_deals",
    }

    return {
        "daily_realized_pl": daily_realized_pl,
        "weekly_realized_pl": weekly_realized_pl,
        "monthly_realized_pl": round(sum(item["profit"] for item in month_trades), 2),
        "day_trades": [item["trade"] for item in day_trades],
        "week_trades": [item["trade"] for item in week_trades],
        "month_trades": [item["trade"] for item in month_trades],
        "daily_reset_ts": daily_reset_ts,
        "weekly_reset_ts": weekly_reset_ts,
        "monthly_start_ts": monthly_start_ts,
        "debug": debug,
    }

def live_trade_has_broker_id(trade):
    if not isinstance(trade, dict):
        return False

    return bool(
        trade.get("position_id")
        or trade.get("broker_position_id")
        or trade.get("broker_order_id")
        or trade.get("order_id")
    )

def normalize_ctrader_money_value(value, trade=None):
    if value is None:
        return None

    raw = trade.get("raw") if isinstance(trade, dict) else None
    money_digits = 2

    if isinstance(raw, dict):
        money_digits = raw.get("moneyDigits", money_digits)

    if isinstance(trade, dict):
        money_digits = trade.get("moneyDigits", money_digits)

    parsed = parse_ctrader_money(value, money_digits)

    if parsed is None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return parsed

def extract_broker_trade_pl(trade):
    if not isinstance(trade, dict):
        return None, None

    raw = trade.get("raw") if isinstance(trade.get("raw"), dict) else {}
    raw_broker_keys = [
        "netUnrealizedPnL",
        "grossUnrealizedPnL",
        "netProfit",
        "unrealizedProfit",
        "netUnrealizedProfit",
        "unrealizedNetProfit",
        "profit",
        "pnl",
        "pl",
        "grossProfit",
        "moneyProfit",
        "grossUnrealizedProfit",
    ]
    trade_broker_keys = [
        "netUnrealizedPnL",
        "grossUnrealizedPnL",
        "netProfit",
        "net_profit",
        "unrealizedProfit",
        "netUnrealizedProfit",
        "unrealizedNetProfit",
        "broker_pnl",
        "pnl",
        "profit",
        "pl",
        "grossProfit",
        "moneyProfit",
        "grossUnrealizedProfit",
    ]

    for source_name, source, broker_keys in [
        ("raw", raw, raw_broker_keys),
        ("trade", trade, trade_broker_keys),
    ]:
        for key in broker_keys:
            if key not in source or source.get(key) is None:
                continue

            if (
                source_name == "trade"
                and key in ["pnl", "profit", "pl", "broker_pnl"]
                and trade.get("broker_pnl_source") == "fallback"
            ):
                continue

            value = normalize_ctrader_money_value(source.get(key), trade)

            if value is not None:
                return value, f"{source_name}.{key}"

    return None, None

def extract_trade_pl(trade):
    if not isinstance(trade, dict):
        return 0

    broker_pl, _source = extract_broker_trade_pl(trade)

    if broker_pl is not None:
        return broker_pl

    result = trade.get("result")

    for key in [
        "pnl",
        "profit",
        "pl",
        "floating_pl",
        "floating_pnl",
        "floatingProfit",
        "floatingPnl",
        "unrealizedProfit",
        "unrealizedNetProfit",
        "grossUnrealizedProfit",
        "netUnrealizedProfit",
        "net_profit",
        "netProfit",
        "grossProfit",
        "moneyProfit",
        "broker_pnl",
    ]:
        value = trade.get(key)

        if value is None and isinstance(result, dict):
            value = result.get(key)

        try:
            return float(value)
        except (TypeError, ValueError):
            continue

    return 0

def extract_broker_realized_pl(trade):
    if not isinstance(trade, dict):
        return None, None

    raw = trade.get("raw") if isinstance(trade.get("raw"), dict) else {}
    raw_realized_keys = [
        "netProfit",
        "profit",
        "grossProfit",
        "moneyProfit",
        "realizedProfit",
        "realizedNetProfit",
        "closedProfit",
    ]
    trade_realized_keys = [
        "broker_realized_profit",
        "realized_profit",
        "realizedProfit",
        "realizedNetProfit",
        "closed_profit",
        "closedProfit",
        "netProfit",
        "grossProfit",
        "moneyProfit",
    ]

    for key in raw_realized_keys:
        if key not in raw or raw.get(key) is None:
            continue

        value = normalize_ctrader_money_value(raw.get(key), trade)

        if value is not None:
            return value, f"raw.{key}"

    for key in trade_realized_keys:
        if key not in trade or trade.get(key) is None:
            continue

        if key in [
            "broker_realized_profit",
            "realized_profit",
            "closed_profit",
        ]:
            try:
                value = float(trade.get(key))
            except (TypeError, ValueError):
                value = None
            if value is not None and math.isfinite(value):
                return value, f"trade.{key}"

        value = normalize_ctrader_money_value(trade.get(key), trade)

        if value is not None:
            return value, f"trade.{key}"

    source = str(trade.get("broker_pnl_source") or "").lower()

    if source and source not in ["fallback", "missing"]:
        for key in ["broker_pnl", "pnl", "profit", "pl"]:
            if key not in trade or trade.get(key) is None:
                continue

            value = normalize_ctrader_money_value(trade.get(key), trade)

            if value is not None:
                return value, f"trade.{key}:{source}"

    return None, None

def get_trade_result_from_pnl(pnl):
    try:
        value = float(pnl)
    except (TypeError, ValueError):
        return "BROKER_CLOSED"

    if value > 0:
        return "WIN"
    if value < 0:
        return "LOSS"

    return "BROKER_CLOSED"

def calculate_tp1_from_tp2(entry, tp2, side):
    entry_value = float(entry)
    tp2_value = float(tp2)

    if str(side or "").upper() == "BUY":
        return entry_value + ((tp2_value - entry_value) * 0.50)

    return entry_value - ((entry_value - tp2_value) * 0.50)

def calculate_protected_sl_price(entry, tp2, side):
    entry_value = float(entry)
    tp2_value = float(tp2)

    if str(side or "").upper() == "BUY":
        return entry_value + ((tp2_value - entry_value) * 0.50)

    return entry_value - ((entry_value - tp2_value) * 0.50)

def live_sl_protection_confirmed(trade):
    if not isinstance(trade, dict) or not trade.get("profit_protected"):
        return False

    broker_result = trade.get("sl_protection_broker_result")

    return isinstance(broker_result, dict) and broker_result.get("ok") is True

def log_trade_visual_levels(trade):
    if not isinstance(trade, dict):
        return

    print("TRADE_VISUAL_LEVELS =", {
        "symbol": normalize_symbol(trade.get("symbol")),
        "entry": trade.get("entry"),
        "original_sl": trade.get("original_sl"),
        "current_sl": trade.get("sl"),
        "planned_sl": trade.get("planned_sl"),
        "broker_stop_loss_confirmed": trade.get("broker_stop_loss_confirmed"),
        "broker_stop_loss_missing": trade.get("broker_stop_loss_missing"),
        "tp1": trade.get("tp1"),
        "tp2": trade.get("tp2"),
        "planned_tp1": trade.get("planned_tp1"),
        "planned_tp2": trade.get("planned_tp2"),
        "broker_take_profit_confirmed": trade.get("broker_take_profit_confirmed"),
        "broker_take_profit_missing": trade.get("broker_take_profit_missing"),
        "hit_tp1": bool(trade.get("hit_tp1")),
        "profit_protected": live_sl_protection_confirmed(trade),
        "protected_sl_price": trade.get("protected_sl_price"),
    })

def protect_live_trade_after_tp1(trade):
    if not isinstance(trade, dict) or trade.get("hit_tp1"):
        return trade

    try:
        symbol = normalize_symbol(trade.get("symbol"))
        decimals = 2 if symbol == "XAUUSD" else 5
        protected_sl = round(
            calculate_protected_sl_price(
                trade.get("entry"),
                trade.get("tp2"),
                trade.get("side") or trade.get("action")
            ),
            decimals
        )
    except (TypeError, ValueError):
        return trade

    position_id = (
        trade.get("position_id")
        or trade.get("broker_position_id")
    )
    original_sl = trade.get("original_sl", trade.get("sl"))

    if not position_id:
        trade["hit_tp1"] = True
        trade["profit_protected"] = False
        trade["protected_sl_price"] = protected_sl
        trade["original_sl"] = original_sl
        trade["result"] = "TP1 HIT"
        trade["sl_protection_failed"] = True
        trade["sl_protection_warning"] = "TP1 hit, but broker SL protection failed"
        trade["sl_protection_error"] = "Missing cTrader position id"

        print("LIVE_SL_PROTECTION_FAILED:", {
            "symbol": symbol,
            "side": trade.get("side") or trade.get("action"),
            "position_id": position_id,
            "entry": trade.get("entry"),
            "tp1": trade.get("tp1"),
            "tp2": trade.get("tp2"),
            "original_sl": original_sl,
            "protected_sl_price": protected_sl,
            "reason": "Missing cTrader position id",
        })

        return trade

    modify_result = modify_position_stop_loss(position_id, protected_sl)

    trade["hit_tp1"] = True
    trade["protected_sl_price"] = protected_sl
    trade["original_sl"] = original_sl
    trade["result"] = "TP1 HIT"

    if not modify_result.get("ok"):
        trade["profit_protected"] = False
        trade["sl_protection_failed"] = True
        trade["sl_protection_warning"] = "TP1 hit, but broker SL protection failed"
        trade["sl_protection_error"] = modify_result.get("reason") or "Unknown cTrader SL modify error"
        trade["sl_protection_broker_result"] = modify_result

        print("LIVE_SL_PROTECTION_FAILED:", {
            "symbol": symbol,
            "side": trade.get("side") or trade.get("action"),
            "position_id": position_id,
            "entry": trade.get("entry"),
            "tp1": trade.get("tp1"),
            "tp2": trade.get("tp2"),
            "original_sl": original_sl,
            "protected_sl_price": protected_sl,
            "reason": trade["sl_protection_error"],
            "broker_result": modify_result,
        })

        return trade

    trade["profit_protected"] = True
    trade["sl"] = protected_sl
    trade["sl_protection_failed"] = False
    trade["sl_protection_warning"] = None
    trade["sl_protection_error"] = None
    trade["sl_protection_broker_result"] = modify_result

    print("TP1_HIT_PROTECT_PROFIT:", {
        "symbol": symbol,
        "side": trade.get("side") or trade.get("action"),
        "entry": trade.get("entry"),
        "tp1": trade.get("tp1"),
        "tp2": trade.get("tp2"),
        "original_sl": original_sl,
        "protected_sl_price": protected_sl,
        "profit_protected": True,
    })

    print("LIVE_SL_PROTECTED_ON_BROKER:", {
        "symbol": symbol,
        "side": trade.get("side") or trade.get("action"),
        "position_id": position_id,
        "entry": trade.get("entry"),
        "tp1": trade.get("tp1"),
        "tp2": trade.get("tp2"),
        "original_sl": original_sl,
        "protected_sl_price": protected_sl,
        "broker_result": modify_result,
    })

    return trade

def update_live_trade_tp_protection(trade):
    if not isinstance(trade, dict):
        return trade

    hit_tp1_before = bool(trade.get("hit_tp1"))
    side = str(trade.get("side") or trade.get("action") or "").upper()

    def optional_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    current_price = optional_float(trade.get("current_price"))
    current_high = optional_float(trade.get("current_high"))
    current_low = optional_float(trade.get("current_low"))
    tp1 = optional_float(trade.get("tp1"))
    tp2 = optional_float(trade.get("tp2"))

    if current_high is None:
        current_high = current_price

    if current_low is None:
        current_low = current_price

    tp1_hit_detected = bool(
        tp1 is not None
        and (
            (side == "BUY" and current_high is not None and current_high >= tp1)
            or
            (side == "SELL" and current_low is not None and current_low <= tp1)
        )
    )
    tp2_hit_detected = bool(
        tp2 is not None
        and (
            (side == "BUY" and current_high is not None and current_high >= tp2)
            or
            (side == "SELL" and current_low is not None and current_low <= tp2)
        )
    )

    if tp2_hit_detected and not trade.get("tp2_close_requested"):
        position_id = (
            trade.get("position_id")
            or trade.get("broker_position_id")
        )
        volume_units = (
            trade.get("volume_units")
            or ((trade.get("raw") or {}).get("tradeData") or {}).get("volume")
        )
        close_result = close_position(position_id, volume=volume_units)
        trade["tp2_hit"] = True
        trade["tp2_close_requested"] = close_result.get("ok") is True
        trade["tp2_close_result"] = close_result

        if close_result.get("ok"):
            trade["status"] = "CLOSING"
            trade["result"] = "TP2 HIT"
            trade["exit_status"] = "CLOSING"
            trade["exit_reason"] = "TP2 hit; broker close requested"
        else:
            trade["tp2_close_failed"] = True
            trade["tp2_close_error"] = (
                close_result.get("reason")
                or "Broker close request failed"
            )

        print("LIVE_TP2_CLOSE_DEBUG =", {
            "symbol": normalize_symbol(trade.get("symbol")),
            "side": side,
            "position_id": position_id,
            "volume_units": volume_units,
            "tp2": tp2,
            "current_high": current_high,
            "current_low": current_low,
            "close_result": close_result,
        })
        return trade

    if hit_tp1_before or tp1 is None or tp2 is None:
        print("LIVE_TP1_PROTECTION_DEBUG", {
            "symbol": normalize_symbol(trade.get("symbol")),
            "side": side,
            "current_price": current_price,
            "current_high": current_high,
            "current_low": current_low,
            "tp1": tp1,
            "tp2": tp2,
            "hit_tp1_before": hit_tp1_before,
            "tp1_hit_detected": tp1_hit_detected,
            "protected_sl_price": trade.get("protected_sl_price"),
        })
        return trade

    if tp1_hit_detected:
        trade = protect_live_trade_after_tp1(trade)

    print("LIVE_TP1_PROTECTION_DEBUG", {
        "symbol": normalize_symbol(trade.get("symbol")),
        "side": side,
        "current_price": current_price,
        "current_high": current_high,
        "current_low": current_low,
        "tp1": tp1,
        "tp2": tp2,
        "hit_tp1_before": hit_tp1_before,
        "tp1_hit_detected": tp1_hit_detected,
        "protected_sl_price": trade.get("protected_sl_price"),
    })

    return trade

def calculate_live_trade_stats():
    run_weekly_live_reset()

    active_ids = {
        str(get_live_trade_match_key(trade))
        for trade in LIVE_ACTIVE_ORDERS.values()
        if trade and get_live_trade_match_key(trade)
    }
    broker_history = get_live_broker_closed_history()
    history_source = broker_history if broker_history else LIVE_TRADE_HISTORY
    today_history = [
        trade for trade in history_source
        if trade_is_today(trade)
        and str(get_live_trade_match_key(trade)) not in active_ids
        and (broker_history or is_usable_local_live_history_trade(trade))
    ]
    today_active = [
        trade for trade in LIVE_ACTIVE_ORDERS.values()
        if trade and trade_is_today(trade)
    ]
    seen = set()
    total_today = 0

    for trade in today_history + today_active:
        trade_key = (
            trade.get("broker_order_id")
            or trade.get("position_id")
            or trade.get("order_id")
            or id(trade)
        )

        if trade_key in seen:
            continue

        seen.add(trade_key)
        total_today += 1

    wins = 0
    losses = 0
    closed = 0
    total_pl = 0

    for trade in today_history:
        status = get_live_trade_status(trade)

        if status in ["RUNNING", "OPEN"]:
            continue

        closed += 1
        broker_pl, _source = extract_broker_trade_pl(trade)
        pl = broker_pl if broker_pl is not None else 0
        total_pl += pl

        if status in ["WIN", "WON", "PROFIT"] or pl > 0:
            wins += 1
        elif status in ["LOSS", "LOST"] or pl < 0:
            losses += 1

    active_pl = sum(get_stored_live_floating_pl(trade) for trade in today_active)
    total_pl += active_pl

    running = sum(
        1
        for trade in LIVE_ACTIVE_ORDERS.values()
        if trade and get_live_trade_status(trade) in ["RUNNING", "OPEN"]
    )

    return {
        "total_today": total_today,
        "wins": wins,
        "losses": losses,
        "running": running,
        "closed": closed,
        "total_pl": round(total_pl, 2),
        "total_pnl": round(total_pl, 2),
    }

def calculate_live_pl_sync():
    run_weekly_live_reset()
    weekly_history = get_live_broker_closed_history(force=True)
    monthly_history = get_live_broker_monthly_history(force=True)
    raw_closed_trades = list(weekly_history or []) + list(monthly_history or [])
    closed_windows = calculate_closed_pl_windows(raw_closed_trades)
    daily_realized_pl = closed_windows["daily_realized_pl"]
    weekly_realized_pl = closed_windows["weekly_realized_pl"]
    monthly_realized_pl = closed_windows["monthly_realized_pl"]

    print("LIVE_PL_WINDOW_DEBUG =", closed_windows["debug"])
    print("WEEKLY_PL_TOTAL =", weekly_realized_pl)

    open_trades = [
        trade
        for trade in LIVE_ACTIVE_ORDERS.values()
        if (
            isinstance(trade, dict)
            and live_trade_has_broker_id(trade)
            and get_live_trade_status(trade) in ["RUNNING", "OPEN", "TP1 HIT"]
        )
    ]
    open_position_debug = []
    floating_values = []

    for trade in open_trades:
        floating_pl = get_stored_live_floating_pl(trade)
        floating_values.append(floating_pl)
        open_position_debug.append({
            "position_id": (
                trade.get("position_id")
                or trade.get("broker_position_id")
                or trade.get("broker_order_id")
                or trade.get("order_id")
            ),
            "symbol": normalize_symbol(trade.get("symbol")),
            "entry": trade.get("entry"),
            "current_price": trade.get("current_price")
                or trade.get("currentPrice"),
            "side": trade.get("side") or trade.get("action"),
            "volume": trade.get("volume_units")
                or trade.get("volume")
                or trade.get("lot_size"),
            "floating_pl": round(floating_pl, 2),
        })

    floating_live_pl = sum(floating_values)
    daily_total_pl = daily_realized_pl + floating_live_pl
    weekly_total_pl = weekly_realized_pl + floating_live_pl

    print("LIVE_PL_DEBUG =", {
        **closed_windows["debug"],
        "open_positions": open_position_debug,
        "position_id": (
            open_position_debug[0]["position_id"]
            if open_position_debug else None
        ),
        "symbol": (
            open_position_debug[0]["symbol"]
            if open_position_debug else None
        ),
        "entry": (
            open_position_debug[0]["entry"]
            if open_position_debug else None
        ),
        "current_price": (
            open_position_debug[0]["current_price"]
            if open_position_debug else None
        ),
        "side": (
            open_position_debug[0]["side"]
            if open_position_debug else None
        ),
        "volume": (
            open_position_debug[0]["volume"]
            if open_position_debug else None
        ),
        "floating_pl": (
            open_position_debug[0]["floating_pl"]
            if open_position_debug else 0
        ),
        "floating_live_pl": round(floating_live_pl, 2),
        "daily_realized_pl": round(daily_realized_pl, 2),
        "daily_total_pl": round(daily_total_pl, 2),
        "weekly_realized_pl": round(weekly_realized_pl, 2),
        "monthly_realized_pl": round(monthly_realized_pl, 2),
        "weekly_total_pl": round(weekly_total_pl, 2),
    })
    log_live_trade_audit(
        "pl_sync",
        {"source": "broker", "status": "PL_SYNC", "result": "PL_SYNC"},
        weekly_realized_pl=round(weekly_realized_pl, 2),
        floating_live_pl=round(floating_live_pl, 2),
        weekly_total_pl=round(weekly_total_pl, 2),
        reason=f"open_positions={len(open_trades)}"
    )

    return {
        "pl_calculation_version": "closed-windows-v2",
        "daily_realized_pl": round(daily_realized_pl, 2),
        "daily_total_pl": round(daily_total_pl, 2),
        "weekly_realized_pl": round(weekly_realized_pl, 2),
        "monthly_realized_pl": round(monthly_realized_pl, 2),
        "floating_live_pl": round(floating_live_pl, 2),
        "weekly_total_pl": round(weekly_total_pl, 2),
        "open_positions_count": len(open_trades),
        "daily_reset_ts": closed_windows["daily_reset_ts"],
        "weekly_reset_ts": closed_windows["weekly_reset_ts"],
        "monthly_start_ts": closed_windows["monthly_start_ts"],
        "calculated_at": time.time(),
        "debug": closed_windows["debug"],
    }


def get_performance_data():
    monthly_trades = get_closed_deals_for_current_month(max_rows=500)
    if monthly_trades:
        closed_trades = [
            trade
            for trade in monthly_trades
            if trade_is_current_week(trade)
        ]
    else:
        closed_trades = get_live_broker_closed_history(force=False)
        monthly_trades = list(closed_trades)
    active_trades = [
        trade
        for trade in LIVE_ACTIVE_ORDERS.values()
        if (
            isinstance(trade, dict)
            and get_live_trade_status(trade)
            in ["RUNNING", "OPEN", "TP1 HIT"]
        )
    ]
    floating_pnl = sum(
        get_stored_live_floating_pl(trade)
        for trade in active_trades
    )
    return {
        "closed_trades": closed_trades,
        "monthly_trades": monthly_trades,
        "active_trades": active_trades,
        "floating_pnl": floating_pnl,
    }


configure_performance_data_provider(get_performance_data)

def calculate_live_trade_pips(symbol, side, entry, current_price):
    try:
        entry_value = float(entry)
        current_value = float(current_price)
    except (TypeError, ValueError):
        return None

    pip_size = 0.01 if normalize_symbol(symbol) == "XAUUSD" else 0.0001
    normalized_side = normalize_live_trade_side(side)

    if normalized_side == "BUY":
        price_difference = current_value - entry_value
    elif normalized_side == "SELL":
        price_difference = entry_value - current_value
    else:
        return None

    return round(price_difference / pip_size, 1)

def normalize_live_trade_side(side):
    value = str(side or "").strip().upper()

    if value in ["1", "BUY", "LONG"]:
        return "BUY"
    if value in ["2", "SELL", "SHORT"]:
        return "SELL"

    return value

def calculate_live_floating_pl_from_prices(trade):
    if not isinstance(trade, dict):
        return None

    symbol = normalize_symbol(trade.get("symbol"))
    side = normalize_live_trade_side(trade.get("side") or trade.get("action"))
    position_current_price = trade.get("current_price") or trade.get("currentPrice")
    live_prices = {}

    try:
        live_prices = (get_live_prices() or {}).get("live_prices", {})
    except Exception:
        live_prices = {}

    live_tick = live_prices.get(symbol) or {}
    live_bid = live_tick.get("bid")
    live_ask = live_tick.get("ask")
    used_price = position_current_price

    if side == "BUY" and live_bid is not None:
        used_price = live_bid
    elif side == "SELL" and live_ask is not None:
        used_price = live_ask

    pips = calculate_live_trade_pips(
        symbol,
        side,
        trade.get("entry"),
        used_price
    )

    if pips is None:
        print("LIVE_PL_SOURCE =", {
            "symbol": symbol,
            "side": side,
            "entry": trade.get("entry"),
            "position_current_price": position_current_price,
            "live_bid": live_bid,
            "live_ask": live_ask,
            "used_price": used_price,
            "floating_pl": None,
            "reason": "Cannot calculate pips"
        })
        return None

    lots = get_live_trade_display_lots(trade)

    try:
        lots = float(lots or 0)
    except (TypeError, ValueError):
        lots = 0

    if lots <= 0:
        return None

    try:
        metadata = get_ctrader_symbol_risk_metadata(symbol) or {}
    except Exception:
        metadata = {}

    try:
        pip_value_per_lot = float(
            metadata.get("pip_value_per_lot")
            or (1.0 if symbol == "XAUUSD" else 10.0)
        )
    except (TypeError, ValueError):
        pip_value_per_lot = 1.0 if symbol == "XAUUSD" else 10.0

    try:
        entry_value = float(trade.get("entry"))
        used_price_value = float(used_price)
    except (TypeError, ValueError):
        entry_value = None
        used_price_value = None

    if entry_value is not None and used_price_value is not None:
        if side == "BUY":
            price_difference = used_price_value - entry_value
        elif side == "SELL":
            price_difference = entry_value - used_price_value
        else:
            price_difference = None
    else:
        price_difference = None

    floating_pl = round(pips * lots * pip_value_per_lot, 2)
    broker_net_pl, _broker_net_pl_source = extract_broker_trade_pl(trade)

    print("LIVE_PL_SOURCE =", {
        "symbol": symbol,
        "side": side,
        "entry": trade.get("entry"),
        "position_current_price": position_current_price,
        "live_bid": live_bid,
        "live_ask": live_ask,
        "used_price": used_price,
        "floating_pl": floating_pl
    })
    print("FLOATING_PL_DIRECTION_DEBUG =", {
        "symbol": symbol,
        "side": side,
        "entry": trade.get("entry"),
        "current_price": used_price,
        "price_difference": price_difference,
        "floating_pl": floating_pl,
        "broker_net_pl": broker_net_pl,
    })

    return floating_pl

def get_broker_pl_debug_fields(trade):
    if not isinstance(trade, dict):
        return {}

    raw = trade.get("raw") if isinstance(trade.get("raw"), dict) else {}

    return {
        "broker_profit_field": raw.get("profit", trade.get("profit")),
        "broker_net_profit_field": raw.get("netProfit", trade.get("netProfit")),
        "broker_unrealized_profit_field": (
            raw.get("unrealizedProfit")
            if raw.get("unrealizedProfit") is not None
            else trade.get("unrealizedProfit")
        ),
        "broker_unrealized_net_profit_field": (
            trade.get("netUnrealizedPnL")
            if trade.get("netUnrealizedPnL") is not None
            else raw.get("netUnrealizedPnL")
        ),
        "broker_unrealized_net_profit_legacy_field": (
            raw.get("netUnrealizedProfit")
            if raw.get("netUnrealizedProfit") is not None
            else raw.get("unrealizedNetProfit", trade.get("unrealizedNetProfit"))
        ),
        "commission": raw.get("commission", trade.get("commission")),
        "swap": raw.get("swap", trade.get("swap")),
    }

def get_live_floating_pl(trade):
    broker_pl, broker_pl_source = extract_broker_trade_pl(trade)
    fallback_pl = None
    source = broker_pl_source or "fallback"
    used_pl = broker_pl

    if broker_pl is None:
        fallback_pl = calculate_live_floating_pl_from_prices(trade)
        used_pl = fallback_pl
    elif broker_pl_source in ["trade.netUnrealizedPnL", "raw.netUnrealizedPnL"]:
        print("LIVE_PL_SOURCE =", {
            "position_id": (
                trade.get("position_id")
                or trade.get("broker_position_id")
                or trade.get("id")
            ),
            "source": "netUnrealizedPnL",
            "value": round(broker_pl, 2),
        })

    if used_pl is None:
        used_pl = 0
        source = "missing"

    symbol = normalize_symbol(trade.get("symbol")) if isinstance(trade, dict) else ""
    side = normalize_live_trade_side(
        trade.get("side") or trade.get("action")
    ) if isinstance(trade, dict) else ""
    current_price = (
        trade.get("current_price")
        or trade.get("currentPrice")
        if isinstance(trade, dict)
        else None
    )
    price_difference = None

    try:
        entry_value = float(trade.get("entry"))
        current_price_value = float(current_price)
    except (TypeError, ValueError, AttributeError):
        entry_value = None
        current_price_value = None

    if entry_value is not None and current_price_value is not None:
        if side == "BUY":
            price_difference = current_price_value - entry_value
        elif side == "SELL":
            price_difference = entry_value - current_price_value

    raw_volume = (
        trade.get("volume_units")
        or trade.get("volumeInUnits")
        or (trade.get("tradeData") or {}).get("volume")
        or trade.get("volume")
        if isinstance(trade, dict)
        else None
    )

    print("LIVE_PL_SCALE_DEBUG =", {
        "symbol": symbol,
        "broker_net_pl": None if broker_pl is None else round(broker_pl, 2),
        "fallback_pl": None if fallback_pl is None else round(fallback_pl, 2),
        "raw_volume": raw_volume,
        "displayed_lots": get_live_trade_display_lots(trade),
        "lot_size": trade.get("lot_size") if isinstance(trade, dict) else None,
        "used_pl": round(used_pl, 2),
        "source": source,
    })
    broker_debug_fields = get_broker_pl_debug_fields(trade)
    print("LIVE_PL_FINAL_DEBUG =", {
        "position_id": (
            trade.get("position_id")
            or trade.get("broker_position_id")
            or trade.get("id")
            if isinstance(trade, dict)
            else None
        ),
        "netProfit": broker_debug_fields.get("broker_net_profit_field"),
        "profit": broker_debug_fields.get("broker_profit_field"),
        "unrealizedProfit": broker_debug_fields.get("broker_unrealized_profit_field"),
        "unrealizedNetProfit": broker_debug_fields.get("broker_unrealized_net_profit_field"),
        "commission": broker_debug_fields.get("commission"),
        "swap": broker_debug_fields.get("swap"),
        "final_pl_sent_to_frontend": round(used_pl, 2),
    })
    print("FLOATING_PL_DIRECTION_DEBUG =", {
        "symbol": symbol,
        "side": side,
        "entry": trade.get("entry") if isinstance(trade, dict) else None,
        "current_price": current_price,
        "price_difference": price_difference,
        "floating_pl": round(used_pl, 2),
        "broker_net_pl": broker_pl,
    })

    return round(used_pl, 2)

def get_stored_live_floating_pl(trade):
    if not isinstance(trade, dict):
        return 0

    for key in ["floating_pl", "floating_pnl", "broker_pnl", "pnl", "profit"]:
        try:
            value = float(trade.get(key))
        except (TypeError, ValueError):
            continue

        return round(value, 2)

    return 0

def get_default_broker_lot_size(symbol):
    return 100 if normalize_symbol(symbol) == "XAUUSD" else 100000

def get_ctrader_volume_read_scale(symbol):
    return CTRADER_PAYLOAD_VOLUME_SCALE.get(normalize_symbol(symbol), 1)

def get_live_trade_display_lots(trade):
    if not isinstance(trade, dict):
        return None

    symbol = normalize_symbol(trade.get("symbol"))
    raw_volume = (
        trade.get("volume_units")
        or trade.get("volumeInUnits")
        or (trade.get("tradeData") or {}).get("volume")
    )

    lots = normalize_broker_volume_to_lots(raw_volume, symbol)

    if lots is not None:
        return lots

    lots = normalize_broker_volume_to_lots(trade.get("volume"), symbol)

    if lots is not None:
        return lots

    try:
        lot_size = float(trade.get("lot_size"))
    except (TypeError, ValueError):
        return None

    return round(lot_size, 2) if lot_size > 0 else None

def get_trade_identifier_set(trade):
    if not isinstance(trade, dict):
        return set()

    return {
        str(value)
        for value in [
            trade.get("position_id"),
            trade.get("broker_position_id"),
            trade.get("broker_order_id"),
            trade.get("order_id"),
        ]
        if value
    }

def broker_position_matches_trade(position, trade):
    if not isinstance(position, dict) or not isinstance(trade, dict):
        return False

    position_ids = get_trade_identifier_set(position)
    trade_ids = get_trade_identifier_set(trade)

    if position_ids and trade_ids and position_ids.intersection(trade_ids):
        return True

    position_symbol = normalize_symbol(position.get("symbol"))
    trade_symbol = normalize_symbol(trade.get("symbol"))
    position_side = str(position.get("side") or "").upper()
    trade_side = str(trade.get("side") or trade.get("action") or "").upper()

    return (
        position_symbol == trade_symbol
        and bool(position_side)
        and position_side == trade_side
    )

def find_matching_broker_position(trade, positions):
    for position in positions:
        if broker_position_matches_trade(position, trade):
            return position

    return None

def load_live_backup():
    global LAST_LIVE_RESET

    if not os.path.exists(LIVE_BACKUP_FILE):
        return

    try:
        with open(LIVE_BACKUP_FILE, "r") as f:
            backup = json.load(f)

        active_orders = backup.get("live_active_orders", {})
        history = backup.get("live_trade_history", [])
        last_execution_time = backup.get("live_last_execution_time", {})
        LAST_LIVE_RESET = float(backup.get(LIVE_RESET_KEY, 0) or 0)

        if not isinstance(active_orders, dict):
            return

        for symbol, trade in active_orders.items():
            execution_symbol = normalize_symbol(symbol)

            if execution_symbol in LIVE_ACTIVE_ORDERS and trade:
                LIVE_ACTIVE_ORDERS[execution_symbol] = {
                    **trade,
                    "symbol": execution_symbol
                }

        if isinstance(history, list):
            LIVE_TRADE_HISTORY[:] = [
                {
                    **trade,
                    "symbol": normalize_symbol(trade.get("symbol"))
                }
                for trade in history[:MAX_LIVE_TRADE_HISTORY]
                if isinstance(trade, dict)
            ]

        if isinstance(last_execution_time, dict):
            for symbol, timestamp in last_execution_time.items():
                execution_symbol = normalize_symbol(symbol)

                if execution_symbol in LIVE_LAST_EXECUTION_TIME:
                    try:
                        LIVE_LAST_EXECUTION_TIME[execution_symbol] = float(timestamp or 0)
                    except (TypeError, ValueError):
                        pass

        print("LIVE BACKUP LOADED:", get_persistable_live_active_orders())
        run_weekly_live_reset()

    except Exception as e:
        print("LIVE BACKUP LOAD ERROR:", e)

load_live_backup()

def sync_ctrader_account_state(force=False):
    connector_state = get_connection_state(force=force)

    LIVE_ACCOUNT_STATE["connected"] = connector_state["connected"]
    LIVE_ACCOUNT_STATE["mode"] = connector_state["mode"]
    LIVE_ACCOUNT_STATE["broker"] = "ctrader"
    LIVE_ACCOUNT_STATE["account_id"] = connector_state.get("account_id")
    LIVE_ACCOUNT_STATE["execution_ready"] = connector_state.get("execution_ready", False)
    LIVE_ACCOUNT_STATE["auth_ok"] = connector_state.get("auth_ok", False)
    LIVE_ACCOUNT_STATE["account_found"] = connector_state.get("account_found", False)
    LIVE_ACCOUNT_STATE["reason"] = connector_state.get("reason")
    LIVE_ACCOUNT_STATE["degraded"] = connector_state.get("degraded", False)
    LIVE_ACCOUNT_STATE["consecutive_failures"] = connector_state.get(
        "consecutive_failures",
        0,
    )
    LIVE_ACCOUNT_STATE["last_success_at"] = connector_state.get(
        "last_success_at"
    )

    return LIVE_ACCOUNT_STATE

def get_signal_trade_plan(symbol):
    cached_data = PANEL_CACHE.get("data")
    execution_symbol = normalize_symbol(symbol)

    if not isinstance(cached_data, dict):
        return None

    return cached_data.get(execution_symbol)

def is_missing_trade_value(value):
    return value is None or value == "" or value == "--"

def choose_backend_trade_value(plan, payload, backend_key, *payload_keys):
    backend_value = plan.get(backend_key)

    if not is_missing_trade_value(backend_value):
        return backend_value, "backend"

    for key in payload_keys:
        payload_value = payload.get(key)

        if not is_missing_trade_value(payload_value):
            return payload_value, "payload"

    return backend_value, "missing"

def log_frontend_trade_level_mismatch(symbol, action, level_name, backend_value, payload_value):
    if is_missing_trade_value(backend_value) or is_missing_trade_value(payload_value):
        return

    try:
        backend_float = float(backend_value)
        payload_float = float(payload_value)
    except (TypeError, ValueError):
        if str(backend_value) == str(payload_value):
            return
    else:
        if backend_float == payload_float:
            return

    print(
        "CTRADER FRONTEND LEVEL MISMATCH:",
        symbol,
        action,
        level_name,
        "backend=",
        backend_value,
        "payload=",
        payload_value
    )

def log_rejected_ctrader_trade(symbol, action, entry, sl, tp1, tp2, reason):
    print(
        "CTRADER ORDER REJECTED:",
        symbol,
        action,
        entry,
        sl,
        tp1,
        tp2,
        reason
    )

def reject_ctrader_order(symbol, action, entry, sl, tp1, tp2, reason):
    log_rejected_ctrader_trade(symbol, action, entry, sl, tp1, tp2, reason)

    return {
        "ok": False,
        "broker": "ctrader",
        "mode": LIVE_ACCOUNT_STATE.get("mode", "demo"),
        "symbol": symbol,
        "action": action,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "volume": None,
        "reason": reason,
        "message": reason
    }

def get_live_trade_status(trade):
    if not isinstance(trade, dict):
        return ""

    status = trade.get("status") or trade.get("result")

    if isinstance(status, dict):
        status = status.get("status") or status.get("result")

    return str(status or "").upper()

def mark_signal_history_broker_closed(symbol):
    try:
        from brain import SIGNAL_HISTORY
    except Exception as e:
        print("LIVE SIGNAL HISTORY BROKER_CLOSE UPDATE ERROR:", e)
        return

    normalized_symbol = normalize_symbol(symbol)
    signal_symbols = {normalized_symbol}

    for trade in SIGNAL_HISTORY:
        if not isinstance(trade, dict):
            continue

        trade_symbol = normalize_symbol(trade.get("symbol"))

        if trade_symbol not in signal_symbols:
            continue

        if str(trade.get("result") or "").upper() == "RUNNING":
            trade["result"] = "BROKER_CLOSED"

def build_broker_closed_trade(trade, closed_at):
    pnl, pnl_source = extract_broker_realized_pl(trade)

    if pnl is None:
        pnl = 0
        pnl_source = "missing"

    realized_result = get_trade_result_from_pnl(pnl)

    closed_trade = {
        **trade,
        "status": realized_result,
        "result": realized_result,
        "broker_close_status": "BROKER_CLOSED",
        "pnl": pnl,
        "profit": pnl,
        "broker_pnl": pnl,
        "broker_realized_profit": pnl,
        "broker_realized_source": pnl_source,
        "broker_pnl_source": pnl_source,
        "closed_at": closed_at,
        "note": "Broker position no longer found in cTrader open positions."
    }
    ensure_live_trade_identity(closed_trade)
    log_live_trade_audit("broker_closed_build", closed_trade, reason=closed_trade.get("note"))
    return closed_trade

def apply_broker_closed_to_panel_signal_history(data):
    if not isinstance(data, dict):
        return

    history = data.get("history")

    if not isinstance(history, list):
        return

    broker_closed_symbols = {
        normalize_symbol(trade.get("symbol"))
        for trade in LIVE_TRADE_HISTORY
        if isinstance(trade, dict)
        and get_live_trade_status(trade) == "BROKER_CLOSED"
        and not LIVE_ACTIVE_ORDERS.get(normalize_symbol(trade.get("symbol")))
    }

    if not broker_closed_symbols:
        return

    for item in history:
        if not isinstance(item, dict):
            continue

        item_symbol = normalize_symbol(item.get("symbol"))

        if item_symbol not in broker_closed_symbols:
            continue

        if str(item.get("result") or "").upper() == "RUNNING":
            item["result"] = "BROKER_CLOSED"

def reject_live_execution_block(symbol, action, trade_payload, reason, log_message, details=None):
    print(log_message)

    rejected = reject_ctrader_order(
        symbol,
        action,
        trade_payload.get("entry"),
        trade_payload.get("sl"),
        trade_payload.get("tp1"),
        trade_payload.get("tp2"),
        reason
    )
    if isinstance(details, dict):
        rejected["details"] = details
        rejected["live_risk_debug"] = details.get("live_risk_debug") or details
    return rejected

def get_required_market_health_keys(symbol):
    normalized = normalize_symbol(symbol)
    prefix = "XAUUSD" if normalized == "XAUUSD" else "EURUSD"
    return [
        f"{prefix}:1h",
        f"{prefix}:15min",
        f"{prefix}:5min",
    ]

def check_live_market_data_health(symbol):
    try:
        from brain import get_ctrader_data_health

        health = get_ctrader_data_health()
    except Exception as e:
        print("LIVE EXECUTION BLOCKED: market data unhealthy", e)
        return {
            "ok": False,
            "reason": "Market data is stale/unhealthy"
        }

    required_keys = get_required_market_health_keys(symbol)
    stale_keys = health.get("stale_keys") or []
    missing_or_stale_keys = [
        key for key in required_keys
        if key in stale_keys
    ]

    if health.get("data_health") != "OK" or missing_or_stale_keys:
        print(
            "LIVE EXECUTION BLOCKED: market data unhealthy",
            {
                "symbol": normalize_symbol(symbol),
                "data_health": health.get("data_health"),
                "required_keys": required_keys,
                "missing_or_stale_keys": missing_or_stale_keys,
                "stale_keys": stale_keys,
            }
        )

        return {
            "ok": False,
            "reason": "cTrader candles stale" if missing_or_stale_keys else "cTrader candles unavailable",
            "health": health
        }

    return {
        "ok": True,
        "health": health
    }

def log_auto_trade_blocked_reason(
    symbol=None,
    signal=None,
    stage=None,
    reason=None,
    details=None
):
    print("AUTO_TRADE_BLOCKED_REASON =", {
        "symbol": normalize_symbol(symbol) if symbol else None,
        "signal": signal,
        "stage": stage,
        "reason": reason,
        "details": details,
        "live_auto_enabled": LIVE_AUTO_TRADE_ENABLED.get("enabled"),
        "broker_connected": LIVE_ACCOUNT_STATE.get("connected"),
        "active_trade": LIVE_ACTIVE_ORDERS.get(normalize_symbol(symbol)) if symbol else None,
        "timestamp": time.time(),
    })

def log_broker_min_distance_decision(
    symbol,
    distance_details=None,
    actual_blocked=False,
    final_block_reason=None
):
    details = distance_details if isinstance(distance_details, dict) else {}
    failed_fields = details.get("failed_distance_fields")

    if isinstance(failed_fields, list):
        failed_fields = [field for field in failed_fields if field]
    elif failed_fields:
        failed_fields = [failed_fields]
    else:
        failed_fields = []

    min_required = details.get("broker_minimum_distance_pips")
    sl_distance = details.get("sl_distance_pips")
    tp1_distance = details.get("tp1_distance_pips") or details.get("tp_distance_pips")
    tp2_distance = details.get("tp2_distance_pips")
    should_block_for_distance = bool(failed_fields)

    try:
        min_value = float(min_required)
        distances = [
            float(value)
            for value in [sl_distance, tp1_distance, tp2_distance]
            if value is not None
        ]

        if distances and any(distance < min_value for distance in distances):
            should_block_for_distance = True
    except (TypeError, ValueError):
        pass

    print("BROKER_MIN_DISTANCE_DECISION_DEBUG =", {
        "symbol": normalize_symbol(symbol) if symbol else None,
        "min_required_pips": min_required,
        "sl_distance_pips": sl_distance,
        "tp1_distance_pips": tp1_distance,
        "tp2_distance_pips": tp2_distance,
        "failed_distance_fields": failed_fields,
        "should_block_for_distance": should_block_for_distance,
        "actual_blocked": bool(actual_blocked),
        "final_block_reason": final_block_reason,
    })

    return should_block_for_distance

def round_down_to_step(value, step):
    try:
        numeric = float(value)
        numeric_step = float(step)
    except (TypeError, ValueError):
        return 0

    if numeric <= 0 or numeric_step <= 0:
        return 0

    return math.floor(numeric / numeric_step) * numeric_step

def round_volume_down_to_step(volume_units, step_units):
    try:
        volume = int(float(volume_units))
        step = int(float(step_units))
    except (TypeError, ValueError):
        return 0

    if volume <= 0 or step <= 0:
        return 0

    return (volume // step) * step

def log_volume_safety_debug(details):
    if not isinstance(details, dict):
        return

    pip_size = details.get("pip_size")
    stop_loss_pips = details.get("sl_pips")
    stop_loss_price_distance = details.get("stop_loss_price_distance")

    if stop_loss_price_distance is None:
        try:
            stop_loss_price_distance = float(stop_loss_pips) * float(pip_size)
        except (TypeError, ValueError):
            stop_loss_price_distance = None

    print("VOLUME_SAFETY_DEBUG =", {
        "symbol": details.get("symbol"),
        "risk_percent": details.get("risk_percent"),
        "stop_loss_pips": stop_loss_pips,
        "stop_loss_price_distance": (
            round(stop_loss_price_distance, 8)
            if isinstance(stop_loss_price_distance, (int, float))
            else stop_loss_price_distance
        ),
        "lot_size_before_rounding": (
            details.get("calculated_lots")
            if details.get("calculated_lots") is not None
            else details.get("raw_lots")
        ),
        "lot_size_after_rounding": details.get("lot_size") or details.get("rounded_lots"),
        "broker_min_volume": details.get("min_volume_units"),
        "broker_max_volume": details.get("max_volume_units"),
        "broker_volume_step": details.get("volume_step_units"),
        "final_volume": details.get("volume_units") or details.get("calculated_volume_units"),
        "blocked_reason": None if details.get("ok") else details.get("reason"),
    })

def build_live_block_details(*parts):
    merged = {}

    for part in parts:
        if isinstance(part, dict):
            merged.update(part)

    return merged

def log_live_execution_safety_check(details):
    log_volume_safety_debug(details)
    print(
        "LIVE EXECUTION SAFETY CHECK:",
        {
            "symbol": details.get("symbol"),
            "account_balance": details.get("account_balance"),
            "risk_percent": details.get("risk_percent"),
            "risk_money": details.get("risk_amount"),
            "stop_loss_pips": details.get("sl_pips"),
            "pip_value_per_1_lot": details.get("pip_value_per_lot"),
            "calculated_lot_size": (
                details.get("calculated_lots")
                if details.get("calculated_lots") is not None
                else details.get("raw_lots")
            ),
            "final_lot_size": details.get("lot_size"),
            "broker_lot_size": details.get("lot_contract_size"),
            "final_ctrader_volume_units": details.get("volume_units"),
            "broker_min_volume": details.get("min_volume_units"),
            "broker_max_volume": details.get("max_volume_units"),
            "broker_step_volume": details.get("volume_step_units"),
            "final_risk_money": details.get("final_risk_amount"),
            "final_risk_percent": details.get("final_risk_percent"),
            "requested_units": details.get("requested_units") or details.get("volume_units"),
            "max_lot_cap_used": details.get("max_lot_cap_used"),
            "broker_max_volume": details.get("max_volume_units"),
            "risk_difference": details.get("risk_difference"),
            "allowed_risk_difference": details.get("allowed_risk_difference"),
            "ok": details.get("ok"),
            "reason": details.get("reason"),
        }
    )

def build_live_risk_debug(
    symbol,
    side,
    trade_payload=None,
    risk_size=None,
    broker_reject_reason=None,
):
    trade_payload = trade_payload or {}
    risk_size = risk_size or {}
    metadata = risk_size.get("symbol_metadata") or {}
    min_volume_units = (
        risk_size.get("min_volume_units")
        or metadata.get("min_volume_units")
    )
    lot_contract_size = (
        risk_size.get("lot_contract_size")
        or metadata.get("lot_size")
        or get_default_broker_lot_size(symbol)
    )
    volume_units = risk_size.get("volume_units")
    volume_step_units = (
        risk_size.get("volume_step_units")
        or metadata.get("volume_step_units")
    )
    payload_scale = CTRADER_PAYLOAD_VOLUME_SCALE.get(normalize_symbol(symbol), 1)

    try:
        broker_min_lots = convert_ctrader_volume_to_lots(
            min_volume_units,
            lot_contract_size,
        )
    except Exception:
        broker_min_lots = None

    try:
        payload_volume = int(float(volume_units) * payload_scale)
    except (TypeError, ValueError):
        payload_volume = None

    debug = {
        "symbol": normalize_symbol(symbol),
        "side": str(side or "").upper(),
        "account_id": LIVE_ACCOUNT_STATE.get("account_id"),
        "account_balance": risk_size.get("account_balance"),
        "account_equity": risk_size.get("account_equity"),
        "risk_percent": risk_size.get("risk_percent"),
        "allowed_risk_percent": (
            risk_size.get("allowed_risk_percent")
            or risk_size.get("maximum_allowed_risk_percent")
        ),
        "risk_amount": risk_size.get("risk_amount"),
        "risk_money": risk_size.get("risk_amount"),
        "entry": trade_payload.get("entry"),
        "sl": trade_payload.get("sl"),
        "tp1": trade_payload.get("tp1"),
        "tp2": trade_payload.get("tp2"),
        "sl_distance_pips": risk_size.get("sl_pips"),
        "sl_pips": risk_size.get("sl_pips"),
        "pip_value": risk_size.get("pip_value_per_lot"),
        "pip_value_per_lot": risk_size.get("pip_value_per_lot"),
        "broker_min_lots": broker_min_lots,
        "broker_min_volume": min_volume_units,
        "broker_max_volume": (
            risk_size.get("max_volume_units")
            or metadata.get("max_volume_units")
        ),
        "broker_volume_step": volume_step_units,
        "volume_step": volume_step_units,
        "calculated_volume": (
            risk_size.get("calculated_volume_units")
            if risk_size.get("calculated_volume_units") is not None
            else risk_size.get("raw_volume_units")
        ),
        "calculated_lots": (
            risk_size.get("calculated_lots")
            if risk_size.get("calculated_lots") is not None
            else risk_size.get("raw_lots")
        ),
        "rounded_lots": (
            risk_size.get("rounded_lots")
            if risk_size.get("rounded_lots") is not None
            else risk_size.get("lot_size")
        ),
        "final_volume": volume_units,
        "requested_units": volume_units,
        "payload_volume": payload_volume,
        "final_risk_percent": risk_size.get("final_risk_percent"),
        "max_lot_cap_used": bool(risk_size.get("max_lot_cap_used", False)),
        "broker_reject_reason": broker_reject_reason,
    }
    print("LIVE_RISK_DEBUG =", debug)
    return debug

def calculate_position_size(symbol, account_balance, risk_percent, stop_loss_pips):
    execution_symbol = normalize_symbol(symbol)

    try:
        balance_value = float(account_balance)
        risk_percent_value = float(risk_percent)
        sl_pips = float(stop_loss_pips)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "reason": "Invalid risk sizing inputs"
        }

    if balance_value <= 0:
        return {
            "ok": False,
            "reason": "Cannot calculate risk without account balance"
        }

    if risk_percent_value <= 0:
        return {
            "ok": False,
            "reason": "Invalid risk percent"
        }

    if sl_pips <= 0:
        return {
            "ok": False,
            "reason": "Cannot calculate risk without valid SL"
        }

    metadata = get_ctrader_symbol_risk_metadata(execution_symbol)

    if not metadata.get("ok"):
        return {
            "ok": False,
            "reason": metadata.get("reason") or "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    pip_value_per_lot = metadata.get("pip_value_per_lot")
    lot_contract_size = metadata.get(
        "lot_size",
        get_default_broker_lot_size(execution_symbol)
    )
    min_volume_units = metadata.get("min_volume_units")
    max_volume_units = metadata.get("max_volume_units")
    volume_step_units = metadata.get("volume_step_units")

    try:
        pip_value_per_lot = float(pip_value_per_lot)
        lot_contract_size = float(lot_contract_size)
        min_volume_units = int(float(min_volume_units))
        max_volume_units = int(float(max_volume_units))
        volume_step_units = int(float(volume_step_units))
    except (TypeError, ValueError):
        return {
            "ok": False,
            "reason": "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    if (
        pip_value_per_lot <= 0
        or lot_contract_size <= 0
        or min_volume_units <= 0
        or max_volume_units <= 0
        or volume_step_units <= 0
        or min_volume_units > max_volume_units
    ):
        return {
            "ok": False,
            "reason": "Invalid broker symbol volume metadata",
            "symbol_metadata": metadata,
        }

    risk_money = balance_value * (risk_percent_value / 100)
    calculated_lots = risk_money / (sl_pips * pip_value_per_lot)

    if calculated_lots <= 0 or not math.isfinite(calculated_lots):
        return {
            "ok": False,
            "reason": "Calculated position size is invalid",
            "symbol_metadata": metadata,
        }

    raw_volume_units = convert_lots_to_ctrader_volume(
        calculated_lots,
        lot_contract_size
    )
    rounded_volume_units = round_volume_down_to_step(
        raw_volume_units,
        volume_step_units
    )
    calculated_volume_units = rounded_volume_units
    minimum_volume_rounded_up = rounded_volume_units < min_volume_units

    if minimum_volume_rounded_up:
        rounded_volume_units = min_volume_units

    rounded_lots = convert_ctrader_volume_to_lots(
        rounded_volume_units,
        lot_contract_size
    )
    broker_min_lots = convert_ctrader_volume_to_lots(
        min_volume_units,
        lot_contract_size,
    )
    payload_scale = CTRADER_PAYLOAD_VOLUME_SCALE.get(execution_symbol, 1)
    payload_volume = int(rounded_volume_units * payload_scale)
    pip_size = metadata.get("pip_size")

    try:
        stop_loss_price_distance = round(sl_pips * float(pip_size), 8)
    except (TypeError, ValueError):
        stop_loss_price_distance = None

    base_check = {
        "symbol": execution_symbol,
        "account_balance": round(balance_value, 2),
        "risk_percent": risk_percent_value,
        "risk_amount": round(risk_money, 2),
        "sl_pips": round(sl_pips, 2),
        "pip_size": pip_size,
        "stop_loss_price_distance": stop_loss_price_distance,
        "pip_value_per_lot": pip_value_per_lot,
        "calculated_lots": round(calculated_lots, 4),
        "rounded_lots": round(rounded_lots, 4) if rounded_lots else 0,
        "broker_min_lots": round(broker_min_lots, 4) if broker_min_lots else None,
        "broker_min_volume": min_volume_units,
        "volume_step": volume_step_units,
        "payload_volume": payload_volume,
        "requested_units": int(rounded_volume_units),
        "max_lot_cap_used": False,
        "lot_contract_size": lot_contract_size,
        "volume_units": int(rounded_volume_units),
        "raw_volume_units": int(raw_volume_units),
        "calculated_volume_units": int(calculated_volume_units),
        "min_volume_units": min_volume_units,
        "max_volume_units": max_volume_units,
        "volume_step_units": volume_step_units,
        "minimum_volume_rounded_up": minimum_volume_rounded_up,
    }

    if rounded_volume_units <= 0:
        result = {
            **base_check,
            "ok": False,
            "reason": "Calculated volume is invalid",
            "symbol_metadata": metadata,
        }
        log_live_execution_safety_check(result)
        return result

    if rounded_volume_units % volume_step_units != 0:
        result = {
            **base_check,
            "ok": False,
            "reason": "Calculated volume is not aligned with broker step",
            "symbol_metadata": metadata,
        }
        log_live_execution_safety_check(result)
        return result

    # Broker limits and final percentage validation are the safety ceiling.
    # A fixed one-lot cap does not scale correctly when the active account
    # changes from 10K to 100K.
    allowed_max_units = max_volume_units

    if rounded_volume_units > allowed_max_units:
        result = {
            "ok": False,
            "reason": "Calculated volume is above broker/safety maximum",
            "symbol_metadata": metadata,
            "pip_value_per_lot": pip_value_per_lot,
            "lot_size": round(calculated_lots, 4),
            "volume_units": int(rounded_volume_units),
            "raw_lots": calculated_lots,
            "raw_volume_units": int(raw_volume_units),
            "sl_pips": round(sl_pips, 2),
            "lot_contract_size": lot_contract_size,
            "volume_step_units": volume_step_units,
            "calculated_volume_units": rounded_volume_units,
            "min_volume_units": min_volume_units,
            "max_volume_units": max_volume_units,
            "broker_max_volume": max_volume_units,
            "safety_max_volume_units": allowed_max_units,
            "max_lot_cap_used": False,
        }
        log_live_execution_safety_check({**base_check, **result})
        return result

    lot_size = convert_ctrader_volume_to_lots(
        rounded_volume_units,
        lot_contract_size
    )

    if not lot_size or lot_size <= 0:
        result = {
            "ok": False,
            "reason": "Calculated lot size is invalid",
            "symbol_metadata": metadata,
        }
        log_live_execution_safety_check({**base_check, **result})
        return result

    final_risk_amount = lot_size * sl_pips * pip_value_per_lot
    final_risk_percent = (final_risk_amount / balance_value) * 100
    volume_step_lots = convert_ctrader_volume_to_lots(
        volume_step_units,
        lot_contract_size
    ) or 0
    allowed_risk_difference = (
        volume_step_lots
        * sl_pips
        * pip_value_per_lot
    ) + 0.01
    risk_difference = abs(risk_money - final_risk_amount)
    maximum_allowed_risk_percent = get_maximum_allowed_live_risk_percent(
        risk_percent_value
    )
    minimum_volume_risk_limit = (
        maximum_allowed_risk_percent + RISK_TOLERANCE_PERCENT
    )

    if (
        minimum_volume_rounded_up
        and final_risk_percent > minimum_volume_risk_limit
    ):
        result = {
            **base_check,
            "ok": False,
            "reason": (
                "LIVE BLOCKED: calculated volume below broker minimum and "
                "min volume exceeds allowed risk."
            ),
            "symbol_metadata": metadata,
            "lot_size": round(lot_size, 4),
            "final_risk_amount": round(final_risk_amount, 2),
            "final_risk_percent": round(final_risk_percent, 4),
            "maximum_allowed_risk_percent": maximum_allowed_risk_percent,
            "allowed_risk_percent": maximum_allowed_risk_percent,
            "risk_tolerance_percent": RISK_TOLERANCE_PERCENT,
        }
        log_live_execution_safety_check(result)
        return result

    if (
        final_risk_amount <= 0
        or not math.isfinite(final_risk_amount)
        or (
            not minimum_volume_rounded_up
            and (
                risk_difference > allowed_risk_difference
                or final_risk_amount > risk_money + allowed_risk_difference
            )
        )
    ):
        result = {
            **base_check,
            "ok": False,
            "reason": (
                f"Calculated risk is not close to {risk_percent_value:.2f}% "
                "after broker rounding"
            ),
            "symbol_metadata": metadata,
            "lot_size": round(lot_size, 4),
            "final_risk_amount": round(final_risk_amount, 2),
            "final_risk_percent": round(final_risk_percent, 4),
            "risk_difference": round(risk_difference, 2),
            "allowed_risk_difference": round(allowed_risk_difference, 2),
        }
        log_live_execution_safety_check(result)
        return result

    result = {
        "ok": True,
        "symbol": execution_symbol,
        "risk_percent": risk_percent_value,
        "risk_amount": round(risk_money, 2),
        "account_balance": round(balance_value, 2),
        "lot_size": round(lot_size, 4),
        "calculated_lots": round(calculated_lots, 4),
        "rounded_lots": round(lot_size, 4),
        "broker_min_lots": round(broker_min_lots, 4) if broker_min_lots else None,
        "volume_units": int(rounded_volume_units),
        "raw_lots": calculated_lots,
        "raw_volume_units": int(raw_volume_units),
        "calculated_volume_units": int(calculated_volume_units),
        "sl_pips": round(sl_pips, 2),
        "pip_size": metadata.get("pip_size"),
        "stop_loss_price_distance": base_check.get("stop_loss_price_distance"),
        "pip_value_per_lot": pip_value_per_lot,
        "pip_size": metadata.get("pip_size"),
        "tick_size": metadata.get("tick_size"),
        "tick_value": metadata.get("tick_value"),
        "pip_position": metadata.get("pip_position"),
        "lot_contract_size": lot_contract_size,
        "volume_step_units": volume_step_units,
        "broker_min_volume": min_volume_units,
        "volume_step": volume_step_units,
        "payload_volume": payload_volume,
        "requested_units": int(rounded_volume_units),
        "max_lot_cap_used": False,
        "min_volume_units": min_volume_units,
        "max_volume_units": max_volume_units,
        "broker_max_volume": max_volume_units,
        "volume_step_lots": metadata.get("volume_step_lots"),
        "min_lot": metadata.get("min_lot"),
        "max_lot": metadata.get("max_lot"),
        "metadata_source": metadata.get("metadata_source"),
        "final_risk_amount": round(final_risk_amount, 2),
        "final_risk_percent": round(final_risk_percent, 4),
        "risk_difference": round(risk_difference, 2),
        "allowed_risk_difference": round(allowed_risk_difference, 2),
        "maximum_allowed_risk_percent": maximum_allowed_risk_percent,
        "allowed_risk_percent": maximum_allowed_risk_percent,
        "risk_tolerance_percent": RISK_TOLERANCE_PERCENT,
        "minimum_volume_rounded_up": minimum_volume_rounded_up,
    }

    print(
        "LIVE POSITION SIZE:",
        {
            "symbol": result["symbol"],
            "balance": result["account_balance"],
            "risk_money": result["risk_amount"],
            "stop_loss_pips": result["sl_pips"],
            "pip_value": result["pip_value_per_lot"],
            "calculated_lots": round(calculated_lots, 4),
            "ctrader_volume_units": result["volume_units"],
            "raw_volume_units": result["raw_volume_units"],
            "min_volume_units": min_volume_units,
            "max_volume_units": max_volume_units,
            "volume_step_units": volume_step_units,
            "lot_size": lot_contract_size,
            "metadata_source": result["metadata_source"],
        }
    )
    log_live_execution_safety_check(result)

    return result

def log_position_size_example(symbol, account_balance=10000, risk_percent=0.5, stop_loss_pips=10):
    result = calculate_position_size(
        symbol,
        account_balance,
        risk_percent,
        stop_loss_pips
    )

    print(
        "LIVE POSITION SIZE EXAMPLE:",
        {
            "symbol": normalize_symbol(symbol),
            "balance": account_balance,
            "risk_percent": risk_percent,
            "stop_loss_pips": stop_loss_pips,
            "ok": result.get("ok"),
            "reason": result.get("reason"),
            "pip_value_used": result.get("pip_value_per_lot"),
            "calculated_lot": result.get("lot_size"),
            "ctrader_volume_units": result.get("volume_units"),
            "broker_min_volume": (
                result.get("min_volume_units")
                or result.get("symbol_metadata", {}).get("min_volume_units")
            ),
            "broker_max_volume": (
                result.get("max_volume_units")
                or result.get("symbol_metadata", {}).get("max_volume_units")
            ),
            "broker_volume_step": (
                result.get("volume_step_units")
                or result.get("symbol_metadata", {}).get("volume_step_units")
            ),
            "broker_lot_size": (
                result.get("lot_contract_size")
                or result.get("symbol_metadata", {}).get("lot_size")
            ),
        }
    )

    return result

def calculate_live_risk_size(symbol, entry, sl):
    execution_symbol = normalize_symbol(symbol)

    try:
        entry_value = float(entry)
        sl_value = float(sl)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "reason": "Cannot calculate risk without valid SL"
        }

    sl_distance = abs(entry_value - sl_value)

    if sl_distance <= 0:
        return {
            "ok": False,
            "reason": "Cannot calculate risk without valid SL"
        }

    account = get_ctrader_account_snapshot()

    if not account.get("ok"):
        return {
            "ok": False,
            "reason": account.get("reason") or "Cannot calculate risk without account balance",
            "account": account,
        }

    equity = account.get("equity")
    balance = account.get("balance")
    account_value = equity if equity is not None else balance

    try:
        account_value = float(account_value)
    except (TypeError, ValueError):
        account_value = 0

    if account_value <= 0:
        return {
            "ok": False,
            "reason": "Cannot calculate risk without account equity",
            "account": account,
        }

    metadata = get_ctrader_symbol_risk_metadata(execution_symbol)

    if not metadata.get("ok"):
        return {
            "ok": False,
            "reason": metadata.get("reason") or "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    pip_size = metadata.get("pip_size")

    try:
        pip_size = float(pip_size)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "reason": "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    if pip_size <= 0:
        return {
            "ok": False,
            "reason": "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    sl_pips = sl_distance / pip_size

    print("LIVE_SL_PIP_DISTANCE_CHECK:", {
        "symbol": execution_symbol,
        "entry": entry_value,
        "sl": sl_value,
        "sl_distance_price": round(sl_distance, 5),
        "pip_size": pip_size,
        "sl_pips": round(sl_pips, 2),
        "metadata_source": metadata.get("metadata_source"),
        "pip_position": metadata.get("pip_position"),
    })

    configured_risk_percent = get_configured_live_risk_percent()
    position_size = calculate_position_size(
        execution_symbol,
        account_value,
        configured_risk_percent,
        sl_pips
    )
    position_size["account_balance"] = balance
    position_size["account_equity"] = account.get("equity")
    position_size["account_equity_used"] = account_value
    print("ACCOUNT_EQUITY_USED", account_value)
    print("RISK_PERCENT_USED", configured_risk_percent)
    print("SL_DISTANCE_USED", {
        "price": round(sl_distance, 8),
        "pips": round(sl_pips, 2),
    })
    print("CALCULATED_VOLUME", {
        "lots": position_size.get("calculated_lots"),
        "volume_units": position_size.get("raw_volume_units"),
    })
    print("FINAL_VOLUME_SENT", {
        "lots": position_size.get("lot_size"),
        "volume_units": position_size.get("volume_units"),
    })
    return position_size

def build_minimum_live_size(symbol, entry, sl, failed_risk_size=None):
    execution_symbol = normalize_symbol(symbol)
    metadata = get_ctrader_symbol_risk_metadata(execution_symbol)
    failed_risk_size = failed_risk_size or {}
    lot_contract_size = (
        metadata.get("lot_size")
        or get_default_broker_lot_size(execution_symbol)
    )
    min_volume_units = metadata.get("min_volume_units")

    try:
        lot_contract_size = float(lot_contract_size)
    except (TypeError, ValueError):
        lot_contract_size = float(get_default_broker_lot_size(execution_symbol))

    try:
        min_volume_units = int(float(min_volume_units))
    except (TypeError, ValueError):
        min_volume_units = convert_lots_to_ctrader_volume(
            MIN_LOT,
            lot_contract_size
        )

    lot_size = convert_ctrader_volume_to_lots(
        min_volume_units,
        lot_contract_size
    ) or MIN_LOT

    try:
        pip_size = float(metadata.get("pip_size"))
        sl_pips = abs(float(entry) - float(sl)) / pip_size
    except (TypeError, ValueError, ZeroDivisionError):
        pip_size = metadata.get("pip_size")
        sl_pips = None

    account_balance = failed_risk_size.get("account_balance")
    account_equity = failed_risk_size.get("account_equity")

    if account_balance is None:
        account = get_ctrader_account_snapshot()

        if account.get("ok"):
            account_balance = account.get("balance") or account.get("equity")
            account_equity = account.get("equity")

    account_value = (
        account_equity
        if account_equity is not None
        else account_balance
    )

    try:
        account_value = float(account_value)
    except (TypeError, ValueError):
        account_value = 0

    try:
        account_balance_value = float(account_balance)
    except (TypeError, ValueError):
        account_balance_value = account_value

    try:
        pip_value_per_lot = float(metadata.get("pip_value_per_lot"))
    except (TypeError, ValueError):
        pip_value_per_lot = 0

    final_risk_amount = None
    final_risk_percent = None

    if (
        account_value > 0
        and sl_pips is not None
        and sl_pips > 0
        and pip_value_per_lot > 0
    ):
        final_risk_amount = float(lot_size) * sl_pips * pip_value_per_lot
        final_risk_percent = (final_risk_amount / account_value) * 100

    if final_risk_percent is None:
        fallback = {
            "ok": False,
            "symbol": execution_symbol,
            "reason": "Cannot validate broker minimum volume risk",
            "minimum_volume_fallback": True,
            "risk_sizing_failure": failed_risk_size,
            "symbol_metadata": metadata,
        }
        print("LIVE_MINIMUM_VOLUME_FALLBACK_BLOCKED =", fallback)
        return fallback

    configured_risk_percent = get_configured_live_risk_percent()
    maximum_allowed_risk_percent = get_maximum_allowed_live_risk_percent(
        configured_risk_percent
    )
    risk_difference = final_risk_percent - maximum_allowed_risk_percent
    risk_decision = (
        "BLOCK"
        if final_risk_percent > maximum_allowed_risk_percent + RISK_TOLERANCE_PERCENT
        else "ALLOW"
    )
    print("RISK_VALIDATION =", {
        "symbol": execution_symbol,
        "actual_risk": round(final_risk_percent, 4),
        "max_risk": maximum_allowed_risk_percent,
        "tolerance": RISK_TOLERANCE_PERCENT,
        "difference": round(risk_difference, 4),
        "decision": risk_decision,
    })

    if risk_decision == "BLOCK":
        fallback = {
            "ok": False,
            "symbol": execution_symbol,
            "reason": (
                "LIVE BLOCKED: calculated volume below broker minimum and "
                "min volume exceeds allowed risk."
            ),
            "lot_size": round(float(lot_size), 4),
            "volume_units": int(min_volume_units),
            "risk_percent": configured_risk_percent,
            "final_risk_amount": round(final_risk_amount, 2),
            "final_risk_percent": round(final_risk_percent, 4),
            "maximum_allowed_risk_percent": maximum_allowed_risk_percent,
            "allowed_risk_percent": maximum_allowed_risk_percent,
            "risk_tolerance_percent": RISK_TOLERANCE_PERCENT,
            "minimum_volume_fallback": True,
            "risk_sizing_failure": failed_risk_size,
            "symbol_metadata": metadata,
        }
        print("LIVE_MINIMUM_VOLUME_FALLBACK_BLOCKED =", fallback)
        return fallback

    fallback = {
        "ok": True,
        "symbol": execution_symbol,
        "lot_size": round(float(lot_size), 4),
        "volume_units": int(min_volume_units),
        "risk_percent": configured_risk_percent,
        "risk_amount": round(account_value * (configured_risk_percent / 100), 2),
        "account_balance": round(account_balance_value, 2),
        "account_equity": account_equity,
        "account_equity_used": account_value,
        "sl_pips": round(sl_pips, 2) if sl_pips is not None else None,
        "pip_size": pip_size,
        "pip_value_per_lot": pip_value_per_lot,
        "lot_contract_size": lot_contract_size,
        "min_volume_units": int(min_volume_units),
        "max_volume_units": metadata.get("max_volume_units"),
        "volume_step_units": metadata.get("volume_step_units"),
        "final_risk_amount": round(final_risk_amount, 2),
        "final_risk_percent": round(final_risk_percent, 4),
        "maximum_allowed_risk_percent": maximum_allowed_risk_percent,
        "allowed_risk_percent": maximum_allowed_risk_percent,
        "risk_tolerance_percent": RISK_TOLERANCE_PERCENT,
        "symbol_metadata": metadata,
        "minimum_volume_fallback": True,
        "risk_sizing_failure": failed_risk_size,
    }
    print("LIVE_MINIMUM_VOLUME_FALLBACK =", fallback)
    return fallback

def normalize_broker_volume_to_lots(volume, symbol=None):
    try:
        numeric = float(volume)
    except (TypeError, ValueError):
        return None

    if numeric <= 0:
        return None

    if symbol and numeric >= 1:
        read_scale = get_ctrader_volume_read_scale(symbol)
        scaled_numeric = numeric / read_scale if read_scale else numeric
        lots = convert_ctrader_volume_to_lots(
            scaled_numeric,
            get_default_broker_lot_size(symbol)
        )
        return round(lots, 2) if lots is not None else None

    if numeric >= 1000:
        lots = convert_ctrader_volume_to_lots(numeric)
        return round(lots, 2) if lots is not None else None

    return round(numeric, 2)

def is_not_enough_money_result(result):
    text = json.dumps(result, default=str).upper()
    return "NOT_ENOUGH_MONEY" in text or "NOT ENOUGH FUNDS" in text

def calculate_retry_lot_size(current_lot, step):
    reduced = round_down_to_step(float(current_lot) / 2, step or MIN_LOT)

    if reduced < MIN_LOT:
        return None

    return round(reduced, 2)

def get_panel_trade_plan(panel_data, symbol):
    if not isinstance(panel_data, dict):
        return None

    execution_symbol = normalize_symbol(symbol)

    return panel_data.get(execution_symbol)

def get_paper_trade_for_live_symbol(panel_data, symbol):
    if not isinstance(panel_data, dict):
        return None

    execution_symbol = normalize_symbol(symbol)
    paper_trades = panel_data.get("paper_trades") or {}

    return paper_trades.get(execution_symbol)

def paper_would_enter_from_plan(panel_data, symbol, plan):
    if not isinstance(plan, dict):
        return False

    execution_symbol = normalize_symbol(symbol)
    signal = str(plan.get("signal") or "WAIT").upper()

    if signal not in ["BUY", "SELL"]:
        return False

    entry = plan.get("entry_price")
    sl = plan.get("stop_loss")
    tp1 = plan.get("tp1")
    tp2 = plan.get("tp2")

    if any(is_missing_trade_value(value) for value in [entry, sl, tp1, tp2]):
        return False

    try:
        normalized = normalize_trade_levels(
            execution_symbol,
            signal,
            entry,
            sl,
            tp1,
            tp2
        )
    except Exception:
        return False

    return bool(normalized.get("ok"))

def trade_payload_has_required_levels(trade_payload):
    if not isinstance(trade_payload, dict):
        return False, "Trade payload missing"

    side = str(
        trade_payload.get("action")
        or trade_payload.get("side")
        or trade_payload.get("signal")
        or ""
    ).upper()

    required_values = [
        trade_payload.get("entry"),
        trade_payload.get("sl"),
        trade_payload.get("tp1"),
        trade_payload.get("tp2"),
    ]

    if any(is_missing_trade_value(value) for value in required_values):
        return False, "Entry, SL, TP1, and TP2 are required before execution"

    normalized = normalize_trade_levels(
        trade_payload.get("symbol"),
        side,
        trade_payload.get("entry"),
        trade_payload.get("sl"),
        trade_payload.get("tp1"),
        trade_payload.get("tp2"),
    )

    if not normalized.get("ok"):
        return False, "LIVE BLOCKED: invalid SL/TP distance."

    return True, None

def log_paper_live_signal_compare(
    symbol,
    plan,
    panel_data,
    live_signal=None,
    live_blocked_by=None,
    live_blocked_reason=None,
    live_would_enter=None
):
    if not isinstance(plan, dict):
        plan = {}

    paper_signal = str(plan.get("signal") or "WAIT").upper()
    live_signal = str(live_signal or paper_signal or "WAIT").upper()
    paper_would_enter = paper_would_enter_from_plan(panel_data, symbol, plan)
    if live_would_enter is None:
        live_would_enter = (
            live_signal in ["BUY", "SELL"]
            and LIVE_AUTO_TRADE_ENABLED.get("enabled")
            and not LIVE_ACTIVE_ORDERS.get(normalize_symbol(symbol))
        )

    debug = {
        "symbol": normalize_symbol(symbol),
        "paper_signal": paper_signal,
        "live_signal": live_signal,
        "same_strategy_signal": paper_signal == live_signal,
        "paper_would_enter": paper_would_enter,
        "live_would_enter": live_would_enter,
        "live_blocked_by": live_blocked_by,
        "live_blocked_reason": live_blocked_reason,
    }

    print("PAPER_LIVE_SIGNAL_COMPARE_DEBUG =", debug)
    return debug

def log_live_xauusd_execution_debug(
    symbol,
    plan=None,
    trade_payload=None,
    risk_size=None,
    stage=None,
    blocked_by=None,
    blocked_reason=None,
    cooldown_active=None,
    existing_position=None,
    payload_valid=None,
    order_sent=False,
    order_accepted=False,
    result=None,
):
    if normalize_symbol(symbol) != "XAUUSD":
        return

    plan = plan if isinstance(plan, dict) else {}
    trade_payload = trade_payload if isinstance(trade_payload, dict) else {}
    risk_size = risk_size if isinstance(risk_size, dict) else {}
    result = result if isinstance(result, dict) else {}

    active_order = LIVE_ACTIVE_ORDERS.get("XAUUSD")

    if existing_position is None:
        existing_position = active_order

    if cooldown_active is None:
        cooldown_active = (
            time.time() - LIVE_LAST_EXECUTION_TIME.get("XAUUSD", 0)
            < LIVE_EXECUTION_COOLDOWN_SECONDS
        )

    if payload_valid is None:
        payload_valid = bool(trade_payload.get("ok")) if trade_payload else None

    strategy_signal = str(plan.get("signal") or trade_payload.get("signal") or "WAIT").upper()
    final_signal = str(trade_payload.get("signal") or plan.get("signal") or "WAIT").upper()
    metadata = risk_size.get("symbol_metadata") if isinstance(risk_size.get("symbol_metadata"), dict) else {}

    debug = {
        "stage": stage,
        "symbol": "XAUUSD",
        "strategy_signal": strategy_signal,
        "final_signal": final_signal,
        "confidence": plan.get("confidence"),
        "blocked_by": blocked_by or plan.get("blocked_by"),
        "blocked_reason": blocked_reason or plan.get("blocked_reason") or result.get("reason") or result.get("message"),
        "entry": trade_payload.get("entry") or plan.get("entry_price"),
        "sl": trade_payload.get("sl") or plan.get("stop_loss"),
        "tp1": trade_payload.get("tp1") or plan.get("tp1"),
        "risk_percent": risk_size.get("risk_percent") or trade_payload.get("risk_percent") or LIVE_RISK_PERCENT,
        "stop_loss_pips": risk_size.get("sl_pips") or trade_payload.get("sl_pips"),
        "lot_size": risk_size.get("lot_size") or trade_payload.get("lot_size") or trade_payload.get("volume"),
        "volume_units": risk_size.get("volume_units") or trade_payload.get("volume_units"),
        "broker_min_volume": risk_size.get("min_volume_units") or metadata.get("min_volume_units"),
        "broker_max_volume": risk_size.get("max_volume_units") or metadata.get("max_volume_units"),
        "volume_step": risk_size.get("volume_step_units") or metadata.get("volume_step_units"),
        "cooldown_active": bool(cooldown_active),
        "existing_position": existing_position,
        "payload_valid": payload_valid,
        "order_sent": bool(order_sent),
        "order_accepted": bool(order_accepted),
    }

    print("LIVE_XAUUSD_EXECUTION_DEBUG =", debug)
    return debug

def run_ctrader_auto_trade_checks(panel_data):
    if not isinstance(panel_data, dict):
        return []

    sync_ctrader_account_state()
    broker_connected = bool(
        LIVE_ACCOUNT_STATE.get("connected")
        and LIVE_ACCOUNT_STATE.get("execution_ready")
    )

    if not LIVE_AUTO_TRADE_ENABLED["enabled"]:
        log_auto_trade_blocked_reason(
            stage="live_auto_disabled",
            reason="Live Auto is off"
        )
        return []

    results = []
    actionable_seen = False

    for symbol in ["EURUSD", "XAUUSD"]:
        plan = get_panel_trade_plan(panel_data, symbol) or {}
        signal = str(plan.get("signal") or "WAIT").upper()
        broker_block_reason = (
            None
            if broker_connected
            else "Live Auto paused — broker disconnected"
        )
        print(f"AUTO TRADE SYMBOL CHECK: {symbol} signal={signal}")
        print("LIVE_AUTO_SIGNAL_DECISION =", {
            "symbol": symbol,
            "signal": signal,
            "strategy_setup_complete": bool(plan.get("strategy_setup_complete")),
            "strategy_setup_type": plan.get("strategy_setup_type"),
            "plan_type": plan.get("plan_type"),
            "entry_timing": plan.get("entry_timing"),
            "blocked_by": plan.get("blocked_by"),
            "blocked_reason": plan.get("blocked_reason"),
            "blocker_rule_name": plan.get("blocker_rule_name"),
            "auto_enabled": LIVE_AUTO_TRADE_ENABLED.get("enabled"),
            "broker_connected": broker_connected,
        })
        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            stage="strategy_signal_seen",
            blocked_by=plan.get("blocked_by"),
            blocked_reason=plan.get("blocked_reason"),
            payload_valid=None,
            order_sent=False,
            order_accepted=False,
        )

        if signal not in ["BUY", "SELL"]:
            print("CTRADER AUTO TRADE SKIPPED: WAIT", symbol)
            signal_before_filters = str(
                plan.get("signal_before_filters")
                or ""
            ).upper()
            blocked_by = str(plan.get("blocked_by") or "").lower()
            blocker_rule_name = str(plan.get("blocker_rule_name") or "").lower()
            held_late_block = (
                signal_before_filters in ["BUY", "SELL"]
                and (
                    blocked_by == "late_entry"
                    or blocker_rule_name == "final_signal_hold_guard"
                )
            )

            display_signal = (
                signal_before_filters
                if held_late_block
                else plan.get("signal") or signal
            )
            wait_reason = (
                plan.get("blocked_reason")
                or plan.get("plan_reason")
                or plan.get("wait_reason")
                or plan.get("entry_timing")
                or "Waiting for BUY/SELL signal"
            )
            set_auto_trade_status(
                symbol=symbol,
                signal=display_signal,
                action=None,
                status="BLOCKED" if held_late_block else "WAIT",
                reason=wait_reason
            )
            log_live_xauusd_execution_debug(
                symbol,
                plan=plan,
                stage="strategy_wait",
                blocked_by=plan.get("blocked_by"),
                blocked_reason=wait_reason if held_late_block else None,
                payload_valid=False,
                order_sent=False,
                order_accepted=False,
            )
            log_paper_live_signal_compare(
                symbol,
                plan,
                panel_data,
                live_signal=signal,
                live_blocked_by=plan.get("blocked_by"),
                live_blocked_reason=wait_reason if held_late_block else None
            )
            continue

        actionable_seen = True
        if symbol == "XAUUSD":
            print("AUTO TRADE XAUUSD ATTEMPT")

        if not broker_connected:
            set_auto_trade_status(
                symbol=symbol,
                signal=signal,
                action=signal,
                status="BLOCKED",
                reason=broker_block_reason
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=signal,
                stage="broker_connection",
                reason=broker_block_reason
            )
            log_paper_live_signal_compare(
                symbol,
                plan,
                panel_data,
                live_signal=signal,
                live_blocked_by="broker_connection",
                live_blocked_reason=broker_block_reason
            )
            log_live_xauusd_execution_debug(
                symbol,
                plan=plan,
                stage="broker_connection",
                blocked_by="broker_connection",
                blocked_reason=broker_block_reason,
                payload_valid=False,
                order_sent=False,
                order_accepted=False,
            )
            print("CTRADER AUTO TRADE PAUSED:", symbol, broker_block_reason)
            continue

        set_auto_trade_status(
            symbol=symbol,
            signal=signal,
            action=signal,
            status="READY",
            reason="Signal ready; running live safety checks"
        )

        log_paper_live_signal_compare(
            symbol,
            plan,
            panel_data,
            live_signal=signal,
            live_blocked_by=None,
            live_blocked_reason=None,
            live_would_enter=True
        )
        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            stage="ready_before_live_execute",
            blocked_by=None,
            blocked_reason=None,
            payload_valid=None,
            order_sent=False,
            order_accepted=False,
        )

        result = execute_live_order_core(
            {
                "symbol": symbol,
                "side": signal,
                "signal": signal,
                "entry": plan.get("entry_price"),
                "entry_price": plan.get("entry_price"),
                "sl": plan.get("stop_loss"),
                "stop_loss": plan.get("stop_loss"),
                "tp1": plan.get("tp1"),
                "tp2": plan.get("tp2"),
            },
            source="auto"
        )
        results.append(result)

        if not result.get("ok"):
            log_live_xauusd_execution_debug(
                symbol,
                plan=plan,
                stage="live_execute_returned_rejected",
                blocked_by=result.get("stage") or result.get("status") or "broker_or_risk",
                blocked_reason=(
                    result.get("reason")
                    or result.get("message")
                    or result.get("result", {}).get("reason")
                    or "unknown"
                ),
                payload_valid=False,
                order_sent=False,
                order_accepted=False,
                result=result,
            )
            log_paper_live_signal_compare(
                symbol,
                plan,
                panel_data,
                live_signal=signal,
                live_blocked_by=result.get("stage") or result.get("status") or "broker_or_risk",
                live_blocked_reason=(
                    result.get("reason")
                    or result.get("message")
                    or result.get("result", {}).get("reason")
                    or "unknown"
                ),
                live_would_enter=True
            )

        if result.get("ok"):
            log_live_xauusd_execution_debug(
                symbol,
                plan=plan,
                stage="live_execute_returned_accepted",
                blocked_by=None,
                blocked_reason=None,
                existing_position=result.get("active_order"),
                payload_valid=True,
                order_sent=True,
                order_accepted=True,
                result=result,
            )
            print("CTRADER AUTO TRADE SENT", symbol, signal)
        else:
            reason = (
                result.get("reason")
                or result.get("message")
                or result.get("result", {}).get("reason")
                or "unknown"
            )
            if symbol == "XAUUSD":
                print("AUTO TRADE XAUUSD BLOCKED:", reason)
            print(
                "CTRADER AUTO TRADE BLOCKED:",
                reason
            )

    return results

def evaluate_live_trade_exit(symbol, active_trade, current_plan):
    if not active_trade:
        return {
            "exit_status": None,
            "exit_reason": None
        }

    plan = current_plan if isinstance(current_plan, dict) else {}
    trade_side = str(
        active_trade.get("side")
        or active_trade.get("action")
        or ""
    ).upper()
    current_signal = str(plan.get("signal") or "WAIT").upper()
    market_condition = str(plan.get("market_condition") or "").upper()

    try:
        confidence = float(plan.get("confidence", 0))
    except (TypeError, ValueError):
        confidence = 0

    if trade_side == "BUY" and current_signal == "SELL":
        return {
            "exit_status": "EXIT_SUGGESTED",
            "exit_reason": "Signal flipped bearish"
        }

    if trade_side == "SELL" and current_signal == "BUY":
        return {
            "exit_status": "EXIT_SUGGESTED",
            "exit_reason": "Signal flipped bullish"
        }

    if current_signal == "WAIT":
        return {
            "exit_status": "WATCH",
            "exit_reason": "Signal moved to WAIT"
        }

    if confidence < 35:
        return {
            "exit_status": "WATCH",
            "exit_reason": "Confidence weakened"
        }

    if market_condition in ["CHOPPY", "UNKNOWN"]:
        return {
            "exit_status": "WATCH",
            "exit_reason": "Market condition unsafe"
        }

    return {
        "exit_status": "HOLD",
        "exit_reason": "Trade still aligned"
    }

def update_live_trade_exit_states(panel_data):
    for symbol, active_trade in list(LIVE_ACTIVE_ORDERS.items()):
        if not active_trade:
            continue

        current_plan = get_signal_trade_plan(symbol)

        if isinstance(panel_data, dict):
            current_plan = (
                panel_data.get(symbol)
                or current_plan
            )

        exit_state = evaluate_live_trade_exit(
            symbol,
            active_trade,
            current_plan
        )

        LIVE_ACTIVE_ORDERS[symbol] = {
            **active_trade,
            **exit_state
        }

def prepare_ctrader_trade(payload, volume=0.01):
    sync_ctrader_account_state()

    raw_symbol = str(payload.get("symbol", "")).upper()
    symbol = normalize_symbol(raw_symbol)
    action = str(payload.get("action") or payload.get("side") or "").upper()
    plan = get_signal_trade_plan(symbol) or {}
    # Auto passes the current actionable panel signal. Do not let an older
    # cached WAIT/late-entry plan value override it during execution.
    payload_signal = str(payload.get("signal") or "").upper()
    plan_signal = str(plan.get("signal") or "").upper()
    if payload_signal in ["BUY", "SELL"]:
        signal = payload_signal
    else:
        signal = str(plan_signal or payload_signal or "WAIT").upper()
    value_plan = {} if payload_signal in ["BUY", "SELL"] else plan

    entry, _ = choose_backend_trade_value(
        value_plan,
        payload,
        "entry_price",
        "entry",
        "entry_price"
    )
    sl, _ = choose_backend_trade_value(
        value_plan,
        payload,
        "stop_loss",
        "sl",
        "stop_loss"
    )
    tp1, _ = choose_backend_trade_value(value_plan, payload, "tp1", "tp1")
    tp2, _ = choose_backend_trade_value(value_plan, payload, "tp2", "tp2")

    try:
        decimals = 2 if symbol == "XAUUSD" else 5
        tp1 = round(calculate_tp1_from_tp2(entry, tp2, action), decimals)
    except (TypeError, ValueError):
        pass

    log_frontend_trade_level_mismatch(
        symbol,
        action,
        "entry",
        plan.get("entry_price"),
        payload.get("entry", payload.get("entry_price"))
    )
    log_frontend_trade_level_mismatch(
        symbol,
        action,
        "sl",
        plan.get("stop_loss"),
        payload.get("sl", payload.get("stop_loss"))
    )
    log_frontend_trade_level_mismatch(
        symbol,
        action,
        "tp1",
        plan.get("tp1"),
        payload.get("tp1")
    )
    log_frontend_trade_level_mismatch(
        symbol,
        action,
        "tp2",
        plan.get("tp2"),
        payload.get("tp2")
    )

    if not LIVE_ACCOUNT_STATE.get("connected"):
        return reject_ctrader_order(symbol, action, entry, sl, tp1, tp2, "No cTrader account connected")

    if symbol not in LIVE_ACTIVE_ORDERS:
        return reject_ctrader_order(symbol, action, entry, sl, tp1, tp2, "Unsupported cTrader symbol")

    if action not in ["BUY", "SELL"]:
        return reject_ctrader_order(symbol, action, entry, sl, tp1, tp2, "Action must be BUY or SELL")

    if signal not in ["BUY", "SELL"]:
        return reject_ctrader_order(symbol, action, entry, sl, tp1, tp2, "Signal is WAIT")

    if action != signal:
        return reject_ctrader_order(symbol, action, entry, sl, tp1, tp2, "Order action does not match signal")

    normalized = normalize_trade_levels(
        symbol,
        action,
        entry,
        sl,
        tp1,
        tp2
    )

    normalized["mode"] = LIVE_ACCOUNT_STATE.get("mode", "demo")
    normalized["signal"] = signal

    if not normalized.get("ok"):
        normalized["message"] = normalized.get("reason")
        log_rejected_ctrader_trade(
            symbol,
            action,
            entry,
            sl,
            tp1,
            tp2,
            normalized.get("reason")
        )
        return normalized

    normalized["volume"] = None

    return normalized

def is_dev_request(request: Request):
    host = request.client.host if request.client else ""
    forwarded = request.headers.get("x-forwarded-for", "")
    origin = request.headers.get("origin", "")
    referer = request.headers.get("referer", "")
    local_hosts = ["127.0.0.1", "localhost", "::1"]
    local_origins = [
        "http://127.0.0.1:5501",
        "http://localhost:5501",
    ]

    return (
        host in local_hosts
        or forwarded.split(",")[0].strip() in local_hosts
        or origin in local_origins
        or any(origin.startswith(f"http://{local}") for local in local_hosts)
        or any(referer.startswith(f"http://{local}") for local in local_hosts)
    )

def sync_live_positions():
    sync_ctrader_account_state()

    connected = bool(LIVE_ACCOUNT_STATE.get("connected"))
    print(
        "LIVE_BROKER_STATUS:",
        "connected" if connected else "disconnected"
    )

    if not connected:
        if LIVE_AUTO_TRADE_ENABLED["enabled"]:
            LIVE_AUTO_TRADE_ENABLED["enabled"] = False
            save_auto_trade_state()

        set_auto_trade_status(
            status="BLOCKED",
            reason="Live Auto paused — broker disconnected"
        )

        print("LIVE_POSITION_SYNC:", {
            "connected": False,
            "positions": [],
            "active_orders": get_persistable_live_active_orders()
        })

        return [
            trade
            for trade in LIVE_ACTIVE_ORDERS.values()
            if trade and get_live_trade_status(trade) in ["RUNNING", "OPEN", "TP1 HIT"]
        ]

    if not LIVE_ACCOUNT_STATE.get("connected"):
        return []

    try:
        positions = get_open_positions()
        print("LIVE_POSITION_SYNC:", {
            "connected": True,
            "positions": positions
        })
        position_fetch_error = get_ctrader_position_fetch_error()

        if position_fetch_error:
            print("LIVE_POSITION_SYNC_ERROR:", position_fetch_error)
            LIVE_POSITION_SYNC_STATUS["last_error"] = position_fetch_error
            return [
                trade
                for trade in LIVE_ACTIVE_ORDERS.values()
                if trade and get_live_trade_status(trade) in ["RUNNING", "OPEN", "TP1 HIT"]
            ]
        else:
            LIVE_POSITION_SYNC_STATUS["last_success"] = time.time()
            LIVE_POSITION_SYNC_STATUS["last_error"] = None

        previous_active_orders = {
            symbol: trade
            for symbol, trade in LIVE_ACTIVE_ORDERS.items()
            if trade
        }

        rebuilt_active_orders = {
            symbol: None
            for symbol in LIVE_ACTIVE_ORDERS
        }

        for position in positions:
            symbol = normalize_symbol(position.get("symbol"))

            if symbol not in LIVE_ACTIVE_ORDERS:
                continue

            broker_lot_size = normalize_broker_volume_to_lots(
                position.get("volume")
                or position.get("volumeInUnits")
                or (position.get("tradeData") or {}).get("volume"),
                symbol
            )
            current_price = (
                position.get("current_price")
                or position.get("currentPrice")
                or position.get("price")
                or position.get("bid")
                or position.get("ask")
            )
            raw_position_for_levels = (
                position.get("raw")
                if isinstance(position.get("raw"), dict)
                else {}
            )
            nested_raw_position = (
                raw_position_for_levels.get("raw")
                if isinstance(raw_position_for_levels.get("raw"), dict)
                else {}
            )
            broker_current_high = (
                position.get("current_high")
                or position.get("currentHigh")
                or position.get("high")
                or raw_position_for_levels.get("currentHigh")
                or raw_position_for_levels.get("high")
            )
            broker_current_low = (
                position.get("current_low")
                or position.get("currentLow")
                or position.get("low")
                or raw_position_for_levels.get("currentLow")
                or raw_position_for_levels.get("low")
            )
            position_id = (
                position.get("position_id")
                or position.get("positionId")
                or position.get("id")
                or f"broker-{symbol}"
            )
            side = normalize_live_trade_side(
                position.get("side")
                or position.get("tradeSide")
                or position.get("direction")
                or ""
            )
            entry = (
                position.get("entry")
                or position.get("entry_price")
                or position.get("entryPrice")
                or position.get("openPrice")
            )
            volume_units = (
                position.get("volume")
                or position.get("volumeInUnits")
                or (position.get("tradeData") or {}).get("volume")
            )
            pips = calculate_live_trade_pips(
                symbol,
                side,
                entry,
                current_price
            )
            broker_position_for_pl = {
                **position,
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "current_price": current_price,
                "lot_size": broker_lot_size,
                "volume_units": volume_units,
            }
            broker_pnl = get_live_floating_pl(broker_position_for_pl)
            broker_net_pl, broker_pnl_source = extract_broker_trade_pl(
                broker_position_for_pl
            )

            if broker_net_pl is None:
                broker_pnl_source = "fallback"
            live_prices_for_position = {}

            try:
                live_prices_for_position = (get_live_prices() or {}).get("live_prices", {})
            except Exception:
                live_prices_for_position = {}

            live_tick_for_position = live_prices_for_position.get(symbol) or {}
            live_bid_for_position = live_tick_for_position.get("bid")
            live_ask_for_position = live_tick_for_position.get("ask")
            used_current_price = current_price

            if side == "BUY" and live_bid_for_position is not None:
                used_current_price = live_bid_for_position
            elif side == "SELL" and live_ask_for_position is not None:
                used_current_price = live_ask_for_position

            current_order = previous_active_orders.get(symbol)
            signal_plan = get_signal_trade_plan(symbol) or {}
            broker_synced_sl = (
                position.get("sl")
                or position.get("stop_loss")
                or position.get("stopLoss")
                or raw_position_for_levels.get("sl")
                or raw_position_for_levels.get("stopLoss")
                or nested_raw_position.get("stopLoss")
            )
            planned_sl = (
                (current_order or {}).get("planned_sl")
                or (current_order or {}).get("original_sl")
                or signal_plan.get("stop_loss")
            )
            planned_tp1 = (
                (current_order or {}).get("planned_tp1")
                or (current_order or {}).get("tp1")
                or signal_plan.get("tp1")
            )
            planned_tp2 = (
                (current_order or {}).get("planned_tp2")
                or (current_order or {}).get("tp2")
                or signal_plan.get("tp2")
            )
            broker_synced_tp2 = (
                position.get("take_profit")
                or position.get("takeProfit")
                or raw_position_for_levels.get("takeProfit")
                or nested_raw_position.get("takeProfit")
                or position.get("tp2")
                or raw_position_for_levels.get("tp2")
                or nested_raw_position.get("tp2")
            )
            synced_sl = broker_synced_sl
            synced_tp2 = broker_synced_tp2
            synced_tp1 = (
                position.get("tp1")
                or raw_position_for_levels.get("tp1")
            )
            refresh_overwrite_blocked = False
            modification_age = None
            user_modified_levels = (
                (current_order or {}).get("user_modified_levels")
                if isinstance((current_order or {}).get("user_modified_levels"), dict)
                else {}
            )
            levels_modified_at = (current_order or {}).get("levels_modified_at")

            try:
                modification_age = time.time() - float(levels_modified_at)
            except (TypeError, ValueError):
                modification_age = None

            def prices_match(left, right):
                try:
                    tolerance = 0.005 if symbol == "XAUUSD" else 0.000005
                    return abs(float(left) - float(right)) <= tolerance
                except (TypeError, ValueError):
                    return False

            def first_valid_price(*values):
                for value in values:
                    try:
                        numeric_value = float(value)
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(numeric_value) and numeric_value > 0:
                        return numeric_value
                return None

            saved_sl = first_valid_price(
                user_modified_levels.get("sl"),
                (current_order or {}).get("sl"),
                (current_order or {}).get("current_sl"),
                (current_order or {}).get("planned_sl"),
            )
            saved_tp1 = first_valid_price(
                user_modified_levels.get("tp1"),
                (current_order or {}).get("tp1"),
                (current_order or {}).get("take_profit_1"),
                (current_order or {}).get("planned_tp1"),
            )
            saved_tp2 = first_valid_price(
                user_modified_levels.get("tp2"),
                (current_order or {}).get("tp2"),
                (current_order or {}).get("take_profit_2"),
                (current_order or {}).get("take_profit"),
                (current_order or {}).get("planned_tp2"),
            )
            broker_grace_active = modification_age is not None and modification_age <= 120

            if saved_sl is not None and (
                synced_sl is None
                or (broker_grace_active and not prices_match(synced_sl, saved_sl))
            ):
                synced_sl = saved_sl
                refresh_overwrite_blocked = True

            if saved_tp2 is not None and (
                synced_tp2 is None
                or (broker_grace_active and not prices_match(synced_tp2, saved_tp2))
            ):
                synced_tp2 = saved_tp2
                refresh_overwrite_blocked = True

            if saved_tp1 is not None:
                if synced_tp1 is None or not prices_match(synced_tp1, saved_tp1):
                    refresh_overwrite_blocked = True
                synced_tp1 = saved_tp1
            else:
                try:
                    if entry is not None and synced_tp2 is not None:
                        synced_tp1 = round(
                            calculate_tp1_from_tp2(entry, synced_tp2, side),
                            2 if symbol == "XAUUSD" else 5,
                        )
                except (TypeError, ValueError):
                    pass

            if synced_tp1 is None or synced_tp2 is None:
                print("TRADE_LEVEL_WARNING =", {
                    "symbol": symbol,
                    "position_id": position_id,
                    "message": "Missing broker TP; keeping last known valid trade level",
                    "saved_tp1": saved_tp1,
                    "saved_tp2": saved_tp2,
                })

            print("refreshOverwriteBlocked", refresh_overwrite_blocked, {
                "symbol": symbol,
                "position_id": position_id,
                "modification_age": modification_age,
                "broker_sl": broker_synced_sl,
                "saved_sl": saved_sl,
                "broker_tp2": broker_synced_tp2,
                "saved_tp2": saved_tp2,
                "saved_tp1": saved_tp1,
            })
            observed_prices = [
                value
                for value in [
                    used_current_price,
                    broker_current_high,
                    broker_current_low,
                    current_order.get("current_high") if current_order else None,
                    current_order.get("current_low") if current_order else None,
                ]
                if value is not None
            ]
            numeric_observed_prices = []

            for value in observed_prices:
                try:
                    numeric_observed_prices.append(float(value))
                except (TypeError, ValueError):
                    continue

            current_high = (
                max(numeric_observed_prices)
                if numeric_observed_prices
                else used_current_price
            )
            current_low = (
                min(numeric_observed_prices)
                if numeric_observed_prices
                else used_current_price
            )

            if symbol == "EURUSD":
                detected_pl_fields = {
                    key: value
                    for key, value in position.items()
                    if any(
                        marker in str(key).lower()
                        for marker in [
                            "pnl",
                            "profit",
                            "pl",
                            "unrealized",
                            "gross",
                            "net",
                            "money",
                            "swap",
                            "commission",
                        ]
                    )
                }
                raw_position = position.get("raw")

                if isinstance(raw_position, dict):
                    detected_pl_fields["raw"] = {
                        key: value
                        for key, value in raw_position.items()
                        if any(
                            marker in str(key).lower()
                            for marker in [
                                "pnl",
                                "profit",
                                "pl",
                                "unrealized",
                                "gross",
                                "net",
                                "money",
                                "swap",
                                "commission",
                            ]
                        )
                    }

                print("RAW_OPEN_POSITION_PL_DEBUG =", {
                    "full_position_object": position,
                    "all_keys": list(position.keys()),
                    "detected_pl_fields": detected_pl_fields,
                    "entry_price": entry,
                    "current_price": current_price,
                    "side": side,
                    "volume": volume_units,
                    "lot_size": broker_lot_size,
                    "calculated_fallback_pl": calculate_live_floating_pl_from_prices({
                        **position,
                        "symbol": symbol,
                        "side": side,
                        "entry": entry,
                        "current_price": current_price,
                        "lot_size": broker_lot_size,
                        "volume_units": volume_units,
                    }),
                    "final_reader_pl": broker_pnl,
                })

            mirrored_order = {
                "order_id": f"broker-{position_id}",
                "trade_id": f"ctrader-pos-{position_id}",
                "position_id": position_id,
                "broker_position_id": position_id,
                "symbol": symbol,
                "side": side,
                "mode": LIVE_ACCOUNT_STATE["mode"],
                "broker": LIVE_ACCOUNT_STATE["broker"],
                "volume": broker_lot_size,
                "lot_size": broker_lot_size,
                "volume_units": volume_units,
                "entry": entry,
                "sl": synced_sl,
                "current_sl": synced_sl,
                "original_sl": (
                    (current_order or {}).get("original_sl")
                    or synced_sl
                    or planned_sl
                ),
                "planned_sl": planned_sl,
                "broker_stop_loss_confirmed": synced_sl is not None,
                "broker_stop_loss_missing": synced_sl is None,
                "tp1": synced_tp1,
                "tp2": synced_tp2,
                "planned_tp1": planned_tp1,
                "planned_tp2": planned_tp2,
                "broker_take_profit_confirmed": synced_tp2 is not None,
                "broker_take_profit_missing": synced_tp2 is None,
                "current_price": used_current_price,
                "current_high": current_high,
                "current_low": current_low,
                "position_current_price": current_price,
                "floating_pl": broker_pnl,
                "floating_pnl": broker_pnl,
                "pnl": broker_pnl,
                "profit": broker_pnl,
                "broker_pnl": broker_pnl,
                "broker_pnl_source": broker_pnl_source,
                "pips": pips,
                "opened_at": position.get("opened_at") or time.time(),
                "source": "broker",
                "result": "RUNNING",
                "signal_setup_id": (
                    (current_order or {}).get("signal_setup_id")
                    or get_signal_setup_id(signal_plan, side)
                ),
                "levels_modified_at": levels_modified_at,
                "user_modified_levels": user_modified_levels,
                "raw": position.get("raw", position),
            }
            ensure_live_trade_identity(mirrored_order, symbol)

            if current_order and broker_position_matches_trade(position, current_order):
                rebuilt_active_orders[symbol] = update_live_trade_tp_protection({
                    **current_order,
                    **mirrored_order,
                    "trade_id": get_live_trade_identity(current_order) or get_live_trade_identity(mirrored_order),
                    "opened_at": current_order.get("opened_at") or mirrored_order["opened_at"],
                    "result": (
                        current_order.get("result")
                        if current_order.get("hit_tp1")
                        else mirrored_order.get("result")
                    ),
                })
                ensure_live_trade_identity(rebuilt_active_orders[symbol], symbol)
                log_live_trade_audit("broker_position_synced", rebuilt_active_orders[symbol])
                log_trade_visual_levels(rebuilt_active_orders[symbol])
                continue

            if not current_order:
                rebuilt_active_orders[symbol] = update_live_trade_tp_protection(mirrored_order)
                ensure_live_trade_identity(rebuilt_active_orders[symbol], symbol)
                log_live_trade_audit("broker_position_mirrored", rebuilt_active_orders[symbol])
                log_trade_visual_levels(rebuilt_active_orders[symbol])
                print("BROKER POSITION MIRRORED:", mirrored_order)

            else:
                rebuilt_active_orders[symbol] = mirrored_order
                ensure_live_trade_identity(rebuilt_active_orders[symbol], symbol)
                log_live_trade_audit("broker_position_replaced", rebuilt_active_orders[symbol])
                log_trade_visual_levels(rebuilt_active_orders[symbol])

        closed_at = time.time()
        removed = []

        for symbol, trade in previous_active_orders.items():
            if not trade:
                continue

            if find_matching_broker_position(trade, positions):
                continue

            old_status = get_live_trade_status(trade) or "RUNNING"
            position_id = (
                trade.get("position_id")
                or trade.get("broker_position_id")
                or trade.get("broker_order_id")
                or trade.get("order_id")
            )
            closed_trade = build_broker_closed_trade(trade, closed_at)

            move_live_trade_to_history_once(closed_trade)
            log_live_trade_audit("broker_position_closed_sync", closed_trade, reason="no broker position")
            mark_signal_history_broker_closed(symbol)
            removed.append({
                "symbol": symbol,
                "order_id": trade.get("order_id"),
                "position_id": position_id,
                "result": "BROKER_CLOSED",
                "reason": "no broker position"
            })

            print("LIVE_BROKER_POSITION_CLOSED_SYNC =", {
                "symbol": symbol,
                "old_status": old_status,
                "new_status": "BROKER_CLOSED",
                "position_id": position_id,
            })
            print("LIVE_TRADE_SYNC_CLOSED_EXTERNALLY", {
                "symbol": symbol,
                "position_id": position_id,
                "closed_at": closed_at,
            })

            print(
                "CTRADER POSITION CLOSED ON BROKER:",
                symbol,
                position_id,
                "BROKER_CLOSED",
                closed_trade.get("pnl")
            )

        LIVE_ACTIVE_ORDERS.update(rebuilt_active_orders)
        active_ids = {
            str(get_live_trade_match_key(trade))
            for trade in LIVE_ACTIVE_ORDERS.values()
            if trade and get_live_trade_match_key(trade)
        }
        cleaned_history = []

        for item in LIVE_TRADE_HISTORY:
            if str(get_live_trade_match_key(item)) in active_ids:
                log_live_trade_audit(
                    "history_removed_because_active",
                    item,
                    reason="same broker position is currently open"
                )
                continue

            result = str(
                item.get("result")
                or item.get("status")
                or ""
            ).upper()

            if result in ["RUNNING", "OPEN"] and not find_matching_broker_position(item, positions):
                old_status = get_live_trade_status(item) or result
                position_id = (
                    item.get("position_id")
                    or item.get("broker_position_id")
                    or item.get("broker_order_id")
                    or item.get("order_id")
                )
                closed_item = build_broker_closed_trade(item, closed_at)
                mark_signal_history_broker_closed(item.get("symbol"))
                log_live_trade_audit(
                    "running_history_converted_closed",
                    closed_item,
                    reason="unconfirmed running history converted to broker closed"
                )
                removed.append({
                    "symbol": item.get("symbol"),
                    "order_id": item.get("order_id"),
                    "position_id": position_id,
                    "result": "BROKER_CLOSED",
                    "reason": "unconfirmed running history converted to broker closed"
                })

                print("LIVE_BROKER_POSITION_CLOSED_SYNC =", {
                    "symbol": normalize_symbol(item.get("symbol")),
                    "old_status": old_status,
                    "new_status": "BROKER_CLOSED",
                    "position_id": position_id,
                })

                cleaned_history.append(closed_item)
                continue

            cleaned_history.append(item)

        LIVE_TRADE_HISTORY[:] = cleaned_history
        del LIVE_TRADE_HISTORY[MAX_LIVE_TRADE_HISTORY:]
        save_live_backup()

        if removed:
            print("LIVE_STALE_RUNNING_REMOVED:", removed)

        return positions
    except Exception as e:
        print("LIVE_POSITION_SYNC_ERROR:", e)
        LIVE_POSITION_SYNC_STATUS["last_error"] = str(e)
        return []

@app.get("/ctrader/connect")
def ctrader_connect():
    return build_ctrader_authorization_url()


@app.get("/ctrader/oauth-debug")
def ctrader_oauth_debug():
    result = build_ctrader_authorization_url()
    redirect_debug = result.get("redirect_uri_debug") or get_ctrader_redirect_uri_debug()
    return {
        "ok": result.get("ok", False),
        "client_id": os.getenv("CTRADER_CLIENT_ID"),
        "redirect_uri": result.get("redirect_uri"),
        "redirect_uri_env_value": redirect_debug.get("env_value"),
        "redirect_uri_file_value": redirect_debug.get("file_value"),
        "redirect_uri_fallback_value": redirect_debug.get("fallback_value"),
        "redirect_uri_final_value": redirect_debug.get("final_value"),
        "redirect_uri_source": redirect_debug.get("source"),
        "auth_url": result.get("authorization_url"),
        "reason": result.get("reason"),
    }


@app.get("/ctrader/callback-debug")
def ctrader_callback_debug(request: Request):
    redirect_debug = get_ctrader_redirect_uri_debug()
    query = dict(request.query_params)
    error = request.query_params.get("error")
    error_description = request.query_params.get("error_description")
    code_present = bool(request.query_params.get("code"))
    frontend_url = (
        os.getenv("FLOW_SIGNAL_FRONTEND_URL")
        or os.getenv("FRONTEND_URL")
        or "http://127.0.0.1:5501"
    ).rstrip("/")
    reason = None

    if error:
        reason = error_description or error
    elif not code_present:
        reason = "Missing cTrader authorization code"

    return {
        "ok": reason is None,
        "would_redirect_with_ctrader_error": reason is not None,
        "reason": reason,
        "query": query,
        "code_present": code_present,
        "frontend_url": frontend_url,
        "success_redirect_url": f"{frontend_url}/?brokerAccounts=1&ctrader=connected",
        "error_redirect_url": f"{frontend_url}/?brokerAccounts=1&ctrader=error",
        "redirect_uri_env_value": redirect_debug.get("env_value"),
        "redirect_uri_file_value": redirect_debug.get("file_value"),
        "redirect_uri_fallback_value": redirect_debug.get("fallback_value"),
        "redirect_uri_final_value": redirect_debug.get("final_value"),
        "redirect_uri_source": redirect_debug.get("source"),
    }

def get_frontend_redirect_url(status):
    frontend_url = (
        os.getenv("FLOW_SIGNAL_FRONTEND_URL")
        or os.getenv("FRONTEND_URL")
        or "http://127.0.0.1:5501"
    ).rstrip("/")
    redirect_url = f"{frontend_url}/?brokerAccounts=1&ctrader={status}"

    print("CTRADER_CALLBACK_FRONTEND_REDIRECT =", {
        "frontend_url": frontend_url,
        "status": status,
        "redirect_url": redirect_url,
    })

    return redirect_url


@app.get("/ctrader/callback")
def ctrader_oauth_callback(request: Request):
    code = request.query_params.get("code")
    error = request.query_params.get("error")
    error_description = request.query_params.get("error_description")

    if error:
        reason = error_description or error
        redirect_url = get_frontend_redirect_url("error")
        print("CTRADER_CALLBACK_ERROR_DEBUG =", {
            "reason": reason,
            "query": dict(request.query_params),
            "redirect_uri": get_ctrader_redirect_uri_debug(),
            "redirect_url": redirect_url,
        })
        return HTMLResponse(
            f"""
            <!doctype html>
            <html><body>
              <script>
                localStorage.setItem("flowsignalCtraderOAuth", JSON.stringify({{"ok": false, "reason": {json.dumps(reason)}}}));
                window.location.href = {json.dumps(redirect_url)};
              </script>
              cTrader authorization failed. Returning to FlowSignal...
            </body></html>
            """
        )

    if not code:
        reason = "Missing cTrader authorization code"
        redirect_url = get_frontend_redirect_url("error")
        print("CTRADER_CALLBACK_ERROR_DEBUG =", {
            "reason": reason,
            "query": dict(request.query_params),
            "redirect_uri": get_ctrader_redirect_uri_debug(),
            "redirect_url": redirect_url,
        })
        return HTMLResponse(
            f"""
            <!doctype html>
            <html><body>
              <script>
                localStorage.setItem("flowsignalCtraderOAuth", JSON.stringify({{"ok": false, "reason": {json.dumps(reason)}}}));
                window.location.href = {json.dumps(redirect_url)};
              </script>
              Missing authorization code. Returning to FlowSignal...
            </body></html>
            """
        )

    token_result = exchange_ctrader_authorization_code(code)

    if token_result.get("ok"):
        accounts_result = fetch_ctrader_accounts(refresh=True)
        sync_ctrader_account_state(force=True)
        oauth_result = {
            "ok": accounts_result.get("ok", True),
            "reason": accounts_result.get("reason"),
        }
    else:
        oauth_result = token_result
        print("CTRADER_CALLBACK_ERROR_DEBUG =", {
            "reason": token_result.get("reason"),
            "redirect_uri": get_ctrader_redirect_uri_debug(),
        })

    status = "connected" if oauth_result.get("ok") else "error"
    redirect_url = get_frontend_redirect_url(status)

    return HTMLResponse(
        f"""
        <!doctype html>
        <html><body>
          <script>
            localStorage.setItem("flowsignalCtraderOAuth", JSON.stringify({json.dumps(oauth_result)}));
            window.location.href = {json.dumps(redirect_url)};
          </script>
          cTrader authorization finished. Returning to FlowSignal...
        </body></html>
        """
    )


@app.post("/ctrader/disconnect")
def ctrader_disconnect_endpoint():
    return disconnect_ctrader()


@app.get("/ctrader/accounts/refresh")
def ctrader_accounts_refresh_endpoint():
    result = fetch_ctrader_accounts(refresh=True)
    sync_ctrader_account_state(force=True)
    return result


@app.get("/ctrader/accounts")
def ctrader_accounts_endpoint():
    return fetch_ctrader_accounts(refresh=False)


@app.post("/ctrader/accounts/active")
def ctrader_accounts_active_endpoint(payload: dict):
    result = set_active_ctrader_account(
        payload.get("accountId")
        or payload.get("account_id")
    )
    sync_ctrader_account_state(force=True)
    return {
        **result,
        "live_account": LIVE_ACCOUNT_STATE,
    }


@app.post("/ctrader/accounts/forget")
def ctrader_accounts_forget_endpoint(payload: dict):
    result = forget_ctrader_account(
        payload.get("accountId")
        or payload.get("account_id")
    )
    sync_ctrader_account_state(force=True)
    return {
        **result,
        "live_account": LIVE_ACCOUNT_STATE,
    }


@app.post("/ctrader/accounts/clear")
def ctrader_accounts_clear_endpoint():
    result = clear_ctrader_saved_accounts()
    sync_ctrader_account_state(force=True)
    return {
        **result,
        "live_account": LIVE_ACCOUNT_STATE,
    }

@app.post("/debug/set-broker-positions")
async def debug_set_broker_positions(
    payload: DebugBrokerPositionsRequest,
    request: Request
):
    if not is_dev_request(request):
        print(
            "DEBUG BROKER POSITIONS BLOCKED:",
            {
                "client_host": request.client.host if request.client else "",
                "origin": request.headers.get("origin", ""),
            }
        )

        return {
            "ok": False,
            "message": "Debug endpoint is only available locally"
        }

    received_payload = payload.model_dump()
    requested_positions = received_payload.get("positions")

    print("DEBUG BROKER POSITIONS PAYLOAD:", received_payload)

    if not isinstance(requested_positions, list):
        print("DEBUG BROKER POSITIONS VALIDATION ERROR:", received_payload)

        return {
            "ok": False,
            "message": "Expected payload shape: { positions: [] }"
        }

    positions = set_debug_open_positions(requested_positions)

    print("DEBUG BROKER POSITIONS:", positions)

    return {
        "ok": True,
        "positions": positions
    }

@app.post("/connect-ctrader")
def connect_ctrader(payload: dict):
    from ctrader_connector import get_ctrader_config

    config = get_ctrader_config()
    mode = str(payload.get("mode") or (config.get("env") if config else "demo")).lower()
    account_id = payload.get("account_id")

    if mode not in ["demo", "live"]:
        return {
            "ok": False,
            "message": "Invalid mode"
        }

    connector_result = connect_account(account_id, mode)
    accounts_result = fetch_ctrader_accounts(refresh=True)
    sync_ctrader_account_state()

    print("CTRADER CONNECTED:", LIVE_ACCOUNT_STATE)

    return {
        "ok": connector_result.get("ok", True),
        "reason": connector_result.get("reason") or accounts_result.get("reason"),
        "connected": LIVE_ACCOUNT_STATE["connected"],
        "mode": LIVE_ACCOUNT_STATE["mode"],
        "broker": LIVE_ACCOUNT_STATE["broker"],
        "account_id": LIVE_ACCOUNT_STATE["account_id"],
        "execution_ready": LIVE_ACCOUNT_STATE["execution_ready"],
        "accounts": accounts_result.get("accounts", []),
        "active_account_id": accounts_result.get("active_account_id"),
    }

@app.get("/ctrader-accounts")
def ctrader_accounts():
    return fetch_ctrader_accounts(refresh=False)


@app.post("/refresh-ctrader-accounts")
def refresh_ctrader_accounts():
    result = fetch_ctrader_accounts(refresh=True)
    sync_ctrader_account_state(force=True)
    return result


@app.post("/set-active-ctrader-account")
def set_active_ctrader_account_endpoint(payload: dict):
    result = set_active_ctrader_account(payload.get("account_id"))
    sync_ctrader_account_state(force=True)
    return {
        **result,
        "live_account": LIVE_ACCOUNT_STATE,
    }


@app.post("/forget-ctrader-account")
def forget_ctrader_account_endpoint(payload: dict):
    result = forget_ctrader_account(payload.get("account_id"))
    sync_ctrader_account_state(force=True)
    return {
        **result,
        "live_account": LIVE_ACCOUNT_STATE,
    }


@app.post("/disconnect-ctrader")
def disconnect_ctrader():
    LIVE_AUTO_TRADE_ENABLED["enabled"] = False
    save_auto_trade_state()

    disconnected_at = time.time()

    for symbol, trade in list(LIVE_ACTIVE_ORDERS.items()):
        if not trade:
            continue

        order_id = trade.get("order_id")
        history_trade = None

        for item in LIVE_TRADE_HISTORY:
            if order_id and item.get("order_id") == order_id:
                history_trade = item
                break

        disconnected_trade = {
            **trade,
            "symbol": symbol,
            "result": "DISCONNECTED",
            "closed_at": disconnected_at,
            "note": "FlowSignal tracking stopped; broker positions were not auto-closed."
        }

        move_live_trade_to_history_once(disconnected_trade)

    del LIVE_TRADE_HISTORY[MAX_LIVE_TRADE_HISTORY:]

    disconnect_account()
    sync_ctrader_account_state()
    LIVE_ACTIVE_ORDERS["EURUSD"] = None
    LIVE_ACTIVE_ORDERS["XAUUSD"] = None
    save_live_backup()

    print("CTRADER DISCONNECTED")

    return {
        "ok": True,
        "connected": False,
        "mode": "demo",
        "broker": "ctrader",
        "account_id": None,
        "execution_ready": False,
        "live_auto_enabled": False
    }

@app.post("/close-live-trade")
def close_live_trade(payload: dict):
    symbol = normalize_symbol(payload.get("symbol"))

    if symbol not in LIVE_ACTIVE_ORDERS or not LIVE_ACTIVE_ORDERS.get(symbol):
        return {
            "ok": False,
            "symbol": symbol,
            "reason": "No active live trade for symbol",
            "message": "No active live trade for symbol"
        }

    active_trade = LIVE_ACTIVE_ORDERS[symbol]
    closed_at = time.time()
    warning = None

    if active_trade.get("source") == "broker":
        remove_debug_open_position(symbol)
        warning = "Broker close is simulated only"

    closed_trade = {
        **active_trade,
        "symbol": symbol,
        "trade_id": get_live_trade_identity(active_trade),
        "status": "MANUAL_CLOSE",
        "result": "MANUAL_CLOSE",
        "closed_at": closed_at,
    }
    ensure_live_trade_identity(closed_trade, symbol)

    if warning:
        closed_trade["warning"] = warning

    move_live_trade_to_history_once(closed_trade)
    log_live_trade_audit("manual_close", closed_trade, reason=warning)
    LIVE_ACTIVE_ORDERS[symbol] = None
    save_live_backup()

    print("LIVE TRADE MANUAL CLOSE:", closed_trade)

    response = {
        "ok": True,
        "symbol": symbol,
        "closed_trade": closed_trade
    }

    if warning:
        response["warning"] = warning

    return response


@app.post("/modify-live-position-levels")
def modify_live_position_levels(payload: dict):
    symbol = normalize_symbol(payload.get("symbol"))
    trade = LIVE_ACTIVE_ORDERS.get(symbol)

    if not trade:
        return {"ok": False, "reason": "No active live trade for symbol"}

    position_id = trade.get("position_id") or trade.get("broker_position_id")
    requested_position_id = payload.get("position_id")

    if not position_id or (
        requested_position_id is not None
        and str(requested_position_id) != str(position_id)
    ):
        return {"ok": False, "reason": "Live position does not match"}

    try:
        entry = float(trade.get("entry") or trade.get("entry_price"))
        stop_loss = float(payload.get("stop_loss"))
        tp1 = float(payload.get("tp1"))
        tp2 = float(payload.get("tp2"))
    except (TypeError, ValueError):
        return {"ok": False, "reason": "Entry, SL, TP1, and TP2 must be valid prices"}

    side = str(trade.get("side") or trade.get("action") or "").upper()
    if side == "BUY" and not (stop_loss < entry < tp1 <= tp2):
        return {
            "ok": False,
            "reason": "BUY requires SL below Entry and TP1/TP2 above Entry",
        }
    if side == "SELL" and not (stop_loss > entry > tp1 >= tp2):
        return {
            "ok": False,
            "reason": "SELL requires SL above Entry and TP1/TP2 below Entry",
        }
    if side not in {"BUY", "SELL"}:
        return {"ok": False, "reason": "Unknown live trade side"}

    changed_level = str(payload.get("changed_level") or "").lower()
    if changed_level not in {"sl", "tp1", "tp2"}:
        return {"ok": False, "reason": "Unknown trade level"}

    broker_result = {"ok": True, "local_only": changed_level == "tp1"}

    if changed_level in {"sl", "tp2"}:
        broker_result = modify_position_sltp(
            position_id,
            stop_loss_price=stop_loss if changed_level == "sl" else None,
            take_profit_price=tp2 if changed_level == "tp2" else None,
        )
        if not broker_result.get("ok"):
            print("backendUpdate fail", {
                "symbol": symbol,
                "position_id": position_id,
                "changed_level": changed_level,
                "reason": broker_result.get("reason"),
            })
            return {
                "ok": False,
                "reason": broker_result.get("reason") or "cTrader rejected the modification",
                "broker_result": broker_result,
            }

    modified_at = time.time()
    user_modified_levels = {
        **(
            trade.get("user_modified_levels")
            if isinstance(trade.get("user_modified_levels"), dict)
            else {}
        ),
        "sl": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
        "updated_at": modified_at,
    }
    trade.update({
        "sl": stop_loss,
        "current_sl": stop_loss,
        "tp1": tp1,
        "take_profit_1": tp1,
        "tp2": tp2,
        "take_profit_2": tp2,
        "take_profit": tp2,
        "broker_stop_loss_confirmed": True,
        "broker_stop_loss_missing": False,
        "broker_take_profit_confirmed": True,
        "broker_take_profit_missing": False,
        "levels_modified_at": modified_at,
        "user_modified_levels": user_modified_levels,
    })
    log_live_trade_audit(
        "chart_levels_modified",
        trade,
        reason=f"Changed {changed_level or 'levels'} from chart",
    )
    save_live_backup()
    print("backendUpdate success", {
        "symbol": symbol,
        "position_id": position_id,
        "changed_level": changed_level,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
    })

    return {
        "ok": True,
        "symbol": symbol,
        "changed_level": changed_level,
        "active_order": trade,
        "broker_result": broker_result,
    }

@app.post("/live-auto-toggle")
def live_auto_toggle(payload: dict):
    sync_ctrader_account_state()

    enabled = bool(
        payload.get("enabled", False)
    )

    if enabled and not LIVE_ACCOUNT_STATE["connected"]:
        return {
            "status": "error",
            "enabled": LIVE_AUTO_TRADE_ENABLED["enabled"],
            "message": "Connect cTrader broker before enabling Auto Trade"
        }

    LIVE_AUTO_TRADE_ENABLED["enabled"] = enabled
    save_auto_trade_state()

    print(
        "LIVE AUTO TRADE STATE:",
        LIVE_AUTO_TRADE_ENABLED["enabled"]
    )

    return {
        "status": "ok",
        "enabled": LIVE_AUTO_TRADE_ENABLED["enabled"]
    }

def execute_live_order_core(payload: dict, source="manual"):
    global LAST_EXECUTION_TIME

    trade_payload = prepare_ctrader_trade(payload)

    symbol = trade_payload.get("symbol")
    side = trade_payload.get("action")
    plan = get_signal_trade_plan(symbol) or {}
    log_live_xauusd_execution_debug(
        symbol,
        plan=plan,
        trade_payload=trade_payload,
        stage="payload_prepared",
        blocked_by=None if trade_payload.get("ok") else "payload_validation",
        blocked_reason=None if trade_payload.get("ok") else trade_payload.get("reason") or trade_payload.get("message"),
        payload_valid=bool(trade_payload.get("ok")),
        order_sent=False,
        order_accepted=False,
    )

    if not LIVE_AUTO_TRADE_ENABLED["enabled"]:
        log_prefix = (
            "CTRADER AUTO TRADE BLOCKED:"
            if source == "auto"
            else "LIVE EXECUTION BLOCKED:"
        )
        print(log_prefix, "LIVE auto disabled")

        if source == "auto":
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="BLOCKED",
                reason="Live Auto is off"
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="live_auto_disabled",
                reason="Live Auto is off"
            )

        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            stage="live_auto_disabled",
            blocked_by="live_auto_disabled",
            blocked_reason="Live Auto is off",
            payload_valid=bool(trade_payload.get("ok")),
            order_sent=False,
            order_accepted=False,
        )
        return reject_ctrader_order(
            symbol,
            side,
            trade_payload.get("entry"),
            trade_payload.get("sl"),
            trade_payload.get("tp1"),
            trade_payload.get("tp2"),
            "LIVE auto disabled"
        )

    if not trade_payload.get("ok"):
        original_reason = (
            trade_payload.get("reason")
            or trade_payload.get("message")
            or "validation failed"
        )
        reason = original_reason
        if (
            not any(
                is_missing_trade_value(trade_payload.get(key))
                for key in ["entry", "sl", "tp1", "tp2"]
            )
            and any(token in str(original_reason).lower() for token in [
                "sl",
                "tp",
                "distance",
                "too close",
            ])
        ):
            reason = "LIVE BLOCKED: invalid SL/TP distance."
            trade_payload["reason"] = reason
            trade_payload["message"] = reason
            trade_payload["level_validation_reason"] = original_reason

        if any(token in str(reason).lower() for token in [
            "entry",
            "sl",
            "tp",
            "level",
            "number",
            "distance",
        ]):
            print("LIVE_ORDER_BLOCKED_MISSING_LEVELS", {
                "symbol": symbol,
                "side": side,
                "reason": reason,
                "entry": trade_payload.get("entry"),
                "sl": trade_payload.get("sl"),
                "tp1": trade_payload.get("tp1"),
                "tp2": trade_payload.get("tp2"),
            })
        log_broker_min_distance_decision(
            symbol,
            trade_payload.get("distance_details"),
            actual_blocked=True,
            final_block_reason=reason
        )
        if source == "auto":
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="BLOCKED",
                reason=reason,
                details=trade_payload.get("distance_details")
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="trade_payload_validation",
                reason=reason,
                details=trade_payload.get("distance_details")
            )
            print("CTRADER AUTO TRADE BLOCKED:", reason)
        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            stage="trade_payload_validation",
            blocked_by="payload_validation",
            blocked_reason=reason,
            payload_valid=False,
            order_sent=False,
            order_accepted=False,
        )
        return trade_payload

    levels_ok, levels_reason = trade_payload_has_required_levels(trade_payload)

    if not levels_ok:
        print("LIVE_ORDER_BLOCKED_MISSING_LEVELS", {
            "symbol": symbol,
            "side": side,
            "reason": levels_reason,
            "entry": trade_payload.get("entry"),
            "sl": trade_payload.get("sl"),
            "tp1": trade_payload.get("tp1"),
            "tp2": trade_payload.get("tp2"),
        })
        if source == "auto":
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="BLOCKED",
                reason=levels_reason,
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="missing_tp_levels",
                reason=levels_reason,
            )

        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            stage="missing_tp_levels",
            blocked_by="missing_tp_levels",
            blocked_reason=levels_reason,
            payload_valid=False,
            order_sent=False,
            order_accepted=False,
        )
        return reject_ctrader_order(
            symbol,
            side,
            trade_payload.get("entry"),
            trade_payload.get("sl"),
            trade_payload.get("tp1"),
            trade_payload.get("tp2"),
            levels_reason,
        )

    if source == "auto" and trade_payload.get("adjusted_for_broker_distance"):
        log_broker_min_distance_decision(
            symbol,
            trade_payload.get("distance_details"),
            actual_blocked=False,
            final_block_reason=trade_payload.get("adjustment_reason")
        )
        set_auto_trade_status(
            symbol=symbol,
            signal=trade_payload.get("signal"),
            action=side,
            status="READY",
            reason=trade_payload.get("adjustment_reason"),
            details=trade_payload.get("distance_details")
        )

    active_order = LIVE_ACTIVE_ORDERS.get(symbol)
    active_status = get_live_trade_status(active_order)

    if active_order and active_status in ["RUNNING", "OPEN"]:
        active_side = str(
            active_order.get("side")
            or active_order.get("action")
            or "trade"
        ).upper()
        duplicate_reason = (
            f"{symbol} blocked — existing {active_side} trade already running"
        )
        log_message = (
            f"CTRADER AUTO TRADE BLOCKED: {duplicate_reason}"
            if source == "auto"
            else "LIVE EXECUTION BLOCKED: active trade already exists"
        )

        if source == "auto":
            log_broker_min_distance_decision(
                symbol,
                trade_payload.get("distance_details"),
                actual_blocked=True,
                final_block_reason=duplicate_reason
            )
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="BLOCKED",
                reason=duplicate_reason
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="duplicate_active_trade",
                reason=duplicate_reason,
                details={
                    "active_status": active_status,
                    "active_side": active_side,
                    "active_position_id": (
                        active_order.get("position_id")
                        or active_order.get("broker_position_id")
                        or active_order.get("broker_order_id")
                        or active_order.get("order_id")
                    )
                }
            )

        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            stage="duplicate_active_trade",
            blocked_by="duplicate_active_trade",
            blocked_reason=duplicate_reason,
            existing_position=active_order,
            payload_valid=True,
            order_sent=False,
            order_accepted=False,
        )
        return reject_live_execution_block(
            symbol,
            side,
            trade_payload,
            duplicate_reason,
            log_message
        )

    risk_size = calculate_live_risk_size(
        symbol,
        trade_payload.get("entry"),
        trade_payload.get("sl")
    )

    if not risk_size.get("ok"):
        reason = risk_size.get("reason") or "Live risk sizing failed"
        live_risk_debug = build_live_risk_debug(
            symbol,
            side,
            trade_payload,
            risk_size,
            broker_reject_reason=reason,
        )
        risk_block_details = build_live_block_details(
            risk_size,
            live_risk_debug,
            {
                "blocked_reason": reason,
                "broker_rejection_reason": reason,
                "live_risk_debug": live_risk_debug,
            },
        )

        if source == "auto":
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="BLOCKED",
                reason=reason,
                details=risk_block_details,
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="risk_sizing",
                reason=reason,
                details=risk_size,
            )

        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            risk_size=risk_size,
            stage="risk_sizing",
            blocked_by="risk_sizing",
            blocked_reason=reason,
            payload_valid=False,
            order_sent=False,
            order_accepted=False,
        )
        return reject_live_execution_block(
            symbol,
            side,
            trade_payload,
            reason,
            reason,
            details=risk_block_details,
        )

    trade_payload.update({
        "volume": risk_size["lot_size"],
        "lot_size": risk_size["lot_size"],
        "volume_units": risk_size["volume_units"],
        "risk_percent": risk_size["risk_percent"],
        "risk_amount": risk_size["risk_amount"],
        "sl_pips": risk_size["sl_pips"],
        "account_balance_used": risk_size.get("account_balance"),
        "account_equity": risk_size.get("account_equity"),
        "account_equity_used": risk_size.get("account_equity_used"),
        "risk": risk_size,
    })
    log_live_xauusd_execution_debug(
        symbol,
        plan=plan,
        trade_payload=trade_payload,
        risk_size=risk_size,
        stage="risk_sizing_ok",
        blocked_by=None,
        blocked_reason=None,
        payload_valid=True,
        order_sent=False,
        order_accepted=False,
    )

    with LIVE_ORDER_LOCK:
        if symbol in LIVE_ORDER_IN_FLIGHT:
            duplicate_reason = (
                f"{symbol} blocked — order already being sent"
            )

            if source == "auto":
                set_auto_trade_status(
                    symbol=symbol,
                    signal=trade_payload.get("signal"),
                    action=side,
                    status="BLOCKED",
                    reason=duplicate_reason
                )
                log_auto_trade_blocked_reason(
                    symbol=symbol,
                    signal=trade_payload.get("signal"),
                    stage="duplicate_order_in_flight",
                    reason=duplicate_reason,
                )

            print("LIVE DUPLICATE ORDER BLOCKED:", {
                "symbol": symbol,
                "reason": duplicate_reason,
                "source": source,
            })

            log_live_xauusd_execution_debug(
                symbol,
                plan=plan,
                trade_payload=trade_payload,
                risk_size=risk_size,
                stage="duplicate_order_in_flight",
                blocked_by="duplicate_order_in_flight",
                blocked_reason=duplicate_reason,
                existing_position=LIVE_ACTIVE_ORDERS.get(symbol),
                payload_valid=True,
                order_sent=False,
                order_accepted=False,
            )
            return reject_live_execution_block(
                symbol,
                side,
                trade_payload,
                duplicate_reason,
                "LIVE EXECUTION BLOCKED: order already in flight"
            )

        active_order = LIVE_ACTIVE_ORDERS.get(symbol)
        active_status = get_live_trade_status(active_order)

        if active_order and active_status in ["RUNNING", "OPEN"]:
            active_side = str(
                active_order.get("side")
                or active_order.get("action")
                or "trade"
            ).upper()
            duplicate_reason = (
                f"{symbol} blocked — existing {active_side} trade already running"
            )

            if source == "auto":
                set_auto_trade_status(
                    symbol=symbol,
                    signal=trade_payload.get("signal"),
                    action=side,
                    status="BLOCKED",
                    reason=duplicate_reason
                )
                log_auto_trade_blocked_reason(
                    symbol=symbol,
                    signal=trade_payload.get("signal"),
                    stage="duplicate_active_trade_final_gate",
                    reason=duplicate_reason,
                )

            log_live_xauusd_execution_debug(
                symbol,
                plan=plan,
                trade_payload=trade_payload,
                risk_size=risk_size,
                stage="duplicate_active_trade_final_gate",
                blocked_by="duplicate_active_trade",
                blocked_reason=duplicate_reason,
                existing_position=active_order,
                payload_valid=True,
                order_sent=False,
                order_accepted=False,
            )
            return reject_live_execution_block(
                symbol,
                side,
                trade_payload,
                duplicate_reason,
                "LIVE EXECUTION BLOCKED: active trade already exists"
            )

        LIVE_ORDER_IN_FLIGHT.add(symbol)

    broker_positions_before_send = get_open_positions()

    if any(
        normalize_symbol(position.get("symbol")) == symbol
        for position in broker_positions_before_send
    ):
        duplicate_reason = (
            f"{symbol} blocked — broker already has open position"
        )

        with LIVE_ORDER_LOCK:
            LIVE_ORDER_IN_FLIGHT.discard(symbol)

        if source == "auto":
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="BLOCKED",
                reason=duplicate_reason
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="duplicate_broker_position_final_gate",
                reason=duplicate_reason,
                details={"open_positions_count": len(broker_positions_before_send or [])}
            )

        print("LIVE DUPLICATE BROKER POSITION BLOCKED:", {
            "symbol": symbol,
            "reason": duplicate_reason,
            "open_positions": broker_positions_before_send,
        })

        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            risk_size=risk_size,
            stage="duplicate_broker_position_final_gate",
            blocked_by="duplicate_broker_position",
            blocked_reason=duplicate_reason,
            existing_position=[
                position
                for position in broker_positions_before_send
                if normalize_symbol(position.get("symbol")) == symbol
            ],
            payload_valid=True,
            order_sent=False,
            order_accepted=False,
        )
        return reject_live_execution_block(
            symbol,
            side,
            trade_payload,
            duplicate_reason,
            "LIVE EXECUTION BLOCKED: broker position already exists"
        )

    live_risk_debug = build_live_risk_debug(
        symbol,
        side,
        trade_payload,
        risk_size,
        broker_reject_reason=None,
    )
    log_live_xauusd_execution_debug(
        symbol,
        plan=plan,
        trade_payload=trade_payload,
        risk_size=risk_size,
        stage="before_place_market_order",
        blocked_by=None,
        blocked_reason=None,
        existing_position=None,
        payload_valid=True,
        order_sent=True,
        order_accepted=False,
    )

    result = place_market_order(
        symbol=symbol,
        action=side,
        entry=trade_payload["entry"],
        sl=trade_payload["sl"],
        tp1=trade_payload["tp1"],
        tp2=trade_payload["tp2"],
        volume=trade_payload["volume"],
        volume_units=trade_payload.get("volume_units"),
        risk=trade_payload.get("risk"),
        mode=trade_payload["mode"]
    )

    if not result.get("ok", False):
        reason = result.get("reason") or result.get("message") or "Order rejected"

        if result.get("critical_unprotected_position"):
            print("LIVE RISK ERROR: trade has no broker SL/TP", {
                "symbol": symbol,
                "position_id": result.get("position_id"),
                "broker_sl_confirmed": result.get("broker_sl_confirmed"),
                "broker_tp_confirmed": result.get("broker_tp_confirmed"),
            })
            sync_live_positions()

        if is_not_enough_money_result(result):
            reason = (
                f"cTrader says not enough funds for calculated {trade_payload.get('risk_percent', get_configured_live_risk_percent())}% risk size "
                f"({trade_payload.get('lot_size')} lot)"
            )
            result = {
                **result,
                "reason": reason,
                "final_lot_size": trade_payload.get("lot_size"),
            }

        live_risk_debug = build_live_risk_debug(
            symbol,
            side,
            trade_payload,
            risk_size,
            broker_reject_reason=reason,
        )

        if source == "auto":
            rejection_volume_safety = result.get("volume_safety") if isinstance(result, dict) else None
            rejection_details = build_live_block_details(
                trade_payload.get("distance_details"),
                trade_payload.get("risk"),
                rejection_volume_safety,
                {
                    "broker_rejection_reason": reason,
                    "blocked_reason": reason,
                },
                live_risk_debug,
            )
            log_broker_min_distance_decision(
                symbol,
                trade_payload.get("distance_details"),
                actual_blocked=True,
                final_block_reason=reason
            )
            set_auto_trade_status(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                action=side,
                status="ORDER_REJECTED",
                reason=reason,
                details=rejection_details
            )
            log_auto_trade_blocked_reason(
                symbol=symbol,
                signal=trade_payload.get("signal"),
                stage="broker_order_rejected",
                reason=reason,
                details=result
            )

        log_live_xauusd_execution_debug(
            symbol,
            plan=plan,
            trade_payload=trade_payload,
            risk_size=risk_size,
            stage="broker_order_rejected",
            blocked_by="broker_order_rejected",
            blocked_reason=reason,
            existing_position=None,
            payload_valid=True,
            order_sent=True,
            order_accepted=False,
            result=result,
        )
        try:
            return {
                "ok": False,
                "message": reason,
                "reason": reason,
                "broker_rejection_reason": reason,
                "live_risk_debug": live_risk_debug,
                "result": result,
            }
        finally:
            with LIVE_ORDER_LOCK:
                LIVE_ORDER_IN_FLIGHT.discard(symbol)

    LIVE_LAST_EXECUTION_TIME[symbol] = time.time()
    LAST_EXECUTION_TIME = LIVE_LAST_EXECUTION_TIME[symbol]
    order_id = str(uuid.uuid4())
    broker_position_id = result.get("position_id")
    trade_id = (
        f"ctrader-pos-{broker_position_id}"
        if broker_position_id
        else f"flowsignal-{symbol}-{order_id}"
    )

    LIVE_ACTIVE_ORDERS[symbol] = {
        "order_id": order_id,
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side,
        "action": side,
        "status": "OPEN",
        "mode": LIVE_ACCOUNT_STATE["mode"],
        "broker": LIVE_ACCOUNT_STATE["broker"],
        "source": "broker",
        "volume": trade_payload["volume"],
        "lot_size": trade_payload.get("lot_size", trade_payload["volume"]),
        "volume_units": trade_payload.get("volume_units"),
        "risk_percent": trade_payload.get("risk_percent"),
        "risk_amount": trade_payload.get("risk_amount"),
        "sl_pips": trade_payload.get("sl_pips"),
        "distance_details": trade_payload.get("distance_details"),
        "adjusted_for_broker_distance": trade_payload.get("adjusted_for_broker_distance"),
        "account_balance_used": trade_payload.get("account_balance_used"),
        "account_equity": trade_payload.get("account_equity"),
        "account_equity_used": trade_payload.get("account_equity_used"),
        "entry": trade_payload["entry"],
        "current_price": trade_payload["entry"],
        "current_high": trade_payload["entry"],
        "current_low": trade_payload["entry"],
        "sl": trade_payload["sl"],
        "original_sl": trade_payload["sl"],
        "planned_sl": trade_payload["sl"],
        "broker_stop_loss_confirmed": result.get("broker_sl_confirmed") is True,
        "broker_stop_loss_missing": result.get("broker_sl_confirmed") is not True,
        "tp1": trade_payload["tp1"],
        "tp2": trade_payload["tp2"],
        "planned_tp1": trade_payload["tp1"],
        "planned_tp2": trade_payload["tp2"],
        "broker_take_profit_confirmed": result.get("broker_tp_confirmed") is True,
        "broker_take_profit_missing": result.get("broker_tp_confirmed") is not True,
        "position_id": broker_position_id,
        "broker_order_id": result.get("order_id"),
        "broker_position_id": broker_position_id,
        "exit_status": "HOLD",
        "exit_reason": "Trade still aligned",
        "opened_at": time.time(),
        "result": "RUNNING",
        "signal_setup_id": get_signal_setup_id(plan, side),
        "broker_result": result,
        "hit_tp1": False,
        "profit_protected": False,
        "protected_sl_price": None,
        "sl_protection_failed": False,
        "sl_protection_warning": None,
        "sl_protection_error": None,
        "sl_protection_broker_result": None,
    }
    ensure_live_trade_identity(LIVE_ACTIVE_ORDERS[symbol], symbol)
    log_live_trade_audit("order_opened", LIVE_ACTIVE_ORDERS[symbol], reason="broker order accepted")
    clear_persisted_final_signal_hold(symbol, reason="broker order accepted")
    log_live_xauusd_execution_debug(
        symbol,
        plan=plan,
        trade_payload=trade_payload,
        risk_size=risk_size,
        stage="broker_order_accepted",
        blocked_by=None,
        blocked_reason=None,
        existing_position=LIVE_ACTIVE_ORDERS[symbol],
        payload_valid=True,
        order_sent=True,
        order_accepted=True,
        result=result,
    )

    log_trade_visual_levels(LIVE_ACTIVE_ORDERS[symbol])

    save_live_backup()

    if source == "auto":
        order_sent_reason = "Order sent"

        if trade_payload.get("adjusted_for_broker_distance"):
            order_sent_reason = (
                "Order sent after broker distance adjustment: "
                f"{trade_payload.get('adjustment_reason')}"
            )

        set_auto_trade_status(
            symbol=symbol,
            signal=trade_payload.get("signal"),
            action=side,
            status="ORDER_SENT",
            reason=order_sent_reason,
            details=trade_payload.get("distance_details")
        )

    try:
        return {
            "ok": True,
            "result": result,
            "active_order": LIVE_ACTIVE_ORDERS[symbol],
            "execution_ready": LIVE_ACCOUNT_STATE.get("execution_ready", False),
            "broker_order_sent": result.get("broker_order_sent", True)
        }
    finally:
        with LIVE_ORDER_LOCK:
            LIVE_ORDER_IN_FLIGHT.discard(symbol)

@app.post("/execute-live-order")
def execute_live_order(payload: dict):
    return execute_live_order_core(payload, source="manual")
