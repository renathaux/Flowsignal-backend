"""Production-running, non-executing Strategy V2 shadow evaluator.

Architectural safety boundary: this module imports no cTrader, broker, order,
LIVE/PAPER, cooldown, or strategy-memory module.  It observes completed V1
results and closed candles, stores independent state, and simulates outcomes.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import math
import uuid

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from db import SessionLocal
from models import (
    StrategyShadowEvaluation,
    StrategyShadowRuntime,
    StrategyShadowTrade,
)
from services.strategy_settings_service import get_configured_rr_window


logger = logging.getLogger(__name__)
VERSIONS = {
    "XAUUSD": "XAUUSD_V2_SHADOW",
    "EURUSD": "EURUSD_V2_RESEARCH",
}
EXTENSION_LIMIT_ATR = 0.75
RETEST_DISTANCE_ATR = 0.20
MAX_RETEST_M15_BARS = 4
TERMINAL_OUTCOMES = {"FULL_TP", "TP1_PROTECTED", "SL", "EXPIRED", "AMBIGUOUS_INTRABAR"}


def _utc(value):
    if value in (None, ""):
        return None
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")
    return parsed.to_pydatetime()


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _mapping(value):
    """Return diagnostic mappings while tolerating V1 status-string fields."""
    return value if isinstance(value, dict) else {}


def _json_safe(value):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc(value).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return str(value)


def _hash(payload):
    encoded = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _closed_frame(frame, timeframe_minutes, now):
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    index = pd.to_datetime(data.index, utc=True)
    data.index = index
    cutoff = pd.Timestamp(now).tz_convert("UTC") if pd.Timestamp(now).tzinfo else pd.Timestamp(now, tz="UTC")
    return data[index + pd.Timedelta(minutes=timeframe_minutes) <= cutoff]


def _atr14(frame):
    if frame is None or len(frame) < 2:
        return None
    high = frame["High"].astype(float)
    low = frame["Low"].astype(float)
    close = frame["Close"].astype(float)
    true_range = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    value = true_range.rolling(14, min_periods=min(14, len(true_range))).mean().iloc[-1]
    return _number(value)


def _setup_id(result, breakout, confirmation):
    explicit = (
        result.get("signal_setup_id")
        or result.get("setup_fingerprint")
        or result.get("signal_fingerprint")
    )
    if explicit:
        return str(explicit)
    return _hash({
        "symbol": result.get("symbol"),
        "direction": result.get("signal") or breakout.get("side"),
        "swing": {
            key: (breakout.get("swing") or {}).get(key)
            for key in ("type", "time", "price")
        },
        "bos": breakout.get("break_close_time") or breakout.get("break_time"),
        "level": breakout.get("level"),
        "confirmation": confirmation.get("confirmation_close_time"),
    })


def build_context(symbol, result, data_5m, data_15m, now=None):
    """Project a V1 result to finite values used only by the shadow engine."""
    now = _utc(now) or datetime.now(timezone.utc)
    closed_5m = _closed_frame(data_5m, 5, now)
    closed_15m = _closed_frame(data_15m, 15, now)
    breakout = _mapping(result.get("fifteen_m_swing_break"))
    confirmation = _mapping(result.get("confirmation_5m"))
    consolidation = _mapping(result.get("consolidation"))
    trend = _mapping(result.get("trend_15m"))
    signal = str(result.get("signal") or result.get("final_signal") or "WAIT").upper()
    direction = signal if signal in {"BUY", "SELL"} else str(breakout.get("side") or "").upper() or None
    atr = (
        _number(breakout.get("atr14"))
        or _number(consolidation.get("atr14"))
        or _atr14(closed_15m)
    )
    bos_level = _number(breakout.get("level") or result.get("fifteen_m_swing_level"))
    reference_price = _number(result.get("price") or result.get("entry_price"))
    extension = (
        abs(reference_price - bos_level) / atr
        if reference_price is not None and bos_level is not None and atr and atr > 0
        else None
    )
    latest_5m = None
    if not closed_5m.empty:
        row = closed_5m.iloc[-1]
        latest_5m = {
            "open_time": closed_5m.index[-1].to_pydatetime(),
            "close_time": (closed_5m.index[-1] + pd.Timedelta(minutes=5)).to_pydatetime(),
            "open": _number(row["Open"]),
            "high": _number(row["High"]),
            "low": _number(row["Low"]),
            "close": _number(row["Close"]),
        }
    latest_m15_close = None
    if not closed_15m.empty:
        latest_m15_close = (closed_15m.index[-1] + pd.Timedelta(minutes=15)).to_pydatetime()
    entry = _number(result.get("entry_price"))
    sl = _number(result.get("stop_loss"))
    tp1 = _number(result.get("tp1"))
    tp2 = _number(result.get("tp2"))
    rr = _number(result.get("risk_reward_ratio") or result.get("risk_reward"))
    protected = _number(result.get("protected_sl_price"))
    if protected is None and entry is not None and tp2 is not None:
        protected = entry + 0.50 * (tp2 - entry)
    risk_percent = _number(result.get("risk_percent") or result.get("configured_risk_percent"))
    try:
        minimum_rr, maximum_rr = get_configured_rr_window()
    except Exception:
        # Shadow evaluation must never interfere with V1 if settings storage is
        # temporarily unavailable. These are the production-equivalent defaults.
        minimum_rr, maximum_rr = 1.20, 2.00
    return {
        "symbol": str(symbol).upper(),
        "version": VERSIONS[str(symbol).upper()],
        "evaluated_at": now,
        "signal": signal,
        "direction": direction,
        "v1_decision": "TRADE" if signal in {"BUY", "SELL"} else "WAIT",
        "v1_reason": result.get("blocked_reason") or result.get("plan_reason") or result.get("entry_timing"),
        "setup_fingerprint": _setup_id(result, breakout, confirmation),
        "structure_type": str(breakout.get("break_type") or result.get("fifteen_m_break_classification") or "").upper() or None,
        "bos_level": bos_level,
        "bos_timestamp": _utc(breakout.get("break_close_time") or result.get("fifteen_m_break_close_time")),
        "bos_buffer": _number(breakout.get("bos_buffer") or breakout.get("buffer")),
        "atr14": atr,
        "ema_state": str(trend.get("trend") or "NEUTRAL").upper(),
        "consolidation_state": "BLOCKED" if consolidation.get("is_consolidation") else "CLEAR",
        "m5_confirmation_timestamp": _utc(confirmation.get("confirmation_close_time") or result.get("five_m_closed_candle_time")),
        "reference_price": reference_price,
        "extension_atr": extension,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "protected_sl": protected,
        "rr": rr,
        "risk_percent": risk_percent,
        "minimum_rr": minimum_rr,
        "maximum_rr": maximum_rr,
        "latest_5m": latest_5m,
        "latest_m15_close": latest_m15_close,
        "risk_valid": all(value is not None for value in (entry, sl, tp1, tp2, rr)),
    }


def _new_pending(context, reason):
    bos_time = context.get("bos_timestamp") or context["evaluated_at"]
    return {
        "setup_fingerprint": context["setup_fingerprint"],
        "direction": context["direction"],
        "bos_level": context["bos_level"],
        "bos_timestamp": bos_time.isoformat(),
        "bos_buffer": context["bos_buffer"],
        "atr14": context["atr14"],
        "sl": context["sl"],
        "tp1": context["tp1"],
        "tp2": context["tp2"],
        "protected_sl": context["protected_sl"],
        "risk_percent": context["risk_percent"],
        "source_rr": context["rr"],
        "minimum_rr": context.get("minimum_rr", 1.20),
        "maximum_rr": context.get("maximum_rr", 2.00),
        "initial_confirmation": (
            context["m5_confirmation_timestamp"].isoformat()
            if context.get("m5_confirmation_timestamp") else None
        ),
        "retest_timestamp": None,
        "continuation_timestamp": None,
        "expiry": (bos_time + timedelta(minutes=15 * MAX_RETEST_M15_BARS)).isoformat(),
        "reason": reason,
    }


def _retest_decision(context, state, pending):
    latest = context.get("latest_5m")
    expiry = _utc(pending.get("expiry"))
    if context.get("latest_m15_close") and expiry and context["latest_m15_close"] > expiry:
        state["pending_setup"] = None
        return "EXPIRED_NO_RETEST", "SETUP_EXPIRED_AFTER_4_M15_CANDLES", None
    if not latest or latest.get("close") is None:
        return "WAIT_RETEST", "WAIT_CLOSED_M5_DATA", None
    atr = _number(pending.get("atr14"))
    level = _number(pending.get("bos_level"))
    if not atr or level is None:
        state["pending_setup"] = None
        return "BLOCK", "INVALID_RETEST_REFERENCE", None
    close_time = latest["close_time"]
    initial_confirmation = _utc(pending.get("initial_confirmation"))
    if initial_confirmation and close_time <= initial_confirmation:
        return "WAIT_RETEST", "WAIT_M5_AFTER_INITIAL_CONFIRMATION", None
    if not pending.get("retest_timestamp"):
        if abs(latest["close"] - level) <= RETEST_DISTANCE_ATR * atr:
            pending["retest_timestamp"] = close_time.isoformat()
            state["pending_setup"] = pending
            return "WAIT_CONTINUATION", "BOS_RETEST_CONFIRMED", None
        return "WAIT_RETEST", "WAIT_BOS_RETEST", None
    retest_time = _utc(pending["retest_timestamp"])
    if close_time <= retest_time:
        return "WAIT_CONTINUATION", "WAIT_LATER_M5_CONTINUATION", None
    direction = pending["direction"]
    buffer_value = _number(pending.get("bos_buffer")) or 0.0
    bullish = latest["close"] > latest["open"]
    bearish = latest["close"] < latest["open"]
    continuation = (
        bullish and latest["close"] >= level + buffer_value
        if direction == "BUY"
        else bearish and latest["close"] <= level - buffer_value
    )
    if not continuation:
        return "WAIT_CONTINUATION", "WAIT_DIRECTIONAL_M5_CONTINUATION", None
    extension = abs(latest["close"] - level) / atr
    if extension > EXTENSION_LIMIT_ATR:
        return "WAIT_EXTENDED", "CONTINUATION_STILL_EXTENDED", None
    sl = _number(pending.get("sl"))
    tp2 = _number(pending.get("tp2"))
    if sl is None or tp2 is None or latest["close"] == sl:
        return "BLOCK", "RETEST_RISK_PLAN_INVALID", None
    rr = abs(tp2 - latest["close"]) / abs(latest["close"] - sl)
    minimum_rr = _number(pending.get("minimum_rr")) or 1.20
    maximum_rr = _number(pending.get("maximum_rr")) or 2.00
    if rr < minimum_rr or rr > maximum_rr:
        return "BLOCK", "RETEST_RR_INVALID", None
    tp1 = latest["close"] + 0.80 * (tp2 - latest["close"])
    protected = latest["close"] + 0.50 * (tp2 - latest["close"])
    pending["continuation_timestamp"] = close_time.isoformat()
    state["pending_setup"] = None
    trade = {
        "setup_fingerprint": pending["setup_fingerprint"],
        "direction": direction,
        "entry_timestamp": close_time,
        "entry": latest["close"],
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "protected_sl": protected,
        "risk_percent": pending.get("risk_percent"),
        "rr": rr,
        "retest_timestamp": retest_time,
        "continuation_timestamp": close_time,
        "extension_atr": extension,
    }
    return f"{direction}_READY", "RETEST_AND_FRESH_M5_CONFIRMED", trade


def decide_v2(context, state):
    """Pure V2 state transition. It cannot execute or mutate V1 state."""
    state = dict(state or {})
    state.setdefault("pending_setup", None)
    state.setdefault("post_sl_reset", None)
    if state.get("active_shadow_trade_id"):
        return "SHADOW_TRADE_OPEN", "ONE_SHADOW_TRADE_PER_SYMBOL", state, None

    pending = state.get("pending_setup")
    if pending:
        decision, reason, trade = _retest_decision(context, state, dict(pending))
        return decision, reason, state, trade

    direction = context.get("direction")
    reset = dict(state.get("post_sl_reset") or {})
    if reset and direction and direction != reset.get("stopped_direction"):
        structure_type = context.get("structure_type")
        bos_time = context.get("bos_timestamp")
        exit_time = _utc(reset.get("exit_timestamp"))
        if structure_type == "CHOCH" and bos_time and (not exit_time or bos_time > exit_time):
            reset["opposite_direction"] = direction
            reset["choch_timestamp"] = bos_time.isoformat()
            reset["state"] = "OPPOSITE_CHOCH_SEEN"
            state["post_sl_reset"] = reset
            return "WAIT_FRESH_OPPOSITE_BOS", "OPPOSITE_CHOCH_ALONE_BLOCKED", state, None
        choch_time = _utc(reset.get("choch_timestamp"))
        if structure_type != "BOS" or not choch_time or not bos_time or bos_time <= choch_time:
            state["post_sl_reset"] = reset
            return "WAIT_NEW_STRUCTURE_AFTER_SL", "REQUIRE_CHOCH_THEN_LATER_BOS", state, None
        confirmation_time = context.get("m5_confirmation_timestamp")
        if not confirmation_time or confirmation_time <= bos_time:
            reset["state"] = "WAIT_OPPOSITE_M5_CONFIRMATION"
            state["post_sl_reset"] = reset
            return "WAIT_OPPOSITE_M5_CONFIRMATION", "REQUIRE_M5_AFTER_FRESH_BOS", state, None
        state["post_sl_reset"] = None

    if context.get("v1_decision") != "TRADE":
        return "WAIT", context.get("v1_reason") or "V1_SETUP_NOT_READY", state, None
    if not context.get("risk_valid"):
        return "BLOCK", "RISK_OR_RR_NOT_PROVABLE", state, None
    extension = context.get("extension_atr")
    if extension is None:
        return "BLOCK", "EXTENSION_NOT_PROVABLE", state, None

    # EURUSD remains research-only and always requires the observed retest path.
    retest_required = context["symbol"] == "EURUSD" or extension > EXTENSION_LIMIT_ATR
    if retest_required:
        state["pending_setup"] = _new_pending(
            context,
            "EURUSD_RESEARCH_RETEST" if context["symbol"] == "EURUSD" else "ENTRY_TOO_EXTENDED",
        )
        decision = "WAIT_RETEST" if context["symbol"] == "EURUSD" else "WAIT_EXTENDED"
        reason = "WAIT_BOS_RETEST" if context["symbol"] == "EURUSD" else "ENTRY_TOO_EXTENDED"
        return decision, reason, state, None

    trade = {
        "setup_fingerprint": context["setup_fingerprint"],
        "direction": context["direction"],
        "entry_timestamp": context.get("m5_confirmation_timestamp") or context["evaluated_at"],
        "entry": context["entry"],
        "sl": context["sl"],
        "tp1": context["tp1"],
        "tp2": context["tp2"],
        "protected_sl": context["protected_sl"],
        "risk_percent": context["risk_percent"],
        "rr": context["rr"],
        "retest_timestamp": None,
        "continuation_timestamp": context.get("m5_confirmation_timestamp"),
        "extension_atr": extension,
    }
    return f"{context['direction']}_READY", "NORMAL_ENTRY_WITHIN_EXTENSION_LIMIT", state, trade


def _touches(trade, candle):
    direction = trade.direction
    low, high = candle["low"], candle["high"]
    active_stop = trade.protected_sl if trade.tp1_reached else trade.sl
    stop_hit = low <= active_stop if direction == "BUY" else high >= active_stop
    tp2_hit = high >= trade.tp2 if direction == "BUY" else low <= trade.tp2
    tp1_hit = high >= trade.tp1 if direction == "BUY" else low <= trade.tp1
    return stop_hit, tp1_hit, tp2_hit, active_stop


def _advance_open_trade(trade, candles, state, now):
    risk_distance = abs(trade.entry - trade.sl)
    if not risk_distance:
        return None
    for candle in candles:
        if trade.last_processed_m5 and candle["close_time"] <= _utc(trade.last_processed_m5):
            continue
        favorable = (
            (candle["high"] - trade.entry) / risk_distance
            if trade.direction == "BUY"
            else (trade.entry - candle["low"]) / risk_distance
        )
        adverse = (
            (trade.entry - candle["low"]) / risk_distance
            if trade.direction == "BUY"
            else (candle["high"] - trade.entry) / risk_distance
        )
        trade.mfe_r = max(float(trade.mfe_r or 0), favorable)
        trade.mae_r = max(float(trade.mae_r or 0), adverse)
        tp1_was_reached = bool(trade.tp1_reached)
        stop_hit, tp1_hit, tp2_hit, active_stop = _touches(trade, candle)
        if stop_hit and (tp2_hit or (tp1_hit and not tp1_was_reached)):
            trade.status = "AMBIGUOUS_INTRABAR"
            trade.exit_timestamp = candle["close_time"]
            trade.exit_price = None
            trade.r_result = None
        elif tp2_hit:
            trade.status = "FULL_TP"
            trade.tp1_reached = True
            trade.tp2_reached = True
            trade.exit_timestamp = candle["close_time"]
            trade.exit_price = trade.tp2
            trade.r_result = trade.rr
        elif stop_hit:
            trade.status = "TP1_PROTECTED" if trade.tp1_reached else "SL"
            trade.sl_reached = True
            trade.exit_timestamp = candle["close_time"]
            trade.exit_price = active_stop
            trade.r_result = (abs(active_stop - trade.entry) / risk_distance) if trade.tp1_reached else -1.0
            if trade.direction == "SELL" and trade.tp1_reached:
                trade.r_result = abs(trade.entry - active_stop) / risk_distance
        elif tp1_hit:
            trade.tp1_reached = True
        trade.last_processed_m5 = candle["close_time"]
        trade.updated_at = now
        if trade.status in TERMINAL_OUTCOMES:
            state["active_shadow_trade_id"] = None
            if trade.status == "SL":
                state["pending_setup"] = None
                state["post_sl_reset"] = {
                    "state": "WAIT_NEW_STRUCTURE_AFTER_SL",
                    "stopped_direction": trade.direction,
                    "exit_timestamp": trade.exit_timestamp.isoformat(),
                    "related_previous_trade_id": trade.id,
                }
            return trade.status
    return None


def _closed_candles_after(frame, after, now):
    data = _closed_frame(frame, 5, now)
    items = []
    for timestamp, row in data.iterrows():
        close_time = (timestamp + pd.Timedelta(minutes=5)).to_pydatetime()
        if after and close_time <= after:
            continue
        items.append({
            "close_time": close_time,
            "open": float(row["Open"]), "high": float(row["High"]),
            "low": float(row["Low"]), "close": float(row["Close"]),
        })
    return items


def _runtime(db, symbol, version, now):
    row = db.execute(
        select(StrategyShadowRuntime)
        .where(
            StrategyShadowRuntime.symbol == symbol,
            StrategyShadowRuntime.strategy_version == version,
        )
        .with_for_update()
    ).scalar_one_or_none()
    if row is None:
        row = StrategyShadowRuntime(
            symbol=symbol,
            strategy_version=version,
            started_at=now,
            updated_at=now,
            state_json={
                "pending_setup": None,
                "post_sl_reset": None,
                "active_shadow_trade_id": None,
            },
        )
        db.add(row)
        db.flush()
    return row


def _create_trade(db, context, state, trade_data, evaluation, now):
    shadow_id = str(uuid.uuid4())
    trade = StrategyShadowTrade(
        shadow_trade_id=shadow_id,
        symbol=context["symbol"], strategy_version=context["version"],
        setup_fingerprint=trade_data["setup_fingerprint"], direction=trade_data["direction"],
        entry_timestamp=trade_data["entry_timestamp"], entry=trade_data["entry"],
        sl=trade_data["sl"], tp1=trade_data["tp1"], tp2=trade_data["tp2"],
        protected_sl=trade_data["protected_sl"], risk_percent=trade_data.get("risk_percent"),
        rr=trade_data["rr"], status="OPEN", last_processed_m5=trade_data["entry_timestamp"],
        related_previous_trade_id=(state.get("post_sl_reset") or {}).get("related_previous_trade_id"),
        v1_evaluation_id=evaluation.id,
        diagnostics_json=_json_safe(trade_data), created_at=now, updated_at=now,
    )
    db.add(trade)
    db.flush()
    state["active_shadow_trade_id"] = shadow_id
    return trade


def evaluate_cycle_safely(symbol, result, data_5m=None, data_15m=None, *, now=None):
    """Best-effort observer. Failure cannot change V1 or execution behavior."""
    try:
        context = build_context(symbol, result, data_5m, data_15m, now=now)
        db = SessionLocal()
        try:
            runtime = _runtime(db, context["symbol"], context["version"], context["evaluated_at"])
            state = dict(runtime.state_json or {})
            active_id = state.get("active_shadow_trade_id")
            terminal_outcome = None
            if active_id:
                trade = db.execute(
                    select(StrategyShadowTrade).where(StrategyShadowTrade.shadow_trade_id == active_id)
                ).scalar_one_or_none()
                if trade and trade.status == "OPEN":
                    candles = _closed_candles_after(data_5m, _utc(trade.last_processed_m5), context["evaluated_at"])
                    terminal_outcome = _advance_open_trade(
                        trade, candles, state, context["evaluated_at"]
                    )
                else:
                    state["active_shadow_trade_id"] = None

            if terminal_outcome:
                decision = "WAIT_NEW_STRUCTURE_AFTER_SL" if terminal_outcome == "SL" else "WAIT"
                reason = f"SHADOW_TRADE_CLOSED_{terminal_outcome}"
                trade_data = None
            else:
                decision, reason, state, trade_data = decide_v2(context, state)
            marker = (context.get("latest_5m") or {}).get("close_time") or context["evaluated_at"]
            evaluation_key = _hash({
                "symbol": context["symbol"], "version": context["version"],
                "marker": marker, "setup": context["setup_fingerprint"],
                "v1": context["v1_decision"], "v2": decision, "reason": reason,
                "retest": (state.get("pending_setup") or {}).get("retest_timestamp"),
            })
            evaluation = StrategyShadowEvaluation(
                evaluation_key=evaluation_key, evaluated_at=context["evaluated_at"],
                symbol=context["symbol"], timeframe="M5", strategy_version=context["version"],
                setup_fingerprint=context["setup_fingerprint"], direction=context["direction"],
                structure_type=context["structure_type"], bos_level=context["bos_level"],
                bos_timestamp=context["bos_timestamp"], bos_buffer=context["bos_buffer"],
                atr14=context["atr14"], ema_state=context["ema_state"],
                consolidation_state=context["consolidation_state"],
                m5_confirmation_timestamp=context["m5_confirmation_timestamp"],
                reference_price=context["reference_price"], extension_atr=context["extension_atr"],
                v1_decision=context["v1_decision"], v1_reason=context["v1_reason"],
                v2_decision=decision, v2_reason=reason,
                hypothetical_entry=(trade_data or {}).get("entry"),
                hypothetical_sl=(trade_data or {}).get("sl"),
                hypothetical_tp1=(trade_data or {}).get("tp1"),
                hypothetical_tp2=(trade_data or {}).get("tp2"),
                hypothetical_rr=(trade_data or {}).get("rr"),
                hypothetical_risk_percent=(trade_data or {}).get("risk_percent"),
                retest_timestamp=_utc((state.get("pending_setup") or {}).get("retest_timestamp")) or (trade_data or {}).get("retest_timestamp"),
                continuation_timestamp=(trade_data or {}).get("continuation_timestamp"),
                setup_expiry=_utc((state.get("pending_setup") or {}).get("expiry")),
                related_previous_trade_id=(state.get("post_sl_reset") or {}).get("related_previous_trade_id"),
                post_sl_reset_state=(state.get("post_sl_reset") or {}).get("state"),
                diagnostics_json=_json_safe({"context": context, "state": state, "shadow_only": True}),
            )
            db.add(evaluation)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                return {"ok": True, "deduplicated": True, "evaluation_key": evaluation_key}
            if trade_data:
                try:
                    _create_trade(db, context, state, trade_data, evaluation, context["evaluated_at"])
                except IntegrityError:
                    db.rollback()
                    return {"ok": True, "deduplicated": True, "setup_fingerprint": context["setup_fingerprint"]}
            runtime.state_json = _json_safe(state)
            runtime.updated_at = context["evaluated_at"]
            db.commit()
            logger.info(
                "V2_SHADOW_EVALUATION symbol=%s version=%s v1=%s v2=%s reason=%s extension_atr=%s",
                context["symbol"], context["version"], context["v1_decision"], decision,
                reason, context["extension_atr"],
            )
            return {"ok": True, "v2_decision": decision, "reason": reason, "shadow_only": True}
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    except Exception as exc:
        logger.warning(
            "V2_SHADOW_EVALUATION_WARNING symbol=%s error_type=%s error=%s",
            str(symbol).upper(), type(exc).__name__, str(exc),
        )
        return {"ok": False, "shadow_only": True, "error": str(exc)}


def link_v1_execution_safely(symbol, setup_fingerprint, broker_result, trade_payload=None):
    """Observability-only link; never changes or retries a broker result."""
    if not setup_fingerprint:
        return False
    try:
        db = SessionLocal()
        evaluation = db.execute(
            select(StrategyShadowEvaluation)
            .where(
                StrategyShadowEvaluation.symbol == str(symbol).upper(),
                StrategyShadowEvaluation.setup_fingerprint == str(setup_fingerprint),
            )
            .order_by(StrategyShadowEvaluation.evaluated_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if not evaluation:
            return False
        evaluation.v1_order_id = str(broker_result.get("order_id") or "") or None
        evaluation.v1_position_id = str(broker_result.get("position_id") or "") or None
        evaluation.v1_outcome_json = _json_safe({
            "ok": bool(broker_result.get("ok")),
            "entry": broker_result.get("entry") or (trade_payload or {}).get("entry"),
            "sl": broker_result.get("sl") or (trade_payload or {}).get("sl"),
            "tp": broker_result.get("tp") or (trade_payload or {}).get("tp2"),
            "reason": broker_result.get("reason") or broker_result.get("message"),
        })
        db.commit()
        return True
    except Exception as exc:
        logger.warning("V2_SHADOW_V1_LINK_WARNING error=%s", str(exc))
        return False
    finally:
        if "db" in locals():
            db.close()


def _trade_metrics(rows):
    closed = [row for row in rows if row.status in TERMINAL_OUTCOMES]
    decided = [row for row in closed if row.r_result is not None]
    wins = [row for row in decided if row.r_result > 0]
    losses = [row for row in decided if row.r_result < 0]
    gross_win = sum(row.r_result for row in wins)
    gross_loss = abs(sum(row.r_result for row in losses))
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    longest_loss = current_loss = 0
    buy_loss_sell_flips = 0
    sell_loss_buy_flips = 0
    ordered = sorted(decided, key=lambda item: item.entry_timestamp)
    for index, row in enumerate(ordered):
        equity += row.r_result
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        if row.r_result < 0:
            current_loss += 1
            longest_loss = max(longest_loss, current_loss)
        else:
            current_loss = 0
        if index:
            previous = ordered[index - 1]
            if previous.r_result < 0 and previous.direction == "BUY" and row.direction == "SELL":
                buy_loss_sell_flips += 1
            elif previous.r_result < 0 and previous.direction == "SELL" and row.direction == "BUY":
                sell_loss_buy_flips += 1
    extension_values = [
        _number((row.diagnostics_json or {}).get("extension_atr"))
        for row in rows
        if _number((row.diagnostics_json or {}).get("extension_atr")) is not None
    ]
    return {
        "trades": len(rows), "open": sum(row.status == "OPEN" for row in rows),
        "wins": len(wins), "losses": len(losses),
        "protected_exits": sum(row.status == "TP1_PROTECTED" for row in rows),
        "full_tp_winners": sum(row.status == "FULL_TP" for row in rows),
        "ambiguous": sum(row.status == "AMBIGUOUS_INTRABAR" for row in rows),
        "win_rate": round(100 * len(wins) / len(decided), 2) if decided else 0.0,
        "net_r": round(sum(row.r_result for row in decided), 4),
        "expectancy_r": round(sum(row.r_result for row in decided) / len(decided), 4) if decided else 0.0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else (None if not gross_win else "INF"),
        "max_drawdown_r": round(max_drawdown, 4),
        "longest_losing_streak": longest_loss,
        "average_trade_r": round(sum(row.r_result for row in decided) / len(decided), 4) if decided else 0.0,
        "entry_extension_average": round(sum(extension_values) / len(extension_values), 4) if extension_values else None,
        "entries_over_075_atr": sum(value > EXTENSION_LIMIT_ATR for value in extension_values),
        "buy_loss_to_sell_flip_count": buy_loss_sell_flips,
        "sell_loss_to_buy_flip_count": sell_loss_buy_flips,
    }


def get_shadow_summary(symbol):
    symbol = str(symbol).upper()
    version = VERSIONS[symbol]
    db = SessionLocal()
    try:
        runtime = db.execute(select(StrategyShadowRuntime).where(
            StrategyShadowRuntime.symbol == symbol,
            StrategyShadowRuntime.strategy_version == version,
        )).scalar_one_or_none()
        evaluations = db.execute(select(StrategyShadowEvaluation).where(
            StrategyShadowEvaluation.symbol == symbol,
            StrategyShadowEvaluation.strategy_version == version,
        ).order_by(StrategyShadowEvaluation.evaluated_at.desc())).scalars().all()
        trades = db.execute(select(StrategyShadowTrade).where(
            StrategyShadowTrade.symbol == symbol,
            StrategyShadowTrade.strategy_version == version,
        ).order_by(StrategyShadowTrade.entry_timestamp.desc())).scalars().all()
        latest = evaluations[0] if evaluations else None
        comparisons = {}
        for row in evaluations:
            key = f"V1_{row.v1_decision}__V2_{row.v2_decision}"
            comparisons[key] = comparisons.get(key, 0) + 1
        extension_values = [row.extension_atr for row in evaluations if row.v1_decision == "TRADE" and row.extension_atr is not None]
        state = dict(runtime.state_json or {}) if runtime else {}
        return {
            "symbol": symbol, "strategy_version": version, "shadow_only": True,
            "warning": "SHADOW — DOES NOT PLACE ORDERS",
            "started_at": runtime.started_at.isoformat() if runtime else None,
            "updated_at": runtime.updated_at.isoformat() if runtime else None,
            "current": {
                "v1_decision": latest.v1_decision if latest else "WAIT",
                "v2_decision": latest.v2_decision if latest else "WAIT",
                "v2_reason": latest.v2_reason if latest else "NO_SHADOW_DATA",
                "direction": latest.direction if latest else None,
                "extension_atr": latest.extension_atr if latest else None,
                "retest_timestamp": latest.retest_timestamp.isoformat() if latest and latest.retest_timestamp else None,
                "post_sl_reset_state": latest.post_sl_reset_state if latest else None,
                "hypothetical_open_position": state.get("active_shadow_trade_id"),
            },
            "v1": {
                "evaluated_setups": len(evaluations),
                "trade_decisions": sum(row.v1_decision == "TRADE" for row in evaluations),
                "linked_executions": sum(bool(row.v1_position_id or row.v1_order_id) for row in evaluations),
                "entry_extension_average": round(sum(extension_values) / len(extension_values), 4) if extension_values else None,
                "entries_over_075_atr": sum(value > EXTENSION_LIMIT_ATR for value in extension_values),
                "note": "Actual outcome metrics are populated only when a V1 execution can be linked after shadow start.",
            },
            "v2": {"evaluated_setups": len(evaluations), **_trade_metrics(trades)},
            "disagreements": comparisons,
            "recent_trades": [_serialize_trade(row) for row in trades[:10]],
        }
    finally:
        db.close()


def _serialize_trade(row):
    return {
        "shadow_trade_id": row.shadow_trade_id, "date_time": row.entry_timestamp.isoformat(),
        "symbol": row.symbol, "direction": row.direction, "entry": row.entry,
        "sl": row.sl, "tp1": row.tp1, "tp2": row.tp2, "outcome": row.status,
        "r": row.r_result, "mae_r": row.mae_r, "mfe_r": row.mfe_r,
        "v1_actual_outcome": row.v1_outcome_json,
    }


def get_shadow_history(symbol=None, decision=None, limit=100, offset=0):
    statement = select(StrategyShadowEvaluation)
    if symbol:
        statement = statement.where(StrategyShadowEvaluation.symbol == str(symbol).upper())
    if decision:
        statement = statement.where(StrategyShadowEvaluation.v2_decision == str(decision).upper())
    statement = statement.order_by(StrategyShadowEvaluation.evaluated_at.desc()).offset(max(0, offset)).limit(min(500, max(1, limit)))
    db = SessionLocal()
    try:
        rows = db.execute(statement).scalars().all()
        return [{
            "id": row.id, "timestamp": row.evaluated_at.isoformat(), "symbol": row.symbol,
            "strategy_version": row.strategy_version, "setup_fingerprint": row.setup_fingerprint,
            "direction": row.direction, "v1_decision": row.v1_decision,
            "v2_decision": row.v2_decision, "v2_reason": row.v2_reason,
            "bos_level": row.bos_level, "atr14": row.atr14, "extension_atr": row.extension_atr,
            "entry": row.hypothetical_entry, "sl": row.hypothetical_sl,
            "tp1": row.hypothetical_tp1, "tp2": row.hypothetical_tp2,
            "rr": row.hypothetical_rr, "diagnostics": row.diagnostics_json,
            "v1_actual_outcome": row.v1_outcome_json,
        } for row in rows]
    finally:
        db.close()
