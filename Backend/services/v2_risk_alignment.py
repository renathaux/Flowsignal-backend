"""Risk-plan alignment for Strategy V2 shadow evaluation.

This module changes simulation only. It never imports broker, order, LIVE/PAPER,
or execution modules. V2 keeps its research/retest behavior, but any hypothetical
entry must use the same configured minimum SL distance and RR settings as V1.
"""
from __future__ import annotations

import math

from services.settings_service import get_tp1_ratio_of_tp2
from services.strategy_settings_service import get_cached_execution_settings


_INSTALLED = False


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _point_size(symbol):
    return 0.01 if str(symbol or "").upper() == "XAUUSD" else 0.00001


def _minimum_sl_points():
    try:
        configured = get_cached_execution_settings()
        value = float(configured.get("minimum_sl_distance_points", 100))
        return value if math.isfinite(value) and value > 0 else 100.0
    except Exception:
        return 100.0


def _target_rr(minimum_rr, maximum_rr, source_rr=None):
    minimum = _number(minimum_rr) or 1.20
    maximum = _number(maximum_rr) or 2.00
    if maximum < minimum:
        maximum = minimum
    source = _number(source_rr)
    if source is None:
        return minimum
    return min(max(source, minimum), maximum)


def build_v2_risk_plan(
    symbol,
    direction,
    entry,
    sl,
    minimum_rr,
    maximum_rr,
    *,
    source_rr=None,
    minimum_sl_points=None,
):
    """Build TP levels from the actual V2 entry and validated structure SL."""
    entry = _number(entry)
    sl = _number(sl)
    direction = str(direction or "").upper()
    if entry is None or sl is None or direction not in {"BUY", "SELL"}:
        return {"ok": False, "reason": "V2_RISK_LEVELS_MISSING"}

    side_ok = sl < entry if direction == "BUY" else sl > entry
    if not side_ok:
        return {"ok": False, "reason": "V2_SL_WRONG_SIDE"}

    point = _point_size(symbol)
    risk_distance = abs(entry - sl)
    sl_points = risk_distance / point
    required_points = _number(minimum_sl_points)
    if required_points is None:
        required_points = _minimum_sl_points()
    if sl_points + 1e-9 < required_points:
        return {
            "ok": False,
            "reason": "V2_SL_UNDER_MINIMUM_POINTS",
            "sl_distance_points": sl_points,
            "minimum_sl_distance_points": required_points,
        }

    rr = _target_rr(minimum_rr, maximum_rr, source_rr)
    reward_distance = risk_distance * rr
    tp2 = entry + reward_distance if direction == "BUY" else entry - reward_distance
    try:
        tp1_ratio = float(get_tp1_ratio_of_tp2())
    except Exception:
        tp1_ratio = 0.80
    tp1_ratio = min(max(tp1_ratio, 0.01), 1.0)
    tp1 = entry + tp1_ratio * (tp2 - entry)
    protected = entry + 0.50 * (tp2 - entry)

    return {
        "ok": True,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "protected_sl": protected,
        "rr": rr,
        "risk_distance": risk_distance,
        "sl_distance_points": sl_points,
        "minimum_sl_distance_points": required_points,
        "minimum_rr": _number(minimum_rr) or 1.20,
        "maximum_rr": _number(maximum_rr) or 2.00,
        "tp1_ratio": tp1_ratio,
        "risk_plan_source": "V2_ACTUAL_ENTRY_CONFIGURED_RR",
    }


def install_v2_risk_alignment():
    """Patch only V2 shadow calculation globals; no execution behavior changes."""
    global _INSTALLED
    if _INSTALLED:
        return False

    from services import v2_shadow_service as v2

    original_build_context = v2.build_context
    original_decide_v2 = v2.decide_v2

    def aligned_build_context(symbol, result, data_5m, data_15m, now=None):
        context = original_build_context(symbol, result, data_5m, data_15m, now=now)
        minimum_points = _minimum_sl_points()
        entry = _number(context.get("entry"))
        sl = _number(context.get("sl"))
        sl_points = (
            abs(entry - sl) / _point_size(symbol)
            if entry is not None and sl is not None
            else None
        )
        bos_time = context.get("bos_timestamp")
        m5_time = context.get("m5_confirmation_timestamp")
        structure_type = str(context.get("structure_type") or "").upper()
        structure_ready = bool(
            context.get("direction") in {"BUY", "SELL"}
            and structure_type in {"BOS", "CHOCH"}
            and context.get("bos_level") is not None
            and bos_time is not None
        )
        confirmation_ready = bool(m5_time and bos_time and m5_time > bos_time)
        sl_valid = bool(sl_points is not None and sl_points + 1e-9 >= minimum_points)
        context.update({
            "minimum_sl_distance_points": minimum_points,
            "sl_distance_points": sl_points,
            "sl_distance_valid": sl_valid,
            "smc_structure_ready": structure_ready,
            "m5_confirmation_ready": confirmation_ready,
            # V2 will rebuild TP1/TP2 from its actual hypothetical entry, so
            # inherited V1 TP values are not required to prove the risk plan.
            "risk_valid": bool(entry is not None and sl is not None and sl_valid),
            "v2_risk_alignment": "CONFIGURED_SL_AND_RR",
        })
        return context

    def aligned_retest_decision(context, state, pending):
        latest = context.get("latest_5m")
        expiry = v2._utc(pending.get("expiry"))
        if context.get("latest_m15_close") and expiry and context["latest_m15_close"] > expiry:
            state["pending_setup"] = None
            return "EXPIRED_NO_RETEST", "SETUP_EXPIRED_AFTER_4_M15_CANDLES", None
        if not latest or latest.get("close") is None:
            return "WAIT_RETEST", "WAIT_CLOSED_M5_DATA", None
        atr = v2._number(pending.get("atr14"))
        level = v2._number(pending.get("bos_level"))
        if not atr or level is None:
            state["pending_setup"] = None
            return "BLOCK", "INVALID_RETEST_REFERENCE", None
        close_time = latest["close_time"]
        initial_confirmation = v2._utc(pending.get("initial_confirmation"))
        if initial_confirmation and close_time <= initial_confirmation:
            return "WAIT_RETEST", "WAIT_M5_AFTER_INITIAL_CONFIRMATION", None
        if not pending.get("retest_timestamp"):
            if abs(latest["close"] - level) <= v2.RETEST_DISTANCE_ATR * atr:
                pending["retest_timestamp"] = close_time.isoformat()
                state["pending_setup"] = pending
                return "WAIT_CONTINUATION", "BOS_RETEST_CONFIRMED", None
            return "WAIT_RETEST", "WAIT_BOS_RETEST", None
        retest_time = v2._utc(pending["retest_timestamp"])
        if close_time <= retest_time:
            return "WAIT_CONTINUATION", "WAIT_LATER_M5_CONTINUATION", None
        direction = str(pending.get("direction") or "").upper()
        buffer_value = v2._number(pending.get("bos_buffer")) or 0.0
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
        if extension > v2.EXTENSION_LIMIT_ATR:
            return "WAIT_EXTENDED", "CONTINUATION_STILL_EXTENDED", None

        plan = build_v2_risk_plan(
            context.get("symbol"),
            direction,
            latest["close"],
            pending.get("sl"),
            pending.get("minimum_rr", context.get("minimum_rr")),
            pending.get("maximum_rr", context.get("maximum_rr")),
            source_rr=pending.get("source_rr"),
            minimum_sl_points=context.get("minimum_sl_distance_points"),
        )
        if not plan.get("ok"):
            return "BLOCK", plan.get("reason") or "RETEST_RISK_PLAN_INVALID", None

        pending["continuation_timestamp"] = close_time.isoformat()
        state["pending_setup"] = None
        trade = {
            "setup_fingerprint": pending["setup_fingerprint"],
            "direction": direction,
            "entry_timestamp": close_time,
            "entry": plan["entry"],
            "sl": plan["sl"],
            "tp1": plan["tp1"],
            "tp2": plan["tp2"],
            "protected_sl": plan["protected_sl"],
            "risk_percent": pending.get("risk_percent"),
            "rr": plan["rr"],
            "retest_timestamp": retest_time,
            "continuation_timestamp": close_time,
            "extension_atr": extension,
            "sl_distance_points": plan["sl_distance_points"],
            "minimum_sl_distance_points": plan["minimum_sl_distance_points"],
            "risk_plan_source": plan["risk_plan_source"],
        }
        return f"{direction}_READY", "RETEST_AND_FRESH_M5_CONFIRMED", trade

    def aligned_decide_v2(context, state):
        decision, reason, next_state, trade = original_decide_v2(context, state)
        if not trade:
            return decision, reason, next_state, trade
        plan = build_v2_risk_plan(
            context.get("symbol"),
            trade.get("direction") or context.get("direction"),
            trade.get("entry"),
            trade.get("sl"),
            context.get("minimum_rr"),
            context.get("maximum_rr"),
            source_rr=context.get("rr"),
            minimum_sl_points=context.get("minimum_sl_distance_points"),
        )
        if not plan.get("ok"):
            return "BLOCK", plan.get("reason") or "V2_RISK_PLAN_INVALID", next_state, None
        trade.update({
            "entry": plan["entry"],
            "sl": plan["sl"],
            "tp1": plan["tp1"],
            "tp2": plan["tp2"],
            "protected_sl": plan["protected_sl"],
            "rr": plan["rr"],
            "sl_distance_points": plan["sl_distance_points"],
            "minimum_sl_distance_points": plan["minimum_sl_distance_points"],
            "risk_plan_source": plan["risk_plan_source"],
        })
        return decision, reason, next_state, trade

    v2.build_context = aligned_build_context
    v2._retest_decision = aligned_retest_decision
    v2.decide_v2 = aligned_decide_v2
    v2.build_v2_risk_plan = build_v2_risk_plan
    v2.V2_RISK_ALIGNMENT_INSTALLED = True
    _INSTALLED = True
    return True
