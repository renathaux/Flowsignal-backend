"""XAUUSD-specific risk-level hardening.

The strict strategy still owns entries, RR rules, TP selection and execution.
This module changes only the source of XAUUSD stop-loss structure: the stop is
anchored to a confirmed 5-minute pivot, never manufactured from the minimum
SL-distance setting.
"""
from __future__ import annotations

import math

import pandas as pd

from indicators.smc.engine import detect_confirmed_swings
from . import shared
from . import strict_trader


def _closed_5m(data_5m):
    return strict_trader.closed_frame(data_5m, 5)


def _entry_cutoff_position(data_5m, entry, setup_break_time):
    """Locate the confirmation candle whose close became the entry price."""
    if data_5m is None or data_5m.empty:
        return None
    anchor = strict_trader.utc_timestamp(
        strict_trader.candle_close_time(setup_break_time, 15)
        if setup_break_time
        else None
    )
    tolerance = max(strict_trader.point_size("XAUUSD") * 0.5, 1e-9)
    matches = []
    for position, (index_value, row) in enumerate(data_5m.iterrows()):
        try:
            ts = pd.Timestamp(index_value)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            close_time = ts + pd.Timedelta(minutes=5)
            close = float(row["Close"])
        except Exception:
            continue
        if anchor is not None and close_time <= anchor:
            continue
        if math.isclose(close, float(entry), rel_tol=0.0, abs_tol=tolerance):
            matches.append(position)
    return matches[0] if matches else len(data_5m) - 1


def _choose_5m_stop(data_5m, side, entry, configured_minimum_sl_points):
    closed = _closed_5m(data_5m)
    if closed is None or len(closed) < 5:
        return {
            "ok": False,
            "reason": "WAIT_NO_CONFIRMED_5M_SWING_SL",
            "sl_structure_source": "confirmed_5m_swing",
        }

    pivots = detect_confirmed_swings(closed, left_bars=2, right_bars=2)
    required_type = "LOW" if side == "BUY" else "HIGH"
    candidates = [pivot for pivot in pivots if pivot.swing_type == required_type]
    if not candidates:
        return {
            "ok": False,
            "reason": "WAIT_NO_CONFIRMED_5M_SWING_SL",
            "sl_structure_source": "confirmed_5m_swing",
        }

    buffer = strict_trader.sl_buffer("XAUUSD")
    minimum = strict_trader.minimum_sl_distance(
        "XAUUSD",
        configured_minimum_sl_points,
    )
    point = strict_trader.point_size("XAUUSD")
    rejected = []

    for pivot in reversed(candidates):
        swing_price = float(pivot.price)
        stop = swing_price - buffer if side == "BUY" else swing_price + buffer
        distance = abs(float(entry) - stop)
        side_ok = stop < float(entry) if side == "BUY" else stop > float(entry)
        if side_ok and distance >= minimum:
            return {
                "ok": True,
                "stop_loss": stop,
                "distance": distance,
                "distance_points": distance / point,
                "buffer": buffer,
                "swing": {
                    "type": pivot.swing_type,
                    "price": swing_price,
                    "time": pivot.timestamp,
                    "confirmed_time": pivot.confirmed_timestamp,
                    "index": int(pivot.index),
                    "confirmed_index": int(pivot.confirmed_index),
                },
                "sl_structure_source": "confirmed_5m_swing",
                "rejected_5m_sl_candidates": rejected,
            }
        rejected.append({
            "price": swing_price,
            "time": pivot.timestamp,
            "distance_points": distance / point,
            "reason": (
                "wrong_side"
                if not side_ok
                else "below_minimum_sl_distance"
            ),
        })

    return {
        "ok": False,
        "reason": "WAIT_NO_SAFE_5M_SWING_SL",
        "minimum_distance": minimum,
        "buffer": buffer,
        "sl_structure_source": "confirmed_5m_swing",
        "rejected_5m_sl_candidates": rejected,
    }


def build_xauusd_risk_levels(
    data_5m,
    data_15m,
    side,
    entry,
    symbol,
    *,
    setup_break_time=None,
    execution_settings=None,
):
    """Build XAUUSD levels with 5m swing SL and existing 15m TP/RR rules."""
    if shared.normalize_symbol(symbol) != "XAUUSD":
        raise ValueError("build_xauusd_risk_levels is XAUUSD-only")

    configured = execution_settings or strict_trader.get_cached_execution_settings()
    configured_minimum_sl_points = configured.get(
        "minimum_sl_distance_points",
        strict_trader.MIN_SL_POINTS,
    )

    closed = _closed_5m(data_5m)
    cutoff = _entry_cutoff_position(closed, entry, setup_break_time)
    if closed is None or closed.empty or cutoff is None:
        return {
            "ok": False,
            "reason": "WAIT_NO_CONFIRMED_5M_SWING_SL",
            "sl_structure_source": "confirmed_5m_swing",
        }
    stop_source = closed.iloc[: cutoff + 1].copy()
    stop = _choose_5m_stop(
        stop_source,
        side,
        float(entry),
        configured_minimum_sl_points,
    )
    if not stop.get("ok"):
        return stop

    swing_source = data_15m.copy()
    setup_timestamp = strict_trader.utc_timestamp(setup_break_time)
    if setup_timestamp is not None:
        try:
            source_index = pd.DatetimeIndex(swing_source.index)
            if source_index.tz is None:
                source_index = source_index.tz_localize("UTC")
            else:
                source_index = source_index.tz_convert("UTC")
            swing_source = swing_source.loc[source_index <= setup_timestamp]
        except Exception:
            return {"ok": False, "reason": "WAIT_NO_VALID_TP_STRUCTURE"}
    else:
        swing_source = swing_source.iloc[:-1].copy()

    swings_15m = strict_trader.detect_valid_swings(swing_source, symbol)
    risk = float(stop["distance"])
    tp2 = strict_trader.select_tp2(
        swings_15m,
        side,
        float(entry),
        risk,
        symbol,
        minimum_rr=configured.get("minimum_rr"),
        maximum_rr=configured.get("maximum_rr"),
    )
    tp2_price = float(tp2["tp2"])
    tp1_ratio = shared.get_tp1_ratio_of_tp2()
    protected_fraction = strict_trader.PROTECTED_SL_TP2_FRACTION
    if side == "BUY":
        tp1 = float(entry) + ((tp2_price - float(entry)) * tp1_ratio)
        protected = float(entry) + ((tp2_price - float(entry)) * protected_fraction)
    else:
        tp1 = float(entry) - ((float(entry) - tp2_price) * tp1_ratio)
        protected = float(entry) - ((float(entry) - tp2_price) * protected_fraction)

    dec = strict_trader.decimals(symbol)
    return {
        "ok": True,
        "entry": round(float(entry), dec),
        "stop_loss": round(float(stop["stop_loss"]), dec),
        "tp1": round(tp1, dec),
        "tp2": round(tp2_price, dec),
        "protected_sl_price": round(protected, dec),
        "risk": round(risk, dec),
        "reward": round(abs(tp2_price - float(entry)), dec),
        "risk_reward_ratio": round(float(tp2["rr"]), 4),
        "risk_reward": f"1:{round(float(tp2['rr']), 2):g}",
        "sl_buffer": round(float(stop["buffer"]), dec),
        "sl_buffer_points": strict_trader.SL_BUFFER_POINTS,
        "minimum_sl_points": int(configured_minimum_sl_points),
        "sl_distance_points": round(float(stop["distance_points"]), 2),
        "sl_swing_used": round(float(stop["swing"]["price"]), dec),
        "sl_swing_time": stop["swing"].get("time"),
        "sl_swing_confirmed_time": stop["swing"].get("confirmed_time"),
        "sl_structure_source": "confirmed_5m_swing",
        "rejected_5m_sl_candidates": stop.get("rejected_5m_sl_candidates", []),
        "tp_structure_used": (
            round(float(tp2["swing"]["price"]), dec)
            if tp2.get("swing")
            else None
        ),
        "tp_structure_source": tp2["source"],
        "rejected_tp_candidates": tp2.get("rejected_tp_candidates", []),
        "tp1_rule": "80% of entry-to-TP2 unless admin overrides it",
        "protected_sl_rule": "50% of entry-to-TP2 after TP1 wick touch",
    }
