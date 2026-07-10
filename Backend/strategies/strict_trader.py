import math
from datetime import datetime, timezone

import pandas as pd

from . import shared


MIN_SWING_POINTS = 100
SL_BUFFER_POINTS = 50
MIN_SL_POINTS = 100
MIN_RR = 1.20
MAX_RR = 2.00
FALLBACK_RR = 2.00


def point_size(symbol):
    try:
        size = 10 ** (-decimals(symbol))
    except (TypeError, ValueError, OverflowError):
        try:
            size = float(shared.get_strategy_pip_size(symbol))
        except (TypeError, ValueError):
            size = 0.0001
    return size if math.isfinite(size) and size > 0 else 0.0001


def decimals(symbol):
    try:
        value = int(shared.get_strategy_decimals(symbol))
    except (TypeError, ValueError):
        value = 5
    return value


def minimum_swing_size(symbol):
    return MIN_SWING_POINTS * point_size(symbol)


def sl_buffer(symbol):
    return SL_BUFFER_POINTS * point_size(symbol)


def minimum_sl_distance(symbol):
    return MIN_SL_POINTS * point_size(symbol)


def closed_frame(data, minutes):
    return shared.remove_current_forming_candle(data, minutes)


def candle_time(index_value):
    try:
        return pd.Timestamp(index_value).isoformat()
    except Exception:
        return None


def trend_filter(data_15m, symbol):
    if data_15m is None or data_15m.empty or len(data_15m) < 21:
        return {
            "trend": "NEUTRAL",
            "buy_allowed": False,
            "sell_allowed": False,
            "reason": "WAIT_NO_15M_TREND",
        }

    close = data_15m["Close"].astype(float)
    ema_fast = close.ewm(span=9, adjust=False).mean().iloc[-1]
    ema_slow = close.ewm(span=21, adjust=False).mean().iloc[-1]
    last_close = close.iloc[-1]

    bullish = last_close > ema_slow and ema_fast > ema_slow
    bearish = last_close < ema_slow and ema_fast < ema_slow

    return {
        "trend": "BULLISH" if bullish else "BEARISH" if bearish else "NEUTRAL",
        "buy_allowed": bullish,
        "sell_allowed": bearish,
        "ema_fast": round(float(ema_fast), decimals(symbol)),
        "ema_slow": round(float(ema_slow), decimals(symbol)),
        "close": round(float(last_close), decimals(symbol)),
        "reason": None if bullish or bearish else "WAIT_EMA_NEUTRAL",
    }


def detect_valid_swings(data_15m, symbol, left=2, right=2):
    if data_15m is None or len(data_15m) < left + right + 3:
        return []

    min_size = minimum_swing_size(symbol)
    highs = data_15m["High"].astype(float).tolist()
    lows = data_15m["Low"].astype(float).tolist()
    index = list(data_15m.index)
    raw = []

    for pos in range(left, len(data_15m) - right):
        high_window = highs[pos - left:pos + right + 1]
        low_window = lows[pos - left:pos + right + 1]
        high = highs[pos]
        low = lows[pos]

        if high == max(high_window) and high > max(highs[pos - left:pos] + highs[pos + 1:pos + right + 1]):
            raw.append({
                "type": "HIGH",
                "price": high,
                "index": pos,
                "time": candle_time(index[pos]),
            })

        if low == min(low_window) and low < min(lows[pos - left:pos] + lows[pos + 1:pos + right + 1]):
            raw.append({
                "type": "LOW",
                "price": low,
                "index": pos,
                "time": candle_time(index[pos]),
            })

    raw.sort(key=lambda item: item["index"])
    previous_by_type = {"HIGH": None, "LOW": None}

    for swing in raw:
        opposite_type = "LOW" if swing["type"] == "HIGH" else "HIGH"
        opposite = previous_by_type.get(opposite_type)
        valid_size = None

        if opposite:
            valid_size = abs(float(swing["price"]) - float(opposite["price"]))
        else:
            row = data_15m.iloc[swing["index"]]
            valid_size = abs(float(row["High"]) - float(row["Low"]))

        swing["swing_size"] = valid_size
        swing["valid"] = valid_size >= min_size
        swing["valid_reason"] = (
            "single_100_point_swing"
            if swing["valid"]
            else None
        )

        previous_by_type[swing["type"]] = swing

    composite_indexes = set()
    for end in range(len(raw)):
        prefix = raw[:end + 1]
        prefix_highs = [s for s in prefix if s.get("type") == "HIGH"]
        prefix_lows = [s for s in prefix if s.get("type") == "LOW"]
        if len(prefix_highs) < 2 or len(prefix_lows) < 2:
            continue

        previous_high, last_high = prefix_highs[-2], prefix_highs[-1]
        previous_low, last_low = prefix_lows[-2], prefix_lows[-1]
        hh = float(last_high["price"]) > float(previous_high["price"])
        hl = float(last_low["price"]) > float(previous_low["price"])
        lh = float(last_high["price"]) < float(previous_high["price"])
        ll = float(last_low["price"]) < float(previous_low["price"])

        bullish_composite_size = (
            abs(float(last_high["price"]) - float(previous_high["price"]))
            + abs(float(last_low["price"]) - float(previous_low["price"]))
        )
        bearish_composite_size = (
            abs(float(previous_high["price"]) - float(last_high["price"]))
            + abs(float(previous_low["price"]) - float(last_low["price"]))
        )

        if hh and hl and bullish_composite_size >= min_size:
            for swing in [previous_high, last_high, previous_low, last_low]:
                swing["valid"] = True
                swing["valid_reason"] = "composite_hh_hl_100_point_structure"
                swing["composite_swing_size"] = bullish_composite_size
                composite_indexes.add(swing["index"])

        if lh and ll and bearish_composite_size >= min_size:
            for swing in [previous_high, last_high, previous_low, last_low]:
                swing["valid"] = True
                swing["valid_reason"] = "composite_lh_ll_100_point_structure"
                swing["composite_swing_size"] = bearish_composite_size
                composite_indexes.add(swing["index"])

    swings = []
    for swing in raw:
        if swing["valid"] or swing["index"] in composite_indexes:
            swings.append(swing)

    return swings


def latest_swing(swings, swing_type):
    candidates = [s for s in swings if s.get("type") == swing_type]
    return candidates[-1] if candidates else None


def older_swings(swings, swing_type):
    return [s for s in swings if s.get("type") == swing_type]


def detect_swing_structure(swings):
    highs = [s for s in swings if s.get("type") == "HIGH"]
    lows = [s for s in swings if s.get("type") == "LOW"]
    structure = {
        "pattern": "NEUTRAL",
        "bias": "NEUTRAL",
        "hh": False,
        "hl": False,
        "lh": False,
        "ll": False,
        "previous_high": None,
        "last_high": None,
        "previous_low": None,
        "last_low": None,
        "reason": "WAIT_NEED_TWO_VALID_HIGHS_AND_LOWS",
    }
    if len(highs) < 2 or len(lows) < 2:
        return structure

    previous_high = highs[-2]
    last_high = highs[-1]
    previous_low = lows[-2]
    last_low = lows[-1]
    hh = float(last_high["price"]) > float(previous_high["price"])
    hl = float(last_low["price"]) > float(previous_low["price"])
    lh = float(last_high["price"]) < float(previous_high["price"])
    ll = float(last_low["price"]) < float(previous_low["price"])

    structure.update({
        "hh": hh,
        "hl": hl,
        "lh": lh,
        "ll": ll,
        "previous_high": previous_high,
        "last_high": last_high,
        "previous_low": previous_low,
        "last_low": last_low,
        "reason": "WAIT_NO_CLEAR_HH_HL_OR_LH_LL_STRUCTURE",
    })
    if hh and hl:
        structure.update({
            "pattern": "HH_HL",
            "bias": "BULLISH",
            "reason": "BULLISH_HH_HL_CONFIRMED",
        })
    elif lh and ll:
        structure.update({
            "pattern": "LH_LL",
            "bias": "BEARISH",
            "reason": "BEARISH_LH_LL_CONFIRMED",
        })
    return structure


def get_watch_key(symbol, side):
    return shared.get_15m_swing_watch_key(symbol, side)


def clear_opposite_watch(symbol, side, reason):
    opposite = "SELL" if side == "BUY" else "BUY"
    key = get_watch_key(symbol, opposite)
    if key in shared.FIFTEEN_M_SWING_WATCH:
        shared.FIFTEEN_M_SWING_WATCH.pop(key, None)
        shared.save_fifteen_m_swing_watch()
        return True
    return False


def save_remembered_breakout(symbol, side, level, break_time, break_close, reason):
    key = get_watch_key(symbol, side)
    shared.FIFTEEN_M_SWING_WATCH[key] = {
        "symbol": shared.normalize_symbol(symbol),
        "side": side,
        "swing_level": float(level),
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "break_confirmed": True,
        "break_candle_time": break_time,
        "break_close": float(break_close),
        "status": "PENDING",
        "reason": reason,
    }
    shared.save_fifteen_m_swing_watch()


def remembered_breakout(symbol, side):
    watch = shared.FIFTEEN_M_SWING_WATCH.get(get_watch_key(symbol, side))
    if not isinstance(watch, dict):
        return None
    if str(watch.get("status") or "PENDING").upper() != "PENDING":
        return None
    try:
        return {
            "side": side,
            "level": float(watch.get("swing_level")),
            "break_time": watch.get("break_candle_time"),
            "break_close": float(watch.get("break_close")),
            "remembered": True,
            "watch": watch,
        }
    except (TypeError, ValueError):
        return None


def evaluate_15m_breakout(data_15m, symbol):
    result = {
        "side": "WAIT",
        "level": None,
        "break_time": None,
        "break_close": None,
        "remembered": False,
        "reason": "WAIT_NO_15M_BREAK",
        "swings": [],
        "structure": {},
        "breakouts": [],
    }

    if data_15m is None or len(data_15m) < 10:
        result["reason"] = "WAIT_NOT_ENOUGH_15M_DATA"
        return result

    swing_source = data_15m.iloc[:-1].copy()
    swings = detect_valid_swings(swing_source, symbol)
    result["swings"] = swings

    if not swings:
        result["reason"] = "WAIT_NO_VALID_100_POINT_SWING"
        return result

    structure = detect_swing_structure(swings)
    result["structure"] = structure
    bullish_structure = structure.get("pattern") == "HH_HL"
    bearish_structure = structure.get("pattern") == "LH_LL"

    last = data_15m.iloc[-1]
    previous = data_15m.iloc[-2]
    last_close = float(last["Close"])
    last_high = float(last["High"])
    last_low = float(last["Low"])
    previous_close = float(previous["Close"])
    break_time = candle_time(data_15m.index[-1])

    high_swing = latest_swing(swings, "HIGH")
    low_swing = latest_swing(swings, "LOW")
    candidates = []

    if high_swing and bullish_structure:
        level = float(high_swing["price"])
        confirmed = last_high > level and last_close > level and previous_close <= level
        if confirmed:
            candidates.append({
                "side": "BUY",
                "level": level,
                "break_time": break_time,
                "break_close": last_close,
                "swing": high_swing,
                "structure": structure,
                "remembered": False,
            })

    if low_swing and bearish_structure:
        level = float(low_swing["price"])
        confirmed = last_low < level and last_close < level and previous_close >= level
        if confirmed:
            candidates.append({
                "side": "SELL",
                "level": level,
                "break_time": break_time,
                "break_close": last_close,
                "swing": low_swing,
                "structure": structure,
                "remembered": False,
            })

    if not candidates:
        for side in ["BUY", "SELL"]:
            remembered = remembered_breakout(symbol, side)
            if not remembered:
                continue
            candidates.append({
                **remembered,
                "structure": structure,
            })

    result["breakouts"] = candidates
    if not candidates:
        raw_buy_break = bool(
            high_swing
            and last_high > float(high_swing["price"])
            and last_close > float(high_swing["price"])
            and previous_close <= float(high_swing["price"])
        )
        raw_sell_break = bool(
            low_swing
            and last_low < float(low_swing["price"])
            and last_close < float(low_swing["price"])
            and previous_close >= float(low_swing["price"])
        )
        if raw_buy_break and not bullish_structure:
            result["reason"] = "WAIT_NO_HH_HL_STRUCTURE"
        elif raw_sell_break and not bearish_structure:
            result["reason"] = "WAIT_NO_LH_LL_STRUCTURE"
        return result

    chosen = candidates[-1]
    clear_opposite_watch(symbol, chosen["side"], "opposite_15m_breakout")
    result.update({
        "side": chosen["side"],
        "level": chosen["level"],
        "break_time": chosen["break_time"],
        "break_close": chosen["break_close"],
        "remembered": bool(chosen.get("remembered")),
        "swing": chosen.get("swing"),
        "structure": chosen.get("structure") or structure,
        "reason": "15M_SWING_BREAK_CLOSED",
    })
    return result


def confirm_5m(data_5m, side, level, break_time):
    base = {
        "side": "WAIT",
        "close_confirmed": False,
        "closed_candle_time": None,
        "reason": "WAIT_NO_5M_CLOSE_CONFIRMATION",
    }
    if side not in ["BUY", "SELL"] or level is None or not break_time:
        return base

    closed = closed_frame(data_5m, 5)
    if closed is None or closed.empty:
        return base

    try:
        anchor = pd.Timestamp(break_time)
        if anchor.tzinfo is None:
            anchor = anchor.tz_localize("UTC")
        else:
            anchor = anchor.tz_convert("UTC")
    except Exception:
        return base

    for candle_index, candle in closed.iterrows():
        try:
            ts = pd.Timestamp(candle_index)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            if ts <= anchor:
                continue
            open_price = float(candle["Open"])
            close_price = float(candle["Close"])
            high_price = float(candle["High"])
            low_price = float(candle["Low"])
        except Exception:
            continue

        passed = (
            side == "BUY"
            and close_price > open_price
            and close_price > float(level)
        ) or (
            side == "SELL"
            and close_price < open_price
            and close_price < float(level)
        )
        if passed:
            return {
                "side": side,
                "close_confirmed": True,
                "closed_candle_time": ts.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "setup_level": float(level),
                "reason": "5M_CLOSE_CONFIRMED",
            }

    return base


def select_stop_loss(swings, side, entry, symbol):
    required = "LOW" if side == "BUY" else "HIGH"
    buffer = sl_buffer(symbol)
    minimum = minimum_sl_distance(symbol)
    candidates = older_swings(swings, required)

    for swing in reversed(candidates):
        swing_price = float(swing["price"])
        stop = swing_price - buffer if side == "BUY" else swing_price + buffer
        distance = abs(float(entry) - stop)
        side_ok = stop < entry if side == "BUY" else stop > entry
        if side_ok and distance >= minimum:
            return {
                "ok": True,
                "stop_loss": stop,
                "swing": swing,
                "distance": distance,
                "distance_points": distance / point_size(symbol),
                "buffer": buffer,
            }

    return {
        "ok": False,
        "reason": "WAIT_SL_SMALLER_THAN_100_POINTS",
        "minimum_distance": minimum,
        "buffer": buffer,
    }


def select_tp2(swings, side, entry, risk, symbol):
    inverse = "HIGH" if side == "BUY" else "LOW"
    candidates = [
        swing
        for swing in older_swings(swings, inverse)
        if (
            float(swing["price"]) > entry
            if side == "BUY"
            else float(swing["price"]) < entry
        )
    ]
    candidates.sort(
        key=lambda swing: abs(float(swing["price"]) - float(entry))
    )
    rejected = []

    for swing in candidates:
        price = float(swing["price"])
        reward = price - entry if side == "BUY" else entry - price
        if reward <= 0:
            continue
        rr = reward / risk
        if MIN_RR <= rr <= MAX_RR:
            return {
                "tp2": price,
                "rr": rr,
                "source": "inverse_15m_swing",
                "swing": swing,
                "rejected_tp_candidates": rejected,
            }
        rejected.append({
            "price": price,
            "rr": rr,
            "reason": (
                "TP swing reward below 1.20R"
                if rr < MIN_RR
                else "TP swing reward above 2.00R"
            ),
            "swing": swing,
        })

    tp2 = entry + (risk * FALLBACK_RR) if side == "BUY" else entry - (risk * FALLBACK_RR)
    return {
        "tp2": tp2,
        "rr": FALLBACK_RR,
        "source": "fallback_2r",
        "swing": None,
        "rejected_tp_candidates": rejected,
    }


def build_risk_levels(data_15m, side, entry, symbol):
    dec = decimals(symbol)
    swings = detect_valid_swings(data_15m.iloc[:-1].copy(), symbol)
    stop = select_stop_loss(swings, side, float(entry), symbol)
    if not stop.get("ok"):
        return {**stop, "ok": False}

    risk = float(stop["distance"])
    tp2 = select_tp2(swings, side, float(entry), risk, symbol)
    tp2_price = float(tp2["tp2"])
    tp1_ratio = shared.get_tp1_ratio_of_tp2()
    if side == "BUY":
        tp1 = float(entry) + ((tp2_price - float(entry)) * tp1_ratio)
        protected = float(entry) + ((tp2_price - float(entry)) * 0.50)
    else:
        tp1 = float(entry) - ((float(entry) - tp2_price) * tp1_ratio)
        protected = float(entry) - ((float(entry) - tp2_price) * 0.50)

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
        "sl_buffer": round(stop["buffer"], dec),
        "sl_buffer_points": SL_BUFFER_POINTS,
        "minimum_sl_points": MIN_SL_POINTS,
        "sl_distance_points": round(stop["distance_points"], 2),
        "sl_swing_used": round(float(stop["swing"]["price"]), dec),
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


def bias_scores_from_context(trend=None, breakout=None, confirmation=None):
    buy_pct = 50
    sell_pct = 50
    confidence = 20

    trend_side = str((trend or {}).get("trend") or "NEUTRAL").upper()
    if trend_side == "BULLISH":
        buy_pct, sell_pct, confidence = 60, 40, 35
    elif trend_side == "BEARISH":
        buy_pct, sell_pct, confidence = 40, 60, 35

    breakout_side = str((breakout or {}).get("side") or "WAIT").upper()
    if breakout_side == "BUY":
        buy_pct, sell_pct, confidence = 70, 30, 55
    elif breakout_side == "SELL":
        buy_pct, sell_pct, confidence = 30, 70, 55

    confirmation_side = str((confirmation or {}).get("side") or "WAIT").upper()
    if confirmation_side == "BUY":
        buy_pct, sell_pct, confidence = 78, 22, 65
    elif confirmation_side == "SELL":
        buy_pct, sell_pct, confidence = 22, 78, 65

    return {
        "buy_pct": buy_pct,
        "sell_pct": sell_pct,
        "confidence": confidence,
        "bias_source": "closed_15m_structure",
        "bias_note": "Market bias only; entry still requires strict setup checks.",
    }


def wait_result(symbol, reason, extra=None):
    normalized_symbol = shared.normalize_symbol(symbol)
    payload = {
        "symbol": normalized_symbol,
        "signal": "WAIT",
        "final_signal": "WAIT",
        "signal_before_filters": "WAIT",
        "signal_after_filters": "WAIT",
        "signal_text": "WAIT",
        "buy_pct": 0,
        "sell_pct": 0,
        "confidence": 0,
        "strategy_model": "strict_15m_trader",
        "strategy_setup_timeframe": "15m",
        "strategy_confirmation_timeframe": "5m",
        "strategy_trend_timeframe": "15m EMA",
        "setup_timeframe_used": "15m",
        "final_signal_source": "strict_trader",
        "fifteen_m_setup": "WAIT",
        "fifteen_m_uses_closed_candle_only": True,
        "fifteen_m_forming_candle_removed": False,
        "confirmation_5m": "WAIT",
        "confirmation_5m_raw": "WAIT",
        "current_5m_entry_confirmation": False,
        "strategy_setup_complete": False,
        "blocked_by": reason,
        "blocked_reason": reason,
        "block_reason": reason,
        "blocker_rule_name": reason,
        "debug_reasons": [reason],
    }
    if extra:
        payload.update(extra)

    breakout = payload.get("fifteen_m_swing_break")
    confirmation = payload.get("confirmation_5m")
    trend = payload.get("trend_15m") or {}
    structure = breakout.get("structure") if isinstance(breakout, dict) else {}
    if not isinstance(structure, dict):
        structure = {}
    setup_side = str((breakout or {}).get("side") or payload.get("fifteen_m_setup") or "WAIT").upper()
    if setup_side not in ["BUY", "SELL"]:
        setup_side = "WAIT"

    diagnostics = {
        "symbol": normalized_symbol,
        "final_signal": "WAIT",
        "signal_before_filters": payload.get("signal_before_filters") or setup_side,
        "signal_after_filters": "WAIT",
        "blocked": True,
        "blocked_by": payload.get("blocked_by"),
        "blocked_reason": payload.get("blocked_reason"),
        "block_reason": payload.get("blocked_reason"),
        "fifteen_m_setup": setup_side,
        "fifteen_m_swing_break": bool(
            isinstance(breakout, dict) and breakout.get("side") in ["BUY", "SELL"]
        ),
        "fifteen_m_swing_break_confirmed": bool(
            isinstance(breakout, dict) and breakout.get("side") in ["BUY", "SELL"]
        ),
        "fifteen_m_close_confirmed": bool(
            isinstance(breakout, dict) and breakout.get("side") in ["BUY", "SELL"]
        ),
        "fifteen_m_structure_pattern": structure.get("pattern"),
        "fifteen_m_structure_bias": structure.get("bias"),
        "hh_hl_confirmed": structure.get("pattern") == "HH_HL",
        "lh_ll_confirmed": structure.get("pattern") == "LH_LL",
        "fifteen_m_break_level": (
            breakout.get("level")
            if isinstance(breakout, dict)
            else None
        ),
        "fifteen_m_break_time": (
            breakout.get("break_time")
            if isinstance(breakout, dict)
            else None
        ),
        "five_m_confirmation": bool(
            isinstance(confirmation, dict) and confirmation.get("close_confirmed")
        ),
        "five_m_signal": (
            confirmation.get("side")
            if isinstance(confirmation, dict)
            else "WAIT"
        ),
        "trend_bias": str(trend.get("trend") or "NEUTRAL").lower(),
        "entry": payload.get("entry_price"),
        "sl": payload.get("stop_loss"),
        "tp1": payload.get("tp1"),
        "tp2": payload.get("tp2"),
    }
    payload["signal_diagnostics"] = diagnostics
    payload["entry_strategy_debug"] = diagnostics.copy()
    payload["strategy_debug"] = diagnostics.copy()
    return payload


def get_mtf_signal(data_5m, data_15m, data_1h, symbol):
    normalized_symbol = shared.normalize_symbol(symbol)
    closed_15m = closed_frame(data_15m, 15)
    closed_5m = closed_frame(data_5m, 5)
    removed_forming_15m = (
        data_15m is not None
        and closed_15m is not None
        and len(data_15m) > len(closed_15m)
    )
    base_meta = {
        "fifteen_m_uses_closed_candle_only": True,
        "fifteen_m_forming_candle_removed": removed_forming_15m,
        "market_condition": "STRUCTURE",
    }

    if data_5m is not None and not data_5m.empty:
        try:
            base_meta["price"] = float(data_5m["Close"].iloc[-1])
        except (KeyError, TypeError, ValueError):
            pass

    if closed_15m is None or len(closed_15m) < 25:
        return wait_result(
            normalized_symbol,
            "WAIT_NOT_ENOUGH_15M_DATA",
            base_meta,
        )

    trend = trend_filter(closed_15m, normalized_symbol)
    breakout = evaluate_15m_breakout(closed_15m, normalized_symbol)
    base_meta = {
        **base_meta,
        **bias_scores_from_context(trend=trend, breakout=breakout),
    }

    if breakout["side"] not in ["BUY", "SELL"]:
        return wait_result(normalized_symbol, breakout["reason"], {
            **base_meta,
            "trend_15m": trend,
            "fifteen_m_swing_break": breakout,
        })

    side = breakout["side"]
    breakout_meta = {
        **base_meta,
        "fifteen_m_setup": side,
        "trend_15m": trend,
        "fifteen_m_swing_break": breakout,
    }

    five_m = confirm_5m(
        closed_5m,
        side,
        breakout["level"],
        breakout["break_time"],
    )
    setup_meta = {
        **breakout_meta,
        **bias_scores_from_context(
            trend=trend,
            breakout=breakout,
            confirmation=five_m,
        ),
        "confirmation_5m": five_m,
    }
    if not five_m.get("close_confirmed"):
        save_remembered_breakout(
            normalized_symbol,
            side,
            breakout["level"],
            breakout["break_time"],
            breakout["break_close"],
            "WAIT_NO_5M_CLOSE_CONFIRMATION",
        )
        return wait_result(normalized_symbol, "WAIT_NO_5M_CLOSE_CONFIRMATION", {
            **setup_meta,
            "remembered_breakout": True,
        })

    shared.FIFTEEN_M_SWING_WATCH.pop(get_watch_key(normalized_symbol, side), None)
    shared.save_fifteen_m_swing_watch()

    entry = five_m.get("close") or breakout["break_close"]
    levels = build_risk_levels(closed_15m, side, entry, normalized_symbol)
    if not levels.get("ok"):
        return wait_result(normalized_symbol, levels.get("reason") or "WAIT_INVALID_RISK_LEVELS", {
            **setup_meta,
            "swing_sl_debug": levels,
        })

    buy_pct = 85 if side == "BUY" else 15
    sell_pct = 85 if side == "SELL" else 15
    return {
        "symbol": normalized_symbol,
        "signal": side,
        "final_signal": side,
        "signal_before_filters": side,
        "signal_after_filters": side,
        "signal_text": f"{side} (15m swing + 5m close)",
        "buy_pct": buy_pct,
        "sell_pct": sell_pct,
        "confidence": 85,
        "market_condition": "STRUCTURE",
        "entry_quality": "STRICT",
        "entry_timing": "5M CLOSED CONFIRMATION",
        "strategy_model": "strict_15m_trader",
        "strategy_setup_timeframe": "15m",
        "strategy_confirmation_timeframe": "5m",
        "strategy_trend_timeframe": "15m EMA",
        "setup_timeframe_used": "15m",
        "final_signal_source": "strict_trader",
        "fifteen_m_setup": side,
        "fifteen_m_uses_closed_candle_only": True,
        "fifteen_m_forming_candle_removed": removed_forming_15m,
        "strategy_setup_complete": True,
        "strategy_setup_type": f"{side}_15M_SWING_BREAK_5M_CONFIRMED",
        "plan_type": f"STRICT {side}",
        "plan_reason": f"{side}: 15m HH/HL or LH/LL, closed swing break, and 5m close confirmed",
        "entry_price": levels["entry"],
        "price": levels["entry"],
        "stop_loss": levels["stop_loss"],
        "tp1": levels["tp1"],
        "tp2": levels["tp2"],
        "protected_sl_price": levels["protected_sl_price"],
        "risk_reward": levels["risk_reward"],
        "risk_reward_ratio": levels["risk_reward_ratio"],
        "trend_15m": trend,
        "fifteen_m_swing_break": breakout,
        "fifteen_m_swing_break_confirmed": True,
        "fifteen_m_swing_level": breakout["level"],
        "fifteen_m_structure_pattern": (breakout.get("structure") or {}).get("pattern"),
        "fifteen_m_structure_bias": (breakout.get("structure") or {}).get("bias"),
        "fifteen_m_closed_candle_close": breakout["break_close"],
        "confirmation_5m": five_m,
        "confirmation_5m_raw": five_m.get("side"),
        "current_5m_entry_confirmation": True,
        "swing_sl_debug": levels,
        "tp1_rule": levels["tp1_rule"],
        "tp2_rule": levels["tp_structure_source"],
        "protected_sl_rule": levels["protected_sl_rule"],
        "blocked_by": None,
        "blocked_reason": None,
        "block_reason": None,
        "blocker_rule_name": None,
        "signal_diagnostics": {
            "symbol": normalized_symbol,
            "final_signal": side,
            "signal_before_filters": side,
            "signal_after_filters": side,
            "blocked": False,
            "blocked_by": None,
            "blocked_reason": None,
            "block_reason": None,
            "fifteen_m_setup": side,
            "fifteen_m_swing_break": True,
            "fifteen_m_swing_break_confirmed": True,
            "fifteen_m_close_confirmed": True,
            "fifteen_m_structure_pattern": (breakout.get("structure") or {}).get("pattern"),
            "fifteen_m_structure_bias": (breakout.get("structure") or {}).get("bias"),
            "hh_hl_confirmed": (breakout.get("structure") or {}).get("pattern") == "HH_HL",
            "lh_ll_confirmed": (breakout.get("structure") or {}).get("pattern") == "LH_LL",
            "fifteen_m_break_level": breakout["level"],
            "fifteen_m_break_time": breakout["break_time"],
            "five_m_confirmation": True,
            "five_m_signal": side,
            "trend_bias": str(trend.get("trend") or "NEUTRAL").lower(),
            "entry": levels["entry"],
            "sl": levels["stop_loss"],
            "tp1": levels["tp1"],
            "tp2": levels["tp2"],
        },
        "debug_reasons": [
            "15m EMA supports trade",
            "15m candle closed beyond valid 100-point swing",
            "5m candle closed in same direction",
            "SL/TP built from strict swing rules",
        ],
    }


def update_trade_with_wick_management(trade, candle):
    if not isinstance(trade, dict) or not isinstance(candle, dict):
        return trade

    side = str(trade.get("side") or trade.get("action") or "").upper()
    if side not in ["BUY", "SELL"]:
        return trade

    updated = dict(trade)
    high = float(candle.get("high", candle.get("High")))
    low = float(candle.get("low", candle.get("Low")))
    tp1 = float(updated["tp1"])
    tp2 = float(updated["tp2"])
    sl = float(updated.get("sl", updated.get("stop_loss")))
    protected = float(
        updated.get("protected_sl_price")
        or build_protected_sl(updated["entry"], tp2, side)
    )

    def touched(price):
        return high >= price if side == "BUY" else low <= price

    def touched_sl(price):
        return low <= price if side == "BUY" else high >= price

    if touched_sl(sl) and not updated.get("hit_tp1"):
        updated.update({"status": "CLOSED", "result": "LOSS", "closed_price": sl})
        return updated

    if touched(tp2):
        updated.update({"status": "CLOSED", "result": "WIN", "closed_price": tp2})
        return updated

    if touched(tp1) and not updated.get("hit_tp1"):
        updated["hit_tp1"] = True
        updated["profit_protected"] = True
        updated["protected_sl_price"] = protected
        updated["sl"] = protected
        updated["result"] = "TP1 HIT"

    if updated.get("hit_tp1") and touched_sl(protected):
        updated.update({
            "status": "CLOSED",
            "result": "PROTECTED WIN",
            "closed_price": protected,
        })

    return updated


def build_protected_sl(entry, tp2, side):
    entry = float(entry)
    tp2 = float(tp2)
    if side == "BUY":
        return entry + ((tp2 - entry) * 0.50)
    return entry - ((entry - tp2) * 0.50)
