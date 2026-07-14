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
PULLBACK_MIN_POINTS = 30
BOS_MIN_BUFFER_POINTS = 10


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
        value = 2 if shared.normalize_symbol(symbol) == "XAUUSD" else 5
    return value


def minimum_swing_size(symbol):
    return MIN_SWING_POINTS * point_size(symbol)


def sl_buffer(symbol):
    return SL_BUFFER_POINTS * point_size(symbol)


def minimum_sl_distance(symbol):
    return MIN_SL_POINTS * point_size(symbol)


def atr14(data):
    if data is None or len(data) < 14:
        return None
    high = data["High"].astype(float)
    low = data["Low"].astype(float)
    close = data["Close"].astype(float)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    value = true_range.tail(14).mean()
    return float(value) if pd.notna(value) and value > 0 else None


def bos_buffer(data_15m, symbol):
    atr = atr14(data_15m) or 0.0
    return max(BOS_MIN_BUFFER_POINTS * point_size(symbol), 0.10 * atr)


def minimum_pullback_size(data_15m, symbol):
    atr = atr14(data_15m) or 0.0
    return max(PULLBACK_MIN_POINTS * point_size(symbol), 0.25 * atr)


def closed_frame(data, minutes):
    return shared.remove_current_forming_candle(data, minutes)


def candle_time(index_value):
    try:
        return pd.Timestamp(index_value).isoformat()
    except Exception:
        return None


def candle_close_time(index_value, minutes):
    try:
        timestamp = pd.Timestamp(index_value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        return (timestamp + pd.Timedelta(minutes=minutes)).isoformat()
    except Exception:
        return None


def utc_timestamp(value):
    try:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        return timestamp
    except Exception:
        return None


def last_position_closed_time(symbol):
    try:
        value = float(shared.LAST_POSITION_CLOSED_AT.get(shared.normalize_symbol(symbol), 0) or 0)
        return pd.Timestamp(value, unit="s", tz="UTC") if value > 0 else None
    except (AttributeError, TypeError, ValueError):
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


def detect_raw_swings(data_15m, symbol, left=2, right=2):
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

    return raw


def detect_valid_swings(data_15m, symbol, left=2, right=2):
    return [
        swing
        for swing in detect_raw_swings(data_15m, symbol, left=left, right=right)
        if swing.get("valid")
    ]


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


def validate_continuation_structure(raw_swings, data_15m, symbol):
    base = {
        "pattern": "NEUTRAL",
        "bias": "NEUTRAL",
        "reason": "WAIT_NO_CLEAR_HH_HL_OR_LH_LL_STRUCTURE",
        "completed_impulse_size": None,
        "completed_impulse_points": None,
        "pullback_size": None,
        "pullback_points": None,
        "minimum_pullback_size": minimum_pullback_size(data_15m, symbol),
        "minimum_impulse_size": minimum_swing_size(symbol),
        "sequence": [],
    }
    if len(raw_swings) < 4:
        return base

    sequence = raw_swings[-4:]
    types = [swing.get("type") for swing in sequence]
    base["sequence"] = sequence
    bullish_sequence = types == ["HIGH", "LOW", "HIGH", "LOW"]
    bearish_sequence = types == ["LOW", "HIGH", "LOW", "HIGH"]

    if bullish_sequence:
        previous_high, previous_low, last_high, last_low = sequence
        if float(last_high["price"]) <= float(previous_high["price"]):
            return base
        side = "BUY"
        pattern = "HH_HL"
        impulse_size = float(last_high["price"]) - float(previous_low["price"])
        pullback_size = float(last_high["price"]) - float(last_low["price"])
        preserved_structure = float(last_low["price"]) > float(previous_low["price"])
        invalidation_level = float(last_low["price"])
        pullback_closes = data_15m.iloc[int(last_low["index"]) + 1:]["Close"].astype(float)
        invalidated = bool(
            not pullback_closes.empty
            and (pullback_closes < invalidation_level).any()
        )
    elif bearish_sequence:
        previous_low, previous_high, last_low, last_high = sequence
        if float(last_low["price"]) >= float(previous_low["price"]):
            return base
        side = "SELL"
        pattern = "LH_LL"
        impulse_size = float(previous_high["price"]) - float(last_low["price"])
        pullback_size = float(last_high["price"]) - float(last_low["price"])
        preserved_structure = float(last_high["price"]) < float(previous_high["price"])
        invalidation_level = float(last_high["price"])
        pullback_closes = data_15m.iloc[int(last_high["index"]) + 1:]["Close"].astype(float)
        invalidated = bool(
            not pullback_closes.empty
            and (pullback_closes > invalidation_level).any()
        )
    else:
        return base

    point = point_size(symbol)
    minimum_impulse = minimum_swing_size(symbol)
    minimum_pullback = minimum_pullback_size(data_15m, symbol)
    base.update({
        "pattern": pattern,
        "bias": "BULLISH" if side == "BUY" else "BEARISH",
        "side": side,
        "completed_impulse_size": impulse_size,
        "completed_impulse_points": impulse_size / point,
        "pullback_size": pullback_size,
        "pullback_points": pullback_size / point,
        "invalidation_level": invalidation_level,
    })

    if impulse_size < minimum_impulse:
        base["reason"] = "WAIT_NO_VALID_100_POINT_IMPULSE"
        return base
    if pullback_size < minimum_pullback:
        base["reason"] = "WAIT_PULLBACK_TOO_SMALL"
        return base
    if pullback_size >= impulse_size:
        base["reason"] = "WAIT_PULLBACK_TOO_LARGE"
        return base
    if not preserved_structure or invalidated:
        base["reason"] = "WAIT_STRUCTURE_INVALIDATED"
        return base

    base.update({
        "valid": True,
        "reason": (
            "BULLISH_HH_HL_VALID_IMPULSE_AND_PULLBACK"
            if side == "BUY"
            else "BEARISH_LH_LL_VALID_IMPULSE_AND_PULLBACK"
        ),
    })
    return base


def classify_consolidation(data_15m, symbol):
    result = {
        "is_consolidation": False,
        "reason": None,
        "high_overlap": False,
        "compressed_range": False,
        "ema_compressed": False,
        "conditions_met": 0,
    }
    if data_15m is None or len(data_15m) < 21:
        return result

    recent = data_15m.tail(8)
    atr = atr14(data_15m)
    if atr is None:
        return result

    overlap_count = 0
    for index in range(1, len(recent)):
        previous = recent.iloc[index - 1]
        current = recent.iloc[index]
        previous_range = float(previous["High"]) - float(previous["Low"])
        current_range = float(current["High"]) - float(current["Low"])
        denominator = min(previous_range, current_range)
        overlap = min(float(previous["High"]), float(current["High"])) - max(
            float(previous["Low"]), float(current["Low"])
        )
        ratio = max(0.0, overlap) / denominator if denominator > 0 else 0.0
        if ratio >= 0.60:
            overlap_count += 1

    eight_candle_range = float(recent["High"].max() - recent["Low"].min())
    close = data_15m["Close"].astype(float)
    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema_spread = abs(float(ema9.iloc[-1]) - float(ema21.iloc[-1]))
    ema9_slope = abs(float(ema9.iloc[-1]) - float(ema9.iloc[-4]))

    result.update({
        "atr14": atr,
        "overlap_pairs": overlap_count,
        "high_overlap": overlap_count >= 5,
        "eight_candle_range": eight_candle_range,
        "compressed_range": eight_candle_range <= 3.0 * atr,
        "ema9": float(ema9.iloc[-1]),
        "ema21": float(ema21.iloc[-1]),
        "ema_spread": ema_spread,
        "ema9_three_candle_slope": ema9_slope,
        "ema_compressed": (
            ema_spread <= 0.20 * atr
            and ema9_slope <= 0.15 * atr
        ),
    })
    result["conditions_met"] = sum(
        bool(result[key])
        for key in ["high_overlap", "compressed_range", "ema_compressed"]
    )
    result["is_consolidation"] = result["conditions_met"] >= 2
    result["reason"] = "WAIT_CONSOLIDATION" if result["is_consolidation"] else None
    result["symbol"] = shared.normalize_symbol(symbol)
    return result


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


def save_remembered_breakout(
    symbol,
    side,
    level,
    break_time,
    break_close,
    reason,
    break_close_time=None,
    required_buffer=None,
):
    key = get_watch_key(symbol, side)
    shared.FIFTEEN_M_SWING_WATCH[key] = {
        "symbol": shared.normalize_symbol(symbol),
        "side": side,
        "swing_level": float(level),
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "break_confirmed": True,
        "break_candle_time": break_time,
        "break_close_time": break_close_time,
        "break_close": float(break_close),
        "bos_buffer": float(required_buffer or 0.0),
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
            "break_close_time": (
                watch.get("break_close_time")
                or candle_close_time(watch.get("break_candle_time"), 15)
            ),
            "break_close": float(watch.get("break_close")),
            "bos_buffer": float(watch.get("bos_buffer") or 0.0),
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
        "break_close_time": None,
        "break_close": None,
        "remembered": False,
        "reason": "WAIT_NO_15M_BREAK",
        "swings": [],
        "structure": {},
        "breakouts": [],
        "bos_buffer": None,
    }

    if data_15m is None or len(data_15m) < 10:
        result["reason"] = "WAIT_NOT_ENOUGH_15M_DATA"
        return result

    swing_source = data_15m.iloc[:-1].copy()
    swings = detect_raw_swings(swing_source, symbol)
    result["swings"] = swings

    if not swings:
        result["reason"] = "WAIT_NO_VALID_100_POINT_IMPULSE"
        return result

    structure = validate_continuation_structure(swings, swing_source, symbol)
    result["structure"] = structure
    if not structure.get("valid"):
        result["reason"] = structure.get("reason") or "WAIT_NO_VALID_100_POINT_IMPULSE"
        return result
    bullish_structure = structure.get("side") == "BUY"
    bearish_structure = structure.get("side") == "SELL"

    last = data_15m.iloc[-1]
    previous = data_15m.iloc[-2]
    last_close = float(last["Close"])
    last_high = float(last["High"])
    last_low = float(last["Low"])
    previous_close = float(previous["Close"])
    break_time = candle_time(data_15m.index[-1])
    break_close_time = candle_close_time(data_15m.index[-1], 15)
    required_buffer = bos_buffer(data_15m, symbol)
    result["bos_buffer"] = required_buffer

    high_swing = latest_swing(swings, "HIGH")
    low_swing = latest_swing(swings, "LOW")
    candidates = []

    if high_swing and bullish_structure:
        level = float(high_swing["price"])
        confirmed = (
            last_high > level
            and last_close >= level + required_buffer
            and previous_close <= level
        )
        if confirmed:
            candidates.append({
                "side": "BUY",
                "level": level,
                "break_time": break_time,
                "break_close_time": break_close_time,
                "break_close": last_close,
                "swing": high_swing,
                "structure": structure,
                "remembered": False,
                "bos_buffer": required_buffer,
            })

    if low_swing and bearish_structure:
        level = float(low_swing["price"])
        confirmed = (
            last_low < level
            and last_close <= level - required_buffer
            and previous_close >= level
        )
        if confirmed:
            candidates.append({
                "side": "SELL",
                "level": level,
                "break_time": break_time,
                "break_close_time": break_close_time,
                "break_close": last_close,
                "swing": low_swing,
                "structure": structure,
                "remembered": False,
                "bos_buffer": required_buffer,
            })

    if not candidates:
        for side in ["BUY", "SELL"]:
            remembered = remembered_breakout(symbol, side)
            if not remembered:
                continue
            candidates.append({
                **remembered,
                "structure": structure,
                "bos_buffer": float(remembered.get("bos_buffer") or required_buffer),
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
        if raw_buy_break and bullish_structure:
            result["reason"] = "WAIT_WEAK_15M_BOS"
        elif raw_sell_break and bearish_structure:
            result["reason"] = "WAIT_WEAK_15M_BOS"
        elif raw_buy_break and not bullish_structure:
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
        "break_close_time": (
            chosen.get("break_close_time")
            or candle_close_time(chosen.get("break_time"), 15)
        ),
        "break_close": chosen["break_close"],
        "remembered": bool(chosen.get("remembered")),
        "bos_buffer": float(chosen.get("bos_buffer") or required_buffer),
        "swing": chosen.get("swing"),
        "structure": chosen.get("structure") or structure,
        "reason": "15M_SWING_BREAK_CLOSED",
    })
    return result


def confirm_5m(
    data_5m,
    side,
    level,
    break_time,
    break_close_time=None,
    not_before=None,
    required_buffer=0.0,
):
    base = {
        "side": "WAIT",
        "close_confirmed": False,
        "closed_candle_time": None,
        "confirmation_close_time": None,
        "reason": "WAIT_NO_5M_CLOSE_CONFIRMATION",
    }
    if side not in ["BUY", "SELL"] or level is None or not break_time:
        return base

    closed = closed_frame(data_5m, 5)
    if closed is None or closed.empty:
        return base

    try:
        anchor = pd.Timestamp(
            break_close_time
            or candle_close_time(break_time, 15)
        )
        if anchor.tzinfo is None:
            anchor = anchor.tz_localize("UTC")
        else:
            anchor = anchor.tz_convert("UTC")
    except Exception:
        return base

    weak_confirmation_seen = False
    threshold_buffer = max(0.0, float(required_buffer or 0.0))
    for candle_index, candle in closed.iterrows():
        try:
            ts = pd.Timestamp(candle_index)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            confirmation_close = ts + pd.Timedelta(minutes=5)
            if confirmation_close <= anchor:
                continue
            freshness_floor = utc_timestamp(not_before)
            if freshness_floor is not None and confirmation_close <= freshness_floor:
                continue
            open_price = float(candle["Open"])
            close_price = float(candle["Close"])
            high_price = float(candle["High"])
            low_price = float(candle["Low"])
        except Exception:
            continue

        direction_matches = (
            side == "BUY"
            and close_price > open_price
        ) or (
            side == "SELL"
            and close_price < open_price
        )
        passed = direction_matches and (
            (
                side == "BUY"
                and close_price >= float(level) + threshold_buffer
            )
            or (
                side == "SELL"
                and close_price <= float(level) - threshold_buffer
            )
        )
        if passed:
            return {
                "side": side,
                "close_confirmed": True,
                "closed_candle_time": ts.isoformat(),
                "confirmation_close_time": confirmation_close.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "setup_level": float(level),
                "bos_buffer": threshold_buffer,
                "reason": "5M_CLOSE_CONFIRMED",
            }
        if direction_matches and (
            (side == "BUY" and close_price > float(level))
            or (side == "SELL" and close_price < float(level))
        ):
            weak_confirmation_seen = True

    if weak_confirmation_seen:
        base["reason"] = "WAIT_WEAK_5M_CONFIRMATION"
        base["bos_buffer"] = threshold_buffer
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
        protected = float(entry) + ((tp2_price - float(entry)) * 0.40)
    else:
        tp1 = float(entry) - ((float(entry) - tp2_price) * tp1_ratio)
        protected = float(entry) - ((float(entry) - tp2_price) * 0.40)

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
        "protected_sl_rule": "40% of entry-to-TP2 after TP1 wick touch",
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
    consolidation = classify_consolidation(closed_15m, normalized_symbol)
    base_meta["consolidation"] = consolidation
    base_meta["market_condition"] = (
        "CONSOLIDATION" if consolidation.get("is_consolidation") else "STRUCTURE"
    )
    if consolidation.get("is_consolidation"):
        return wait_result(normalized_symbol, "WAIT_CONSOLIDATION", {
            **base_meta,
            "trend_15m": trend,
        })

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
    ema_allowed = (
        side == "BUY" and bool(trend.get("buy_allowed"))
    ) or (
        side == "SELL" and bool(trend.get("sell_allowed"))
    )
    if not ema_allowed:
        return wait_result(normalized_symbol, "WAIT_EMA_NOT_ALLOWED", {
            **base_meta,
            "trend_15m": trend,
            "fifteen_m_swing_break": breakout,
            "blocked_by": "WAIT_EMA_NOT_ALLOWED",
            "blocker_rule_name": "strict_15m_ema_permission",
        })

    prior_close = last_position_closed_time(normalized_symbol)
    bos_close = utc_timestamp(
        breakout.get("break_close_time")
        or candle_close_time(breakout.get("break_time"), 15)
    )
    if prior_close is not None and (bos_close is None or bos_close <= prior_close):
        shared.FIFTEEN_M_SWING_WATCH.pop(
            get_watch_key(normalized_symbol, side),
            None,
        )
        shared.save_fifteen_m_swing_watch()
        return wait_result(normalized_symbol, "WAIT_BOS_NOT_FRESH_AFTER_POSITION_CLOSE", {
            **base_meta,
            "trend_15m": trend,
            "fifteen_m_swing_break": breakout,
            "previous_position_closed_at": prior_close.isoformat(),
            "blocked_by": "post_close_setup_freshness",
            "blocker_rule_name": "bos_after_previous_position_close",
        })

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
        break_close_time=breakout.get("break_close_time"),
        not_before=prior_close,
        required_buffer=breakout.get("bos_buffer"),
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
            five_m.get("reason") or "WAIT_NO_5M_CLOSE_CONFIRMATION",
            break_close_time=breakout.get("break_close_time"),
            required_buffer=breakout.get("bos_buffer"),
        )
        return wait_result(normalized_symbol, five_m.get("reason") or "WAIT_NO_5M_CLOSE_CONFIRMATION", {
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
    result = {
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
        "consolidation": consolidation,
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
        "fifteen_m_break_time": breakout["break_time"],
        "fifteen_m_break_close_time": breakout.get("break_close_time"),
        "setup_candle_time": five_m.get("confirmation_close_time"),
        "five_m_closed_candle_time": five_m.get("confirmation_close_time"),
        "previous_position_closed_at": (
            prior_close.isoformat() if prior_close is not None else None
        ),
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
            "fifteen_m_break_close_time": breakout.get("break_close_time"),
            "five_m_confirmation": True,
            "five_m_confirmation_close_time": five_m.get("confirmation_close_time"),
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
    diagnostics = dict(result["signal_diagnostics"])
    result["entry_strategy_debug"] = diagnostics.copy()
    result["strategy_debug"] = diagnostics.copy()
    result["bos_detected"] = True
    result["choch_detected"] = False
    result["smc_direction"] = side
    result["fifteen_m_close_confirmed"] = True
    result["five_m_confirmation"] = True
    return result


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
        return entry + ((tp2 - entry) * 0.40)
    return entry - ((entry - tp2) * 0.40)
