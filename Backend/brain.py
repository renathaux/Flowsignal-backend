# =========================
# 📦 IMPORTS
# =========================
import time
import copy
import pandas as pd
import requests
from datetime import datetime, timezone

def is_market_calendar_closed():
    now = datetime.now(timezone.utc)
    day = now.weekday()  # Monday=0, Sunday=6
    hour = now.hour

    if day == 4 and hour >= 22:
        return True
    if day == 5:
        return True
    if day == 6 and hour < 22:
        return True

    return False

# =========================
# 🧠 V5 HELPERS
# =========================
def detect_recent_pressure(data, symbol):
    close = data["Close"]
    open_ = data["Open"]
    high = data["High"]
    low = data["Low"]

    if len(data) < 6:
        return "NEUTRAL", 0

    c1 = close.iloc[-1].item()
    c2 = close.iloc[-2].item()
    c3 = close.iloc[-3].item()

    o1 = open_.iloc[-1].item()
    o2 = open_.iloc[-2].item()
    o3 = open_.iloc[-3].item()

    h1 = high.iloc[-1].item()
    l1 = low.iloc[-1].item()

    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)
    body3 = abs(c3 - o3)

    avg_range = (high.iloc[-6:] - low.iloc[-6:]).mean().item()

    if symbol == "EURUSD":
        strong_body = 0.0004
    else:
        strong_body = 1.5

    bull_1 = c1 > o1
    bear_1 = c1 < o1
    bull_2 = c2 > o2
    bear_2 = c2 < o2
    bull_3 = c3 > o3
    bear_3 = c3 < o3

    bull_count = sum([bull_1, bull_2, bull_3])
    bear_count = sum([bear_1, bear_2, bear_3])

    pressure_score = 0

    if bull_1:
        pressure_score += 2
    elif bear_1:
        pressure_score -= 2

    if bull_2:
        pressure_score += 1
    elif bear_2:
        pressure_score -= 1

    if bull_3:
        pressure_score += 1
    elif bear_3:
        pressure_score -= 1

    if body1 > strong_body:
        if bull_1:
            pressure_score += 2
        elif bear_1:
            pressure_score -= 2

    candle_range = h1 - l1
    if candle_range > 0:
        close_position = (c1 - l1) / candle_range
        if close_position >= 0.7:
            pressure_score += 1
        elif close_position <= 0.3:
            pressure_score -= 1

    if bull_count == 3 and avg_range > 0:
        pressure_score += 1
    elif bear_count == 3 and avg_range > 0:
        pressure_score -= 1

    if pressure_score >= 3:
        return "BULLISH", pressure_score
    elif pressure_score <= -3:
        return "BEARISH", pressure_score
    else:
        return "NEUTRAL", pressure_score

def detect_momentum_strength(data, symbol):
    close = data["Close"]
    open_ = data["Open"]
    high = data["High"]
    low = data["Low"]

    if len(data) < 6:
        return "WEAK", 0

    c1 = close.iloc[-1].item()
    c2 = close.iloc[-2].item()
    c3 = close.iloc[-3].item()

    o1 = open_.iloc[-1].item()
    o2 = open_.iloc[-2].item()
    o3 = open_.iloc[-3].item()

    h1 = high.iloc[-1].item()
    l1 = low.iloc[-1].item()

    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)
    body3 = abs(c3 - o3)

    avg_range = (high.iloc[-6:] - low.iloc[-6:]).mean().item()
    candle_range = h1 - l1
    body_ratio = body1 / candle_range if candle_range > 0 else 0

    if symbol == "EURUSD":
        strong_body = 0.0004
        min_body = 0.0002
    else:
        strong_body = 1.5
        min_body = 0.8

    bull_1 = c1 > o1
    bear_1 = c1 < o1
    bull_2 = c2 > o2
    bear_2 = c2 < o2
    bull_3 = c3 > o3
    bear_3 = c3 < o3

    bull_count = sum([bull_1, bull_2, bull_3])
    bear_count = sum([bear_1, bear_2, bear_3])

    momentum_score = 0

    if bull_1 or bear_1:
        momentum_score += 2

    if body1 > min_body:
        momentum_score += 1

    if body1 > strong_body:
        momentum_score += 2

    if body_ratio >= 0.65:
        momentum_score += 2
    elif body_ratio >= 0.5:
        momentum_score += 1

    if bull_count >= 2 or bear_count >= 2:
        momentum_score += 1

    if bull_count == 3 or bear_count == 3:
        momentum_score += 1

    if avg_range > 0 and candle_range > avg_range * 1.2:
        momentum_score += 1

    if momentum_score >= 7:
        return "STRONG", momentum_score
    elif momentum_score >= 4:
        return "MEDIUM", momentum_score
    else:
        return "WEAK", momentum_score   

def detect_market_condition(data, ema9, ema21, ema50):
    close = data["Close"]
    high = data["High"]
    low = data["Low"]

    if len(close) < 10:
        return "UNKNOWN"

    c1 = close.iloc[-1].item()
    h1 = high.iloc[-1].item()
    l1 = low.iloc[-1].item()

    candle_range = h1 - l1
    avg_range = (high.iloc[-8:] - low.iloc[-8:]).mean().item()

    ema_bull_stack = ema9 > ema21 > ema50
    ema_bear_stack = ema9 < ema21 < ema50

    close_above_ema9 = c1 > ema9
    close_below_ema9 = c1 < ema9

    small_candle = candle_range < avg_range * 0.65
    big_candle = candle_range > avg_range * 1.15

    if ema_bull_stack and close_above_ema9 and big_candle:
        return "TRENDING_BULL"
    if ema_bear_stack and close_below_ema9 and big_candle:
        return "TRENDING_BEAR"

    if ema_bull_stack and c1 < ema9:
        return "PULLBACK_BULL"
    if ema_bear_stack and c1 > ema9:
        return "PULLBACK_BEAR"

    if small_candle:
        return "CHOPPY"

    return "MIXED"

def detect_liquidity_grab(data):
    if len(data) < 2:
        return None

    last = data.iloc[-1]

    body = abs(last["Close"] - last["Open"])
    lower_wick = min(last["Open"], last["Close"]) - last["Low"]
    upper_wick = last["High"] - max(last["Open"], last["Close"])

        # stronger trap conditions
    bullish_trap = (
        lower_wick > body * 2.5
        and last["Close"] > last["Open"]  # must close bullish
    )

    bearish_trap = (
        upper_wick > body * 2.5
       and last["Close"] < last["Open"]  # must close bearish
    )

    if bullish_trap:
        return "BULLISH"

    if bearish_trap:
        return "BEARISH"

    return None

def get_entry_quality(
    buy_score,
    sell_score,
    score_gap,
    market_condition,
    zone_position,
    htf_structure,
    choch_signal
):
    top_score = max(buy_score, sell_score)

    confluence_score = 0

    if htf_structure == "BULLISH" and choch_signal == "BULLISH":
        confluence_score += 1

    if htf_structure == "BEARISH" and choch_signal == "BEARISH":
        confluence_score += 1

    if zone_position == "DISCOUNT" and htf_structure == "BULLISH":
        confluence_score += 1

    if zone_position == "PREMIUM" and htf_structure == "BEARISH":
        confluence_score += 1

    if market_condition in ["CHOPPY", "MIXED", "UNKNOWN"]:
        if top_score >= 85 and score_gap >= 25 and confluence_score >= 2:
            return "MEDIUM"
        elif top_score >= 80 and score_gap >= 40:
            return "MEDIUM"
        elif top_score >= 70 and score_gap >= 50:
            return "MEDIUM"
        elif top_score >= 75 and confluence_score >= 1:
            return "MEDIUM"
        return "WEAK"

    if top_score >= 95 and score_gap >= 25 and confluence_score >= 2:
        return "STRONG"
    elif top_score >= 85 and score_gap >= 20 and confluence_score >= 1:
        return "MEDIUM"
    else:
        return "WEAK"


def apply_no_trade_logic(
    buy_score,
    sell_score,
    market_condition,
    low_volatility,
    zone_position,
    htf_structure,
    choch_signal,
    momentum_strength,
    recent_pressure
):
    reasons_nt = []

    score_gap = abs(buy_score - sell_score)
    top_score = max(buy_score, sell_score)
    no_trade = False

    bullish_override = (
        htf_structure == "BULLISH"
        and choch_signal == "BULLISH"
        and zone_position == "DISCOUNT"
        and top_score >= 85
        and score_gap >= 20
    )

    bearish_override = (
        htf_structure == "BEARISH"
        and choch_signal == "BEARISH"
        and zone_position == "PREMIUM"
        and top_score >= 85
        and score_gap >= 20
    )

    momentum_override = (
        momentum_strength == "STRONG"
        and (
            (recent_pressure == "BULLISH" and buy_score >= 75 and score_gap >= 15)
            or
            (recent_pressure == "BEARISH" and sell_score >= 75 and score_gap >= 15)
        )
    )

    strong_override = bullish_override or bearish_override or momentum_override

    if market_condition in ["CHOPPY", "MIXED", "UNKNOWN"] and not strong_override:
        if top_score < 75 or score_gap < 15:
            no_trade = True
            reasons_nt.append("Messy market -> WAIT")

    if low_volatility and score_gap < 25 and momentum_strength != "STRONG":
        no_trade = True
        reasons_nt.append("Low volatility + weak edge")

    if buy_score >= 50 and sell_score >= 50 and score_gap < 20:
        no_trade = True
        reasons_nt.append("Mixed direction")

    return no_trade, reasons_nt

def apply_fake_signal_killer(
    symbol,
    buy_score,
    sell_score,
    score_gap,
    market_condition,
    market_state,
    entry_timing,
    htf_structure,
    choch_signal,
    recent_pressure,
    momentum_strength,
    zone_position,
    c1,
    ema9,
    ema21,
    recent_high,
    recent_low
):
    reasons_fk = []
    kill_trade = False

    dist_ema9 = abs(c1 - ema9)
    dist_ema21 = abs(c1 - ema21)

    if symbol == "EURUSD":
        stretch_ema9 = 0.0012
        stretch_ema21 = 0.0018
        breakout_buffer = 0.00025
    else:
        stretch_ema9 = 6.0
        stretch_ema21 = 9.0
        breakout_buffer = 1.5

    top_score = max(buy_score, sell_score)
    direction = "BUY" if buy_score > sell_score else "SELL"

    if top_score < 72 or score_gap < 10:
        kill_trade = True
        reasons_fk.append("Fake killer: weak edge")

    if market_condition in ["CHOPPY", "MIXED", "UNKNOWN"] and momentum_strength != "WEAK":
        kill_trade = True
        reasons_fk.append("Fake killer: messy market")

    if market_state == "RANGE" and score_gap < 26:
        kill_trade = True
        reasons_fk.append("Fake killer: range trap")

    if dist_ema9 > stretch_ema9 or dist_ema21 > stretch_ema21:
        kill_trade = True
        reasons_fk.append("Fake killer: too stretched")

    if direction == "BUY":
        if htf_structure == "BEARISH" and not (
            choch_signal == "BULLISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
            and recent_pressure == "BULLISH"
            and zone_position == "DISCOUNT"
            and buy_score >= 85
            and score_gap >= 20
        ):
            kill_trade = True
            reasons_fk.append("Fake killer: buy against HTF")

        if c1 > recent_high + breakout_buffer and entry_timing in ["LATE_BUY", "WAIT_PULLBACK_BUY"]:
            kill_trade = True
            reasons_fk.append("Fake killer: breakout buy too late")

    elif direction == "SELL":
        if htf_structure == "BULLISH" and not (
            choch_signal == "BEARISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
            and recent_pressure == "BEARISH"
            and zone_position == "PREMIUM"
            and sell_score >= 85
            and score_gap >= 20
        ):
            kill_trade = True
            reasons_fk.append("Fake killer: sell against HTF")

        if c1 < recent_low - breakout_buffer and entry_timing in ["LATE_SELL", "WAIT_PULLBACK_SELL"]:
            kill_trade = True
            reasons_fk.append("Fake killer: breakout sell too late")

    return kill_trade, reasons_fk

def detect_htf_structure(htf_data):
    high = htf_data["High"]
    low = htf_data["Low"]
    close = htf_data["Close"]

    if len(htf_data) < 6:
        return "NEUTRAL"

    h1 = high.iloc[-1].item()
    h2 = high.iloc[-2].item()
    h3 = high.iloc[-3].item()

    l1 = low.iloc[-1].item()
    l2 = low.iloc[-2].item()
    l3 = low.iloc[-3].item()

    c1 = close.iloc[-1].item()
    c2 = close.iloc[-2].item()

    if h1 > h2 > h3 and l1 > l2 > l3:
        return "BULLISH"

    if h1 < h2 < h3 and l1 < l2 < l3:
        return "BEARISH"

    if h1 > h2 and l1 > l2 and c1 > c2:
        return "BULLISH"

    if h1 < h2 and l1 < l2 and c1 < c2:
        return "BEARISH"

    return "NEUTRAL"


def detect_choch(data):
    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    open_ = data["Open"]

    if len(data) < 7:
        return "NONE"

    c1 = close.iloc[-1].item()
    c2 = close.iloc[-2].item()

    h1 = high.iloc[-1].item()
    h2 = high.iloc[-2].item()
    h3 = high.iloc[-3].item()
    h4 = high.iloc[-4].item()
    h5 = high.iloc[-5].item()

    l1 = low.iloc[-1].item()
    l2 = low.iloc[-2].item()
    l3 = low.iloc[-3].item()
    l4 = low.iloc[-4].item()
    l5 = low.iloc[-5].item()

    o1 = open_.iloc[-1].item()

    body1 = abs(c1 - o1)
    recent_range = (high.iloc[-6:] - low.iloc[-6:]).mean().item()
    strong_shift = body1 > recent_range * 0.35

    recent_micro_high = max(h3, h4, h5)
    recent_micro_low = min(l3, l4, l5)

    lower_highs_before = h3 < h4 and h4 < h5
    higher_lows_before = l3 > l4 and l4 > l5

    bullish_break = c1 > recent_micro_high and c1 > c2 and h1 > h2
    bearish_break = c1 < recent_micro_low and c1 < c2 and l1 < l2

    if lower_highs_before and bullish_break and strong_shift:
        return "BULLISH"

    if higher_lows_before and bearish_break and strong_shift:
        return "BEARISH"

    return "NONE"


def detect_pd_zone(data):
    high = data["High"]
    low = data["Low"]
    close = data["Close"]

    if len(data) < 12:
        return "NEUTRAL", 0.5, 0, 0, 0

    range_high = high.iloc[-12:].max().item()
    range_low = low.iloc[-12:].min().item()
    current_price = close.iloc[-1].item()

    dealing_range = range_high - range_low
    if dealing_range <= 0:
        return "NEUTRAL", 0.5, range_high, range_low, current_price

    eq = (range_high + range_low) / 2
    position = (current_price - range_low) / dealing_range

    if current_price < eq:
        zone = "DISCOUNT"
    elif current_price > eq:
        zone = "PREMIUM"
    else:
        zone = "EQUILIBRIUM"

    return zone, position, range_high, range_low, current_price


def detect_swing_liquidity(data):
    high = data["High"]
    low = data["Low"]

    if len(data) < 10:
        return None, None

    swing_highs = []
    swing_lows = []

    for i in range(2, len(data) - 2):
        h = high.iloc[i].item()
        l = low.iloc[i].item()

        h1 = high.iloc[i - 1].item()
        h2 = high.iloc[i - 2].item()
        h3 = high.iloc[i + 1].item()
        h4 = high.iloc[i + 2].item()

        l1 = low.iloc[i - 1].item()
        l2 = low.iloc[i - 2].item()
        l3 = low.iloc[i + 1].item()
        l4 = low.iloc[i + 2].item()

        if h > h1 and h > h2 and h > h3 and h > h4:
            swing_highs.append(h)

        if l < l1 and l < l2 and l < l3 and l < l4:
            swing_lows.append(l)

    last_swing_high = swing_highs[-1] if swing_highs else None
    last_swing_low = swing_lows[-1] if swing_lows else None

    return last_swing_high, last_swing_low


def detect_liquidity_trap(c1, o1, h1, l1, swing_high, swing_low, strong_body):
    body1 = abs(c1 - o1)

    bullish_trap = False
    bearish_trap = False

    if swing_low is not None:
        bullish_trap = l1 < swing_low and c1 > swing_low and c1 > o1 and body1 >= strong_body * 0.8

    if swing_high is not None:
        bearish_trap = h1 > swing_high and c1 < swing_high and c1 < o1 and body1 >= strong_body * 0.8

    if bullish_trap:
        return "BULLISH"
    if bearish_trap:
        return "BEARISH"
    return "NONE"


def detect_entry_timing(
    c1,
    o1,
    h1,
    l1,
    ema9,
    ema21,
    zone_position,
    pd_high,
    pd_low,
    symbol,
    bullish_bos,
    bearish_bos,
    htf_structure,
    choch_signal,
    recent_pressure,
    momentum_strength,
    market_state,
    trap_signal
):
    body1 = abs(c1 - o1)
    candle_range = h1 - l1
    dist_from_ema9 = abs(c1 - ema9)

    if symbol == "EURUSD":
        pullback_limit = 0.00045
        stretch_limit = 0.0010
        huge_body = 0.00065
        reentry_buffer = 0.00018
    else:
        pullback_limit = 2.2
        stretch_limit = 5.0
        huge_body = 2.2
        reentry_buffer = 0.7

    if candle_range <= 0:
        return "NEUTRAL"

    body_ratio = body1 / candle_range

    bullish_context = (
        (
            htf_structure == "BULLISH"
            and market_state in ["BULL_PULLBACK", "BULL_REVERSAL", "BULL_EXPANSION"]
        )
        or
        (
            choch_signal == "BULLISH"
            and recent_pressure == "BULLISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
        )
        or
        (
            trap_signal == "BULLISH"
            and c1 > o1
            and recent_pressure == "BULLISH"
        )
    )

    bearish_context = (
        (
            htf_structure == "BEARISH"
            and market_state in ["BEAR_PULLBACK", "BEAR_REVERSAL", "BEAR_EXPANSION"]
        )
        or
        (
            choch_signal == "BEARISH"
            and recent_pressure == "BEARISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
        )
        or
        (
            trap_signal == "BEARISH"
            and c1 < o1
            and recent_pressure == "BEARISH"
        )
    )

    bullish_rejection = (
        c1 > o1
        and c1 > ema9
        and l1 <= ema9 + reentry_buffer
        and body_ratio >= 0.45
    )

    bearish_rejection = (
        c1 < o1
        and c1 < ema9
        and h1 >= ema9 - reentry_buffer
        and body_ratio >= 0.45
    )

    too_stretched_buy = dist_from_ema9 > stretch_limit or (bullish_bos and body1 > huge_body)
    too_stretched_sell = dist_from_ema9 > stretch_limit or (bearish_bos and body1 > huge_body)

    # 🔥 fast trap-based reversal timing
    bullish_trap_reclaim = (
        trap_signal == "BULLISH"
        and c1 > o1
        and c1 > ema9
    )
    bearish_trap_reject = (
        trap_signal == "BEARISH"
        and c1 < o1
        and c1 < ema9
    )

    if bullish_context:
        if bullish_trap_reclaim:
            if zone_position == "DISCOUNT":
                return "GOOD_BUY"
            return "WAIT_PULLBACK_BUY"

        if zone_position == "PREMIUM":
            return "WAIT_PULLBACK_BUY"

        if too_stretched_buy and c1 > ema9:
            return "LATE_BUY"

        if bullish_rejection and dist_from_ema9 <= pullback_limit:
            return "GOOD_BUY"

        if c1 > ema9 and dist_from_ema9 <= pullback_limit and body_ratio >= 0.35:
            return "GOOD_BUY"

        if c1 > ema9 and not too_stretched_buy:
            return "WAIT_PULLBACK_BUY"

        return "NEUTRAL"

    if bearish_context:
        if bearish_trap_reject:
            if zone_position == "PREMIUM":
                return "GOOD_SELL"
            return "WAIT_PULLBACK_SELL"

        if zone_position == "DISCOUNT":
            return "WAIT_PULLBACK_SELL"

        if too_stretched_sell and c1 < ema9:
            return "LATE_SELL"

        if bearish_rejection and dist_from_ema9 <= pullback_limit:
            return "GOOD_SELL"

        if c1 < ema9 and dist_from_ema9 <= pullback_limit and body_ratio >= 0.35:
            return "GOOD_SELL"

        if c1 < ema9 and not too_stretched_sell:
            return "WAIT_PULLBACK_SELL"

        return "NEUTRAL"

    return "NEUTRAL"


def detect_market_state(data, ema9, ema21, ema50, choch_signal):
    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    open_ = data["Open"]

    if len(data) < 8:
        return "RANGE"

    c1 = close.iloc[-1].item()
    c2 = close.iloc[-2].item()
    c3 = close.iloc[-3].item()

    o1 = open_.iloc[-1].item()
    o2 = open_.iloc[-2].item()
    o3 = open_.iloc[-3].item()

    h1 = high.iloc[-1].item()
    l1 = low.iloc[-1].item()

    body1 = abs(c1 - o1)
    avg_range = (high.iloc[-6:] - low.iloc[-6:]).mean().item()

    bullish_stack = ema9 > ema21 > ema50
    bearish_stack = ema9 < ema21 < ema50

    above_ema9 = c1 > ema9
    below_ema9 = c1 < ema9

    expansion_body = body1 > avg_range * 0.45

    if bullish_stack and above_ema9 and expansion_body and c1 > c2 > c3:
        return "BULL_EXPANSION"

    if bearish_stack and below_ema9 and expansion_body and c1 < c2 < c3:
        return "BEAR_EXPANSION"

    if bullish_stack and c1 < ema9 and choch_signal != "BEARISH":
        return "BULL_PULLBACK"

    if bearish_stack and c1 > ema9 and choch_signal != "BULLISH":
        return "BEAR_PULLBACK"

    if choch_signal == "BULLISH" and c1 > ema9:
        return "BULL_REVERSAL"

    if choch_signal == "BEARISH" and c1 < ema9:
        return "BEAR_REVERSAL"

    return "RANGE"


def calculate_confidence(
    buy_score,
    sell_score,
    htf_structure,
    choch_signal,
    zone_position,
    trap_signal,
    market_state,
    entry_timing,
    market_condition
):
    top_score = max(buy_score, sell_score)
    score_gap = abs(buy_score - sell_score)
    direction = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "NONE"

    confidence = 5

    confidence += top_score * 0.38
    confidence += score_gap * 0.22

    if direction == "BUY":
        if htf_structure == "BULLISH":
            confidence += 7
        if choch_signal == "BULLISH":
            confidence += 5
        if zone_position == "DISCOUNT":
            confidence += 4
        if trap_signal == "BULLISH":
            confidence += 3
        if market_state in ["BULL_PULLBACK", "BULL_REVERSAL", "BULL_EXPANSION"]:
            confidence += 5
        if entry_timing == "GOOD_BUY":
            confidence += 7
        elif entry_timing == "WAIT_PULLBACK_BUY":
            confidence -= 5
        elif entry_timing == "LATE_BUY":
            confidence -= 9

    elif direction == "SELL":
        if htf_structure == "BEARISH":
            confidence += 7
        if choch_signal == "BEARISH":
            confidence += 5
        if zone_position == "PREMIUM":
            confidence += 4
        if trap_signal == "BEARISH":
            confidence += 3
        if market_state in ["BEAR_PULLBACK", "BEAR_REVERSAL", "BEAR_EXPANSION"]:
            confidence += 5
        if entry_timing == "GOOD_SELL":
            confidence += 7
        elif entry_timing == "WAIT_PULLBACK_SELL":
            confidence -= 5
        elif entry_timing == "LATE_SELL":
            confidence -= 9

    if market_condition == "CHOPPY":
        confidence -= 10
    elif market_condition == "MIXED":
        confidence -= 6
    elif market_condition == "UNKNOWN":
        confidence -= 4

    if top_score < 25:
        confidence *= 0.60
    elif top_score < 40:
        confidence *= 0.78

    if score_gap < 10:
        confidence *= 0.65
    elif score_gap < 20:
        confidence *= 0.82

    confidence = max(0, min(int(confidence), 95))
    return confidence


# =========================
# 📊 DATA FETCH (TWELVE DATA)
# =========================
import os


import os

TWELVE_DATA_API_KEY = "9bce0b4c48b1498d8e2afb8a5c186359"


def _normalize_td_values(values):
    if not values:
        return pd.DataFrame()

    df = pd.DataFrame(values).copy()

    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "datetime": "Datetime"
    }
    df = df.rename(columns=rename_map)

    needed = ["Open", "High", "Low", "Close"]
    for col in needed:
        if col not in df.columns:
            return pd.DataFrame()

    if "Volume" not in df.columns:
        df["Volume"] = 0

    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Datetime" in df.columns:
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
        df = df.dropna(subset=["Datetime"])
        df = df.sort_values("Datetime").set_index("Datetime")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return df[["Open", "High", "Low", "Close", "Volume"]]


def safe_download(symbol, interval, outputsize=80, tries=1, pause=1):
    last_error = None

    if not TWELVE_DATA_API_KEY or TWELVE_DATA_API_KEY == "PASTE_YOUR_KEY_HERE":
        print("TWELVE DATA ERROR: missing API key")
        return pd.DataFrame()

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "format": "JSON",
        "timezone": "UTC",
        "apikey": TWELVE_DATA_API_KEY,
    }

    for attempt in range(1, tries + 1):
        try:
            print(f"{symbol} {interval}: attempt {attempt}/{tries}")

            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()

            if payload.get("status") == "error":
                message = payload.get("message", "Unknown Twelve Data error")
                code = payload.get("code", "")
                raise ValueError(f"{code} {message}".strip())

            df = _normalize_td_values(payload.get("values", []))

            if not df.empty:
                print(f"{symbol} {interval}: OK ({len(df)} rows)")
                return df

            print(f"{symbol} {interval}: empty data")

        except Exception as e:
            last_error = e
            print(f"{symbol} {interval}: failed -> {e}")

        time.sleep(pause)

    print(f"{symbol} {interval}: FAILED after {tries} tries. Last error: {last_error}")
    return pd.DataFrame()

_FETCH_LOCK = False
def fetch_market_data():
    global _FETCH_LOCK

    now = datetime.now(timezone.utc)

    if not hasattr(fetch_market_data, "_cache"):
        fetch_market_data._cache = {
            "eurusd_5m": pd.DataFrame(),
            "gold_5m": pd.DataFrame(),
            "eurusd_15m": pd.DataFrame(),
            "gold_15m": pd.DataFrame(),
            "eurusd_1h": pd.DataFrame(),
            "gold_1h": pd.DataFrame(),
            "last_5m_update": None,
            "last_15m_update": None,
            "last_1h_update": None,
        }

    cache = fetch_market_data._cache

    if _FETCH_LOCK:
        print("===== FETCH LOCK ACTIVE: USING CACHE =====")
        return (
            cache["eurusd_5m"],
            cache["gold_5m"],
            cache["eurusd_15m"],
            cache["gold_15m"],
            cache["eurusd_1h"],
            cache["gold_1h"],
        )

    last_5m = cache["last_5m_update"]
    last_15m = cache["last_15m_update"]
    last_1h = cache["last_1h_update"]

    _FETCH_LOCK = True

    need_5m = (
        cache["eurusd_5m"].empty
        or cache["gold_5m"].empty
        or last_5m is None
        or (now - last_5m).total_seconds() >= 300
    )

    need_15m = (
        cache["eurusd_15m"].empty
        or cache["gold_15m"].empty
        or last_15m is None
        or (now - last_15m).total_seconds() >= 900
    )
    need_1h = (
        cache["eurusd_1h"].empty
        or cache["gold_1h"].empty
        or last_1h is None
        or (now - last_1h).total_seconds() >= 3600
    )
    if need_5m:
        print("===== REFRESHING 5M DATA =====")
        cache["eurusd_5m"] = safe_download("EUR/USD", "5min", outputsize=120)
        cache["gold_5m"] = safe_download("XAU/USD", "5min", outputsize=120)
        cache["last_5m_update"] = now
    else:
        print("===== USING CACHED 5M DATA =====")

    if need_15m:
        print("===== REFRESHING 15M DATA =====")
        cache["eurusd_15m"] = safe_download("EUR/USD", "15min", outputsize=120)
        cache["gold_15m"] = safe_download("XAU/USD", "15min", outputsize=120)
        cache["last_15m_update"] = now
    else:
        print("===== USING CACHED 15M DATA =====")

    if need_1h:
        print("===== REFRESHING 1H DATA =====")
        cache["eurusd_1h"] = safe_download("EUR/USD", "1h", outputsize=120)
        cache["gold_1h"] = safe_download("XAU/USD", "1h", outputsize=120)
        cache["last_1h_update"] = now
    else:
        print("===== USING CACHED 1H DATA =====")

    _FETCH_LOCK = False
    eurusd = cache["eurusd_5m"]
    gold = cache["gold_5m"]
    eurusd_htf = cache["eurusd_15m"]
    gold_htf = cache["gold_15m"]
    eurusd_1h = cache["eurusd_1h"]
    gold_1h = cache["gold_1h"]
    print("===== FETCH DEBUG =====")
    print("Time now:", now.strftime("%Y-%m-%d %H:%M:%S UTC"))
    print(
        "Last 5m update:",
        cache["last_5m_update"].strftime("%Y-%m-%d %H:%M:%S UTC")
        if cache["last_5m_update"] else None
    )
    print(
        "Last 15m update:",
        cache["last_15m_update"].strftime("%Y-%m-%d %H:%M:%S UTC")
        if cache["last_15m_update"] else None
    )
    print("EURUSD 5m rows:", len(eurusd))
    print("GOLD 5m rows:", len(gold))
    print("EURUSD 15m rows:", len(eurusd_htf))
    print("GOLD 15m rows:", len(gold_htf))

    print("EURUSD 5m columns:", eurusd.columns.tolist() if not eurusd.empty else "EMPTY")
    print("GOLD 5m columns:", gold.columns.tolist() if not gold.empty else "EMPTY")
    print("EURUSD 15m columns:", eurusd_htf.columns.tolist() if not eurusd_htf.empty else "EMPTY")
    print("GOLD 15m columns:", gold_htf.columns.tolist() if not gold_htf.empty else "EMPTY")

    return eurusd, gold, eurusd_htf, gold_htf, eurusd_1h, gold_1h

def get_signal(data, htf_data, symbol):
    if data.empty or htf_data.empty:
        return {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (no data)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "UNKNOWN",
            "entry_quality": "WEAK",
            "entry_timing": "NEUTRAL",
            "fake_kill": False,
            "debug_reasons": ["No data"]
        }

    if len(data) < 20 or len(htf_data) < 10:
        return {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (not enough data)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "UNKNOWN",
            "entry_quality": "WEAK",
            "entry_timing": "NEUTRAL",
            "fake_kill": False,
            "debug_reasons": ["Not enough data"]
        }

    close = data["Close"]
    open_ = data["Open"]
    high = data["High"]
    low = data["Low"]

    c1 = close.iloc[-1].item()
    o1 = open_.iloc[-1].item()
    h1 = high.iloc[-1].item()
    l1 = low.iloc[-1].item()

    if symbol == "EURUSD":
        strong_body = 0.00035
    else:
        strong_body = 1.2

    reasons = []

    ema9 = close.ewm(span=9, adjust=False).mean().iloc[-1].item()
    ema21 = close.ewm(span=21, adjust=False).mean().iloc[-1].item()
    ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1].item()

    recent_high = high.iloc[-6:-1].max().item()
    recent_low = low.iloc[-6:-1].min().item()

    bullish_bos = c1 > recent_high
    bearish_bos = c1 < recent_low

    recent_pressure, pressure_score = detect_recent_pressure(data, symbol)
    momentum_strength, momentum_score = detect_momentum_strength(data, symbol)
    market_condition = detect_market_condition(data, ema9, ema21, ema50)

    htf_structure = detect_htf_structure(htf_data)
    choch_signal = detect_choch(data)

    zone_position, zone_ratio, pd_high, pd_low, current_price = detect_pd_zone(data)
    swing_high, swing_low = detect_swing_liquidity(data)
    trap_signal = detect_liquidity_trap(c1, o1, h1, l1, swing_high, swing_low, strong_body)

    market_state = detect_market_state(data, ema9, ema21, ema50, choch_signal)

    entry_timing = detect_entry_timing(
        c1,
        o1,
        h1,
        l1,
        ema9,
        ema21,
        zone_position,
        pd_high,
        pd_low,
        symbol,
        bullish_bos,
        bearish_bos,
        htf_structure,
        choch_signal,
        recent_pressure,
        momentum_strength,
        market_state,
        trap_signal
    )
    avg_range = (high.iloc[-8:] - low.iloc[-8:]).mean().item()
    low_volatility = (h1 - l1) < avg_range * 0.65

    buy_score = 0
    sell_score = 0

    # EMA alignment = trend confirmation
    if ema9 < ema21 < ema50:
        sell_score += 12
        reasons.append("EMA bearish alignment +12")
    elif ema9 > ema21 > ema50:
        buy_score += 12
        reasons.append("EMA bullish alignment +12")

    if ema9 < ema21 and c1 < ema21:
        buy_score -= 8
        reasons.append("Buy pressure reduced by bearish EMA context -8")

    if ema9 > ema21 and c1 > ema21:
        sell_score -= 8
        reasons.append("Sell pressure reduced by bullish EMA context -8")
        # -------------------------
    # HTF / STRUCTURE BIAS
    # -------------------------
    if htf_structure == "BULLISH":
        buy_score += 30
        reasons.append("HTF bullish +30")
    elif htf_structure == "BEARISH":
        sell_score += 30
        reasons.append("HTF bearish +30")

    if htf_structure == "BEARISH":
        buy_score -= 12
        reasons.append("Buy reduced by bearish HTF -12")
    elif htf_structure == "BULLISH":
        sell_score -= 12
        reasons.append("Sell reduced by bullish HTF -12")

    if htf_structure == "BEARISH" and ema9 < ema21:
        bullish_reversal_ready = (
            choch_signal == "BULLISH"
            and recent_pressure == "BULLISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
            and c1 > ema9
        )

        if not bullish_reversal_ready:
            buy_score = 0

    elif htf_structure == "BULLISH" and ema9 > ema21:
        bearish_reversal_ready = (
            choch_signal == "BEARISH"
            and recent_pressure == "BEARISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
            and c1 < ema9
        )

        if not bearish_reversal_ready:
            sell_score = 0

    # TREND CONTINUATION BOOST
    if htf_structure == "BEARISH" and ema9 < ema21:
        sell_score += 12
        reasons.append("Bear trend continuation +12")

    elif htf_structure == "BULLISH" and ema9 > ema21:
        buy_score += 12
        reasons.append("Bull trend continuation +12")

            # STRONG TREND BOOST (UPGRADED)
    if htf_structure == "BEARISH" and ema9 < ema21:
        if momentum_strength == "STRONG":
            sell_score += 20
            reasons.append("Strong bearish trend +20")
        elif momentum_strength == "MEDIUM":
            sell_score += 10
            reasons.append("Medium bearish trend +10")

    elif htf_structure == "BULLISH" and ema9 > ema21:
        if momentum_strength == "STRONG":
            buy_score += 20
            reasons.append("Strong bullish trend +20")
        elif momentum_strength == "MEDIUM":
            buy_score += 10
            reasons.append("Medium bullish trend +10")

    if htf_structure == "BEARISH" and buy_score > sell_score:
        buy_score -= 10
        reasons.append("Buy trimmed against bearish HTF -10")

    if htf_structure == "BULLISH" and sell_score > buy_score:
        sell_score -= 10
        reasons.append("Sell trimmed against bullish HTF -10")

    if choch_signal == "BULLISH":
        buy_score += 14
        reasons.append("CHOCH bullish +14")
    elif choch_signal == "BEARISH":
        sell_score += 14
        reasons.append("CHOCH bearish +14")

        # EARLY REVERSAL UNLOCK
    if htf_structure == "BEARISH":
        bullish_reversal_ready = (
            choch_signal == "BULLISH"
            and recent_pressure == "BULLISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
            and c1 > ema9
        )

        if bullish_reversal_ready:
            buy_score += 18
            reasons.append("Early bullish reversal unlock +18")

    if htf_structure == "BULLISH":
        bearish_reversal_ready = (
            choch_signal == "BEARISH"
            and recent_pressure == "BEARISH"
            and momentum_strength in ["MEDIUM", "STRONG"]
            and c1 < ema9
        )

        if bearish_reversal_ready:
            sell_score += 18
            reasons.append("Early bearish reversal unlock +18")

    if market_state in ["BULL_PULLBACK", "BULL_REVERSAL", "BULL_EXPANSION"]:
        buy_score += 14
        reasons.append(f"{market_state} +14")
    elif market_state in ["BEAR_PULLBACK", "BEAR_REVERSAL", "BEAR_EXPANSION"]:
        sell_score += 14
        reasons.append(f"{market_state} +14")

    if recent_pressure == "BULLISH":
        buy_score += 12
        reasons.append(f"Pressure bullish +12 ({pressure_score})")
    elif recent_pressure == "BEARISH":
        sell_score += 12
        reasons.append(f"Pressure bearish +12 ({pressure_score})")

    if momentum_strength == "STRONG":
        if recent_pressure == "BULLISH":
            buy_score += 16
            reasons.append(f"Strong bullish momentum +12 ({momentum_score})")
        elif recent_pressure == "BEARISH":
            sell_score += 16
            reasons.append(f"Strong bearish momentum +12 ({momentum_score})")
    elif momentum_strength == "MEDIUM":
        if recent_pressure == "BULLISH":
            buy_score += 10
            reasons.append(f"Medium bullish momentum +7 ({momentum_score})")
        elif recent_pressure == "BEARISH":
            sell_score += 10
            reasons.append(f"Medium bearish momentum +7 ({momentum_score})")

    # -------------------------
    # PRICE DELIVERY / ZONE
    # -------------------------
    trap_signal = detect_liquidity_grab(data)
    if zone_position == "DISCOUNT" and htf_structure == "BULLISH":
        buy_score += 10
        reasons.append("Discount zone +10")
    elif zone_position == "PREMIUM" and htf_structure == "BEARISH":
        sell_score += 10
        reasons.append("Premium zone +10")

    if trap_signal == "BULLISH":
        buy_score += 10
        reasons.append("Bullish liquidity trap +10")

        if recent_pressure == "BULLISH":
            buy_score += 8
            reasons.append("Bullish trap + bullish pressure +8")

        if choch_signal == "BULLISH":
            buy_score += 8
            reasons.append("Bullish trap + bullish CHOCH +8")

        if zone_position == "DISCOUNT":
            buy_score += 6
            reasons.append("Bullish trap in discount zone +6")

        if htf_structure == "BULLISH":
            buy_score += 6
            reasons.append("Bullish trap with bullish HTF +6")

    elif trap_signal == "BEARISH":
        sell_score += 10
        reasons.append("Bearish liquidity trap +10")

        if recent_pressure == "BEARISH":
            sell_score += 8
            reasons.append("Bearish trap + bearish pressure +8")

        if choch_signal == "BEARISH":
            sell_score += 8
            reasons.append("Bearish trap + bearish CHOCH +8")

        if zone_position == "PREMIUM":
            sell_score += 6
            reasons.append("Bearish trap in premium zone +6")

        if htf_structure == "BEARISH":
            sell_score += 6
            reasons.append("Bearish trap with bearish HTF +6")
    # -------------------------
    # ENTRY TIMING
    # -------------------------
    if entry_timing == "GOOD_BUY":
        buy_score += 12
        reasons.append("Good buy timing +12")
    elif entry_timing == "GOOD_SELL":
        sell_score += 12
        reasons.append("Good sell timing +12")
    elif entry_timing == "WAIT_PULLBACK_BUY":
        if htf_structure == "BEARISH":
            buy_score -= 10
            reasons.append("Blocked pullback buy in bearish trend -10")
        else:
            buy_score -= 4
            reasons.append("Wait pullback buy -4")
    elif entry_timing == "WAIT_PULLBACK_SELL":
        if htf_structure == "BULLISH":
            sell_score -= 10
            reasons.append("Blocked pullback sell in bullish trend -10")
        else:
            sell_score -= 4
            reasons.append("Wait pullback sell -4")
    elif entry_timing == "LATE_BUY":
        buy_score -= 8
        reasons.append("Late buy -8")
    elif entry_timing == "LATE_SELL":
        sell_score -= 8
        reasons.append("Late sell -8")

    # -------------------------
    # MARKET CONDITION PENALTIES
    # -------------------------
    if market_condition == "CHOPPY":
        buy_score -= 4
        sell_score -= 4
        reasons.append("Choppy market -8/-8")
    elif market_condition == "MIXED":
        buy_score -= 4
        sell_score -= 4
        reasons.append("Mixed market -4/-4")

    # slight fallback so neutral trend can still trade if momentum is real
    if htf_structure == "NEUTRAL":
        if recent_pressure == "BULLISH" and momentum_strength in ["MEDIUM", "STRONG"]:
            buy_score += 8
            reasons.append("Neutral HTF bullish fallback +8")
        elif recent_pressure == "BEARISH" and momentum_strength in ["MEDIUM", "STRONG"]:
            sell_score += 8
            reasons.append("Neutral HTF bearish fallback +8")

    # ==============================
    # 🔥 SYMBOL-SPECIFIC CONTROL
    # ==============================

    if symbol == "EURUSD":
        WAIT_PULLBACK_CAP = 68
        LATE_CAP = 58
        CHOPPY_FACTOR = 0.78
        WEAK_FACTOR = 0.88

    elif symbol == "XAUUSD":
        WAIT_PULLBACK_CAP = 74
        LATE_CAP = 64
        CHOPPY_FACTOR = 0.82
        WEAK_FACTOR = 0.92

    else:
        # fallback (safety)
        WAIT_PULLBACK_CAP = 70
        LATE_CAP = 60
        CHOPPY_FACTOR = 0.70
        WEAK_FACTOR = 0.85

    if symbol == "XAUUSD":
        if buy_score == 0 and sell_score == 0:
            # detect last candle direction
            if data["close"].iloc[-1] > data["open"].iloc[-1]:
                buy_score += 10
            else:
                sell_score += 10

    # ==============================
    # 🔥 MARKET CONDITION FILTER
    # ==============================

    if market_condition == "CHOPPY":
    # only reduce if NO strong direction
     if abs(buy_score - sell_score) < 15:
        buy_score *= 0.6
        sell_score *= 0.6

    elif market_condition == "WEAK":
        buy_score *= WEAK_FACTOR
        sell_score *= WEAK_FACTOR


    # ==============================
    # 🔥 ENTRY TIMING CAPS
    # ==============================

    if entry_timing == "WAIT_PULLBACK_BUY":
        buy_score = min(buy_score, WAIT_PULLBACK_CAP)

    if entry_timing == "WAIT_PULLBACK_SELL":
        sell_score = min(sell_score, WAIT_PULLBACK_CAP)

    if entry_timing == "LATE_BUY":
        buy_score = min(buy_score, LATE_CAP)

    if entry_timing == "LATE_SELL":
        sell_score = min(sell_score, LATE_CAP)


    # ==============================
    # 🔥 FINAL NORMALIZATION
    # ==============================

    buy_score = max(0, min(int(buy_score), 92))
    sell_score = max(0, min(int(sell_score), 92))


    # ==============================
    # 🔥 GAP CALCULATION
    # ==============================

    score_gap = abs(buy_score - sell_score)

    no_trade, nt_reasons = apply_no_trade_logic(
        buy_score,
        sell_score,
        market_condition,
        low_volatility,
        zone_position,
        htf_structure,
        choch_signal,
        momentum_strength,
        recent_pressure
    )
    reasons.extend(nt_reasons)

    fake_kill, fk_reasons = apply_fake_signal_killer(
        symbol,
        buy_score,
        sell_score,
        score_gap,
        market_condition,
        market_state,
        entry_timing,
        htf_structure,
        choch_signal,
        recent_pressure,
        momentum_strength,
        zone_position,
        c1,
        ema9,
        ema21,
        recent_high,
        recent_low
    )
    reasons.extend(fk_reasons)

    final_signal = "WAIT"

    if not no_trade and not fake_kill:
        strong_buy_context = (
            buy_score >= 80
            and buy_score > sell_score
            and score_gap >= 18
            and htf_structure == "BULLISH"
            and momentum_strength == "STRONG"
            and recent_pressure == "BULLISH"
            and market_state in ["BULL_EXPANSION", "BULL_REVERSAL"]
        )

        strong_sell_context = (
            sell_score >= 80
            and sell_score > buy_score
            and score_gap >= 18
            and htf_structure == "BEARISH"
            and momentum_strength == "STRONG"
            and recent_pressure == "BEARISH"
            and market_state in ["BEAR_EXPANSION", "BEAR_REVERSAL"]
        )

        if buy_score >= 55 and buy_score > sell_score and score_gap >= 10:
            if entry_timing == "GOOD_BUY":
                final_signal = "BUY"
            elif entry_timing == "WAIT_PULLBACK_BUY":
                final_signal = "WAIT"
            elif entry_timing == "LATE_BUY":
                final_signal = "WAIT"
            elif strong_buy_context and entry_timing == "NEUTRAL":
                final_signal = "BUY"

        elif sell_score >= 55 and sell_score > buy_score and score_gap >= 10:
            if entry_timing == "GOOD_SELL":
                final_signal = "SELL"
            elif entry_timing == "WAIT_PULLBACK_SELL":
                final_signal = "WAIT"
            elif entry_timing == "LATE_SELL":
                final_signal = "WAIT"
            elif strong_sell_context and entry_timing == "NEUTRAL":
                final_signal = "SELL"

    entry_quality = get_entry_quality(
        buy_score,
        sell_score,
        score_gap,
        market_condition,
        zone_position,
        htf_structure,
        choch_signal
    )

    confidence = calculate_confidence(
        buy_score,
        sell_score,
        htf_structure,
        choch_signal,
        zone_position,
        trap_signal,
        market_state,
        entry_timing,
        market_condition
    )

    display_entry_timing = entry_timing

    top_side = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "NONE"

    if top_side == "SELL" and entry_timing in ["GOOD_BUY", "WAIT_PULLBACK_BUY", "LATE_BUY"]:
        display_entry_timing = "NEUTRAL"

    elif top_side == "BUY" and entry_timing in ["GOOD_SELL", "WAIT_PULLBACK_SELL", "LATE_SELL"]:
        display_entry_timing = "NEUTRAL"

    if final_signal == "BUY":
        signal_text = f"BUY 🟢 ({int(buy_score)}% | {entry_quality} | {display_entry_timing})"
    elif final_signal == "SELL":
        signal_text = f"SELL 🔴 ({int(sell_score)}% | {entry_quality} | {display_entry_timing})"
    else:
        signal_text = f"WAIT ⚪ ({market_condition} | {entry_quality} | {display_entry_timing})"

    return {
        "signal": final_signal,
        "signal_text": signal_text,
        "buy_pct": int(max(0, min(buy_score, 100))),
        "sell_pct": int(max(0, min(sell_score, 100))),
        "confidence": int(confidence),
        "market_condition": market_condition,
        "entry_quality": entry_quality,
        "entry_timing": display_entry_timing,
        "fake_kill": bool(fake_kill),
        "debug_reasons": reasons[-14:],
        "price": c1
    }


# =========================
# 🔗 PANEL DATA
# =========================
def _df_to_candles(df, limit=120):
    if df is None or df.empty:
        return []

    df = df.tail(limit).copy()
    candles = []

    for idx, row in df.iterrows():
        try:
            ts = int(pd.Timestamp(idx).timestamp())
            candles.append({
                "time": ts,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
            })
        except Exception:
            continue

    return candles

def _minutes_since_last_candle(df):
    if df is None or df.empty:
        return None

    try:
        last_idx = pd.Timestamp(df.index[-1])
        if last_idx.tzinfo is None:
            last_idx = last_idx.tz_localize("UTC")
        else:
            last_idx = last_idx.tz_convert("UTC")

        now_utc = datetime.now(timezone.utc)
        age_minutes = (now_utc - last_idx.to_pydatetime()).total_seconds() / 60
        return round(age_minutes, 1)
    except Exception:
        return None


def _is_feed_stale(df, max_age_minutes=20):
    age = _minutes_since_last_candle(df)
    if age is None:
        return True
    return age > max_age_minutes

def is_market_calendar_closed():
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    hour = now.hour

    if weekday == 5:  # Saturday
        return True

    if weekday == 6 and hour < 22:  # Sunday before 22:00 UTC
        return True

    if weekday == 4 and hour >= 22:  # Friday after 22:00 UTC
        return True

    return False

def _make_closed_result(symbol, base_result=None, stale_minutes=None):
    result = copy.deepcopy(base_result) if isinstance(base_result, dict) else {}

    result["signal"] = "WAIT"
    result["signal_text"] = f"WAIT ⚪ (market closed)"
    result["market_condition"] = "MARKET_CLOSED"
    result["entry_quality"] = result.get("entry_quality", "WEAK")
    result["entry_timing"] = "CLOSED"
    result["market_closed"] = True
    result["stale_minutes"] = stale_minutes

     # market closed = no fake signal strength
    result["buy_pct"] = 0
    result["sell_pct"] = 0
    result["confidence"] = 0

    result.setdefault("debug_reasons", [])
    result["debug_reasons"] = list(result["debug_reasons"]) + ["Market closed / stale feed"]

    return result

def get_panel_data():
    eurusd, gold, eurusd_htf, gold_htf, eurusd_1h, gold_1h = fetch_market_data()
    if not hasattr(get_panel_data, "_last_open_payload"):
        get_panel_data._last_open_payload = None

    eurusd_stale_minutes = _minutes_since_last_candle(eurusd)
    gold_stale_minutes = _minutes_since_last_candle(gold)

    calendar_closed = is_market_calendar_closed()

    eurusd_closed = calendar_closed or _is_feed_stale(eurusd, max_age_minutes=20)
    gold_closed = calendar_closed or _is_feed_stale(gold, max_age_minutes=20)

    print("Calendar closed:", calendar_closed)
    print("EURUSD closed:", eurusd_closed)
    print("GOLD closed:", gold_closed)

    all_closed = eurusd_closed and gold_closed

    # =========================
    # MARKET CLOSED / STALE FEED MODE
    # =========================
    if all_closed and get_panel_data._last_open_payload is not None:
        payload = copy.deepcopy(get_panel_data._last_open_payload)

        payload["EURUSD"] = _make_closed_result(
            "EURUSD",
            payload.get("EURUSD"),
            stale_minutes=eurusd_stale_minutes
        )
        payload["GOLD"] = _make_closed_result(
            "GOLD",
            payload.get("GOLD"),
            stale_minutes=gold_stale_minutes
        )

        payload["market_closed"] = True
        payload["feed_status"] = {
            "EURUSD": {
                "market_closed": True,
                "stale_minutes": eurusd_stale_minutes,
            },
            "GOLD": {
                "market_closed": True,
                "stale_minutes": gold_stale_minutes,
            },
        }

        return payload

    # =========================
    # NORMAL LIVE MODE
    # =========================
    eurusd_result = get_signal(eurusd, eurusd_htf, "EURUSD")
    gold_result = get_signal(gold, gold_htf, "GOLD")

    if eurusd_closed:
        eurusd_result = _make_closed_result(
            "EURUSD",
            eurusd_result,
            stale_minutes=eurusd_stale_minutes
        )
    else:
        eurusd_result["market_closed"] = False
        eurusd_result["stale_minutes"] = eurusd_stale_minutes

    if gold_closed:
        gold_result = _make_closed_result(
            "GOLD",
            gold_result,
            stale_minutes=gold_stale_minutes
        )
    else:
        gold_result["market_closed"] = False
        gold_result["stale_minutes"] = gold_stale_minutes

    # only update history/results when that symbol feed is live
    if not eurusd_closed:
        update_signal_history("EURUSD", eurusd_result)
        update_trade_results(eurusd, "EURUSD")

    if not gold_closed:
        update_signal_history("GOLD", gold_result)
        update_trade_results(gold, "GOLD")

    payload = {
        "EURUSD": eurusd_result,
        "GOLD": gold_result,
        "candles": {
            "EURUSD": {
                "5m": _df_to_candles(eurusd, limit=120),
                "15m": _df_to_candles(eurusd_htf, limit=120),
                "1h": _df_to_candles(eurusd_1h, limit=120),
            },
            "GOLD": {
                "5m": _df_to_candles(gold, limit=120),
                "15m": _df_to_candles(gold_htf, limit=120),
                "1h": _df_to_candles(gold_1h, limit=120),
            }
        },
        "history": SIGNAL_HISTORY[-20:],
        "market_closed": all_closed,
        "feed_status": {
            "EURUSD": {
                "market_closed": eurusd_closed,
                "stale_minutes": eurusd_stale_minutes,
            },
            "GOLD": {
                "market_closed": gold_closed,
                "stale_minutes": gold_stale_minutes,
            },
        }
    }

    # save last valid fully-live payload
    if not all_closed:
        get_panel_data._last_open_payload = copy.deepcopy(payload)

    return payload

# =========================
# 🕘 SIGNAL HISTORY
# =========================
SIGNAL_HISTORY = []


def update_signal_history(symbol, result):

    global SIGNAL_HISTORY

    entry = {
        "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "symbol": symbol,
        "signal": result.get("signal", "WAIT"),
        "signal_text": result.get("signal_text", ""),
        "buy_pct": result.get("buy_pct", 0),
        "sell_pct": result.get("sell_pct", 0),
        "confidence": result.get("confidence", 0),
        "market_condition": result.get("market_condition", "UNKNOWN"),
        "entry_quality": result.get("entry_quality", "WEAK"),
        "entry_timing": result.get("entry_timing", "NEUTRAL"),
        "fake_kill": result.get("fake_kill", False),

         "entry_price": None,
        "result": "RUNNING",
        "pips": 0,
    }


    if SIGNAL_HISTORY:
        last = SIGNAL_HISTORY[-1]
        same_entry = (
            last["symbol"] == entry["symbol"]
            and last["signal"] == entry["signal"]
            and last["buy_pct"] == entry["buy_pct"]
            and last["sell_pct"] == entry["sell_pct"]
            and last["confidence"] == entry["confidence"]
            and last["entry_quality"] == entry["entry_quality"]
            and last["entry_timing"] == entry["entry_timing"]
        )
        if same_entry:
            return

    SIGNAL_HISTORY.append(entry)
    if entry["signal"] in ["BUY", "SELL"]:
        entry["entry_price"] = result.get("price", None)

    if len(SIGNAL_HISTORY) > 20:
        SIGNAL_HISTORY = SIGNAL_HISTORY[-20:]

def update_trade_results(data, symbol):
    global SIGNAL_HISTORY

    if not SIGNAL_HISTORY or data.empty:
        return

    current_price = data["Close"].iloc[-1].item()

    for trade in SIGNAL_HISTORY:
        if trade["symbol"] != symbol:
            continue

        if trade["result"] != "RUNNING":
            continue

        entry_price = trade.get("entry_price")

        if entry_price is None:
            continue

        if symbol == "EURUSD":
            pips = (current_price - entry_price) * 10000
        else:
            pips = (current_price - entry_price)

        if trade["signal"] == "SELL":
            pips *= -1

        trade["pips"] = round(pips, 1)

        if pips >= 10:
            trade["result"] = "WIN"
        elif pips <= -10:
            trade["result"] = "LOSS"
    
# =========================
# 🧪 TEST MODE
# =========================
if __name__ == "__main__":
    panel_data = get_panel_data()

    print("===== SIGNALS =====")
    for symbol in ["EURUSD", "GOLD"]:
        result = panel_data[symbol]
        print(f"{symbol}: {result['signal_text']}")
        print(f"Buy: {result['buy_pct']}%")
        print(f"Sell: {result['sell_pct']}%")
        print(f"Confidence: {result['confidence']}%")
        print(f"Market Condition: {result['market_condition']}")
        print(f"Entry Quality: {result['entry_quality']}")
        print()