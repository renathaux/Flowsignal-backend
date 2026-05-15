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

    if len(data) < 8:
        return "NEUTRAL", 0

    recent = data.tail(8)

    bull_count = 0
    bear_count = 0

    for _, candle in recent.iterrows():
        if candle["Close"] > candle["Open"]:
            bull_count += 1
        elif candle["Close"] < candle["Open"]:
            bear_count += 1

    c1 = close.iloc[-1].item()
    o1 = open_.iloc[-1].item()
    h1 = high.iloc[-1].item()
    l1 = low.iloc[-1].item()

    body1 = abs(c1 - o1)
    candle_range = h1 - l1

    if symbol == "EURUSD":
        strong_body = 0.0004
    else:
        strong_body = 1.5

    pressure_score = 0

    if bull_count >= 5:
        pressure_score += 3
    elif bull_count >= 4:
        pressure_score += 2

    if bear_count >= 5:
        pressure_score -= 3
    elif bear_count >= 4:
        pressure_score -= 2

    if c1 > o1:
        pressure_score += 1
    elif c1 < o1:
        pressure_score -= 1

    if body1 > strong_body:
        if c1 > o1:
            pressure_score += 2
        elif c1 < o1:
            pressure_score -= 2

    if candle_range > 0:
        close_position = (c1 - l1) / candle_range

        if close_position >= 0.7:
            pressure_score += 1
        elif close_position <= 0.3:
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

    if market_condition in ["CHOPPY", "MIXED", "UNKNOWN"] and momentum_strength == "WEAK":
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

# =========================
# 🧭 MULTI-TIMEFRAME CONFLUENCE
# =========================
def detect_mtf_confluence(data_5m, data_15m, symbol):
    if data_5m.empty or data_15m.empty or len(data_15m) < 30:
        return "NEUTRAL", 0

    close_15 = data_15m["Close"]

    ema9_15 = close_15.ewm(span=9).mean().iloc[-1].item()
    ema21_15 = close_15.ewm(span=21).mean().iloc[-1].item()
    ema50_15 = close_15.ewm(span=50).mean().iloc[-1].item()

    htf_structure = detect_htf_structure(data_15m)

    score = 0

    if ema9_15 > ema21_15 > ema50_15:
        score += 2
        bias = "BULLISH"
    elif ema9_15 < ema21_15 < ema50_15:
        score -= 2
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    if htf_structure == "BULLISH":
        score += 2
    elif htf_structure == "BEARISH":
        score -= 2

    if score >= 3:
        return "BULLISH", abs(score)

    if score <= -3:
        return "BEARISH", abs(score)

    return bias, abs(score)

# =========================
# 🧠 SMART CONFIDENCE ENGINE
# =========================
def calculate_smart_confidence(
    session_active,
    bullish_fvg,
    bearish_fvg,
    bullish_displacement,
    bearish_displacement,
    fake_breakout,
    mtf_bias,
    signal_side
):
    confidence = 50

    # Session
    if session_active:
        confidence += 10
    else:
        confidence -= 8

    # FVG
    if signal_side == "BUY" and bullish_fvg:
        confidence += 8

    if signal_side == "SELL" and bearish_fvg:
        confidence += 8

    # Displacement
    if signal_side == "BUY" and bullish_displacement:
        confidence += 10

    if signal_side == "SELL" and bearish_displacement:
        confidence += 10

    # Fake breakout penalty
    if fake_breakout != "NONE":
        confidence -= 18

    # MTF alignment
    if signal_side == "BUY":
        if mtf_bias == "BULLISH":
            confidence += 12
        elif mtf_bias == "BEARISH":
            confidence -= 12

    if signal_side == "SELL":
        if mtf_bias == "BEARISH":
            confidence += 12
        elif mtf_bias == "BULLISH":
            confidence -= 12

    return max(1, min(99, confidence))


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

    swing_points = []
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

            swing_points.append({
                "type": "HIGH",
                "price": float(h),
                "index": int(i),
                "time": int(pd.Timestamp(data.index[i]).timestamp())
            })
        if l < l1 and l < l2 and l < l3 and l < l4:
            swing_lows.append(l)
            swing_points.append({
                "type": "LOW",
                "price": float(l),
                "index": int(i),
                "time": int(pd.Timestamp(data.index[i]).timestamp())
            })
    last_swing_high = swing_highs[-1] if len(swing_highs) >= 1 else None
    prev_swing_high = swing_highs[-2] if len(swing_highs) >= 2 else None

    last_swing_low = swing_lows[-1] if len(swing_lows) >= 1 else None
    prev_swing_low = swing_lows[-2] if len(swing_lows) >= 2 else None

    market_structure = "RANGE"

    if (
        last_swing_high is not None
        and prev_swing_high is not None
        and last_swing_low is not None
        and prev_swing_low is not None
    ):
        if last_swing_high > prev_swing_high and last_swing_low > prev_swing_low:
            market_structure = "BULLISH"

        elif last_swing_high < prev_swing_high and last_swing_low < prev_swing_low:
            market_structure = "BEARISH"

    return (
        last_swing_high,
        last_swing_low,
        prev_swing_high,
        prev_swing_low,
        market_structure,
        swing_points[-12:]
    )

def detect_equal_levels(data, tolerance):

    highs = data["High"].tolist()
    lows = data["Low"].tolist()

    equal_highs = []
    equal_lows = []

    for i in range(len(highs) - 1):
        for j in range(i + 1, len(highs)):
            if abs(highs[i] - highs[j]) <= tolerance:
                equal_highs.append(round((highs[i] + highs[j]) / 2, 5))

    for i in range(len(lows) - 1):
        for j in range(i + 1, len(lows)):
            if abs(lows[i] - lows[j]) <= tolerance:
                equal_lows.append(round((lows[i] + lows[j]) / 2, 5))

    return {
        "equal_highs": equal_highs[-5:],
        "equal_lows": equal_lows[-5:]
    }


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
TWELVE_DATA_API_KEY = "6cdda0e63fd34eb586552edf157a188b"

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
    calendar_closed = is_market_calendar_closed()

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
    try:
        if need_5m and (not calendar_closed or cache["eurusd_5m"].empty or cache["gold_5m"].empty):
            print("===== REFRESHING 5M DATA =====")
            cache["eurusd_5m"] = safe_download("EUR/USD", "5min", outputsize=5000)
            cache["gold_5m"] = safe_download("XAU/USD", "5min", outputsize=5000)
            cache["last_5m_update"] = now
        else:
            print("===== USING CACHED 5M DATA =====")

        if need_15m and (not calendar_closed or cache["eurusd_15m"].empty or cache["gold_15m"].empty):
            print("===== REFRESHING 15M DATA =====")
            cache["eurusd_15m"] = safe_download("EUR/USD", "15min", outputsize=5000)
            cache["gold_15m"] = safe_download("XAU/USD", "15min", outputsize=5000)
            cache["last_15m_update"] = now
        else:
            print("===== USING CACHED 15M DATA =====")

        if need_1h and (not calendar_closed or cache["eurusd_1h"].empty or cache["gold_1h"].empty):
            print("===== REFRESHING 1H DATA =====")
            cache["eurusd_1h"] = safe_download("EUR/USD", "1h", outputsize=5000)
            cache["gold_1h"] = safe_download("XAU/USD", "1h", outputsize=5000)
            cache["last_1h_update"] = now
        else:
            print("===== USING CACHED 1H DATA =====")

    finally:
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

def detect_market_mode(
    data,
    htf_data,
    ema9,
    ema21,
    ema50,
    htf_structure,
    recent_pressure,
    momentum_strength,
    symbol
):
    close = data["Close"]
    high = data["High"]
    low = data["Low"]

    if len(data) < 12 or len(htf_data) < 8:
        return "UNKNOWN"

    c1 = close.iloc[-1].item()
    c2 = close.iloc[-2].item()

    recent_high = high.iloc[-8:-1].max().item()
    recent_low = low.iloc[-8:-1].min().item()

    bullish_ema = ema9 > ema21 > ema50
    bearish_ema = ema9 < ema21 < ema50

    bullish_breakout = c1 > recent_high and c1 > c2
    bearish_breakout = c1 < recent_low and c1 < c2

    ema_spread = abs(ema9 - ema21)

    if symbol == "EURUSD":
        flat_limit = 0.00018
    else:
        flat_limit = 1.2

    if ema_spread < flat_limit and momentum_strength == "WEAK":
        return "RANGE"

    if htf_structure == "BULLISH" and bullish_ema:
        if bullish_breakout and momentum_strength in ["MEDIUM", "STRONG"]:
            return "BREAKOUT_BULL"
        if recent_pressure in ["BULLISH", "NEUTRAL"]:
            return "TREND_CONTINUATION_BULL"

    if htf_structure == "BEARISH" and bearish_ema:
        if bearish_breakout and momentum_strength in ["MEDIUM", "STRONG"]:
            return "BREAKOUT_BEAR"
        if recent_pressure in ["BEARISH", "NEUTRAL"]:
            return "TREND_CONTINUATION_BEAR"

    if htf_structure == "BULLISH" and recent_pressure == "BEARISH" and c1 < ema21:
        return "SHIFT_BEAR"

    if htf_structure == "BEARISH" and recent_pressure == "BULLISH" and c1 > ema21:
        return "SHIFT_BULL"

    return "MIXED"

SMC_STATE = {
    "EURUSD": {
       "pending": None,
        "stage": "WAIT",
        "bos_level": None,
        "sweep_level": None,
        "last_idea": None
        },
    "GOLD": {
        "pending": None,
        "stage": "WAIT",
        "bos_level": None,
        "sweep_level": None,
        "last_idea": None
            }
        }

# =========================
# 🤖 PAPER AUTO-TRADER
# =========================
AUTO_TRADES = {
    "EURUSD": None,
    "GOLD": None,
}

PAPER_TRADE_HISTORY = []

# =========================
# 🧱 FVG + SESSION HELPERS
# =========================
def detect_fvg(data, symbol):
    if len(data) < 5:
        return None

    candles = data.tail(8)

    if symbol == "EURUSD":
        min_gap = 0.00015
    else:
        min_gap = 0.80

    fvgs = []

    for i in range(2, len(candles)):
        c0 = candles.iloc[i - 2]
        c2 = candles.iloc[i]

        # Bullish FVG: candle 3 low is above candle 1 high
        if c2["Low"] > c0["High"]:
            gap = c2["Low"] - c0["High"]
            if gap >= min_gap:
                fvgs.append({
                    "type": "BULLISH",
                    "low": float(c0["High"]),
                    "high": float(c2["Low"]),
                    "mid": float((c0["High"] + c2["Low"]) / 2)
                })

        # Bearish FVG: candle 3 high is below candle 1 low
        if c2["High"] < c0["Low"]:
            gap = c0["Low"] - c2["High"]
            if gap >= min_gap:
                fvgs.append({
                    "type": "BEARISH",
                    "low": float(c2["High"]),
                    "high": float(c0["Low"]),
                    "mid": float((c2["High"] + c0["Low"]) / 2)
                })

    return fvgs[-1] if fvgs else None


def is_in_fvg_retest(price, fvg):
    if not fvg:
        return False

    return fvg["low"] <= price <= fvg["high"]


def get_session_state():
    now = datetime.now(timezone.utc)
    hour = now.hour

    # Main forex activity windows UTC
    london = 7 <= hour < 11
    new_york = 13 <= hour < 17
    overlap = 13 <= hour < 16

    if overlap:
        return "NY_LONDON_OVERLAP", True

    if london:
        return "LONDON", True

    if new_york:
        return "NEW_YORK", True

    return "LOW_ACTIVITY", False

# =========================
# ⚡ DISPLACEMENT ENGINE
# =========================
def detect_displacement(data, symbol):
    if len(data) < 8:
        return "WEAK", 0

    last = data.iloc[-1]
    recent = data.tail(8)

    body = abs(last["Close"] - last["Open"])
    candle_range = last["High"] - last["Low"]

    if candle_range <= 0:
        return "WEAK", 0

    avg_range = (recent["High"] - recent["Low"]).mean()
    body_ratio = body / candle_range

    if symbol == "EURUSD":
        min_body = 0.00025
    else:
        min_body = 1.0

    score = 0

    if body >= min_body:
        score += 2

    if candle_range >= avg_range * 1.25:
        score += 2

    if body_ratio >= 0.65:
        score += 3
    elif body_ratio >= 0.50:
        score += 2

    if last["Close"] > last["Open"]:
        direction = "BULLISH"
    elif last["Close"] < last["Open"]:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    if score >= 6:
        return f"STRONG_{direction}", score
    elif score >= 4:
        return f"MEDIUM_{direction}", score
    else:
        return f"WEAK_{direction}", score

# =========================
# 🧨 FAKE BREAKOUT KILLER
# =========================
def detect_fake_breakout(c1, o1, h1, l1, recent_high, recent_low, symbol):
    body = abs(c1 - o1)
    candle_range = h1 - l1

    if candle_range <= 0:
        return "NONE"

    upper_wick = h1 - max(c1, o1)
    lower_wick = min(c1, o1) - l1

    if symbol == "EURUSD":
        reclaim_buffer = 0.00015
    else:
        reclaim_buffer = 0.80

    fake_bull_breakout = (
        h1 > recent_high
        and c1 < recent_high - reclaim_buffer
        and upper_wick > body * 1.8
    )

    fake_bear_breakout = (
        l1 < recent_low
        and c1 > recent_low + reclaim_buffer
        and lower_wick > body * 1.8
    )

    if fake_bull_breakout:
        return "FAKE_BULL_BREAKOUT"

    if fake_bear_breakout:
        return "FAKE_BEAR_BREAKOUT"

    return "NONE"

def update_paper_trade(symbol, result, current_price):
    global AUTO_TRADES, PAPER_TRADE_HISTORY

    trade = AUTO_TRADES.get(symbol)

    def calc_pips(symbol, side, entry, close_price):
        if symbol == "EURUSD":
            pip_value = 0.0001
        else:
            pip_value = 1.0

        if side == "BUY":
            return round((close_price - entry) / pip_value, 1)
        else:
            return round((entry - close_price) / pip_value, 1)

    signal = result.get("signal")
    plan_side = result.get("plan_side") or result.get("plan_bias")

    is_buy_trade = signal == "BUY" or plan_side == "BUY"
    is_sell_trade = signal == "SELL" or plan_side == "SELL"

    # OPEN NEW TRADE
    if trade is None and (is_buy_trade or is_sell_trade):
        entry = result.get("entry_price")
        sl = result.get("stop_loss")
        tp1 = result.get("tp1")
        tp2 = result.get("tp2")

        if entry == "--" or sl == "--" or tp1 == "--" or tp2 == "--":
            print("PAPER BLOCKED:", symbol, signal, plan_side, entry, sl, tp1, tp2)
            return

        side = "BUY" if is_buy_trade else "SELL"

        new_trade = {
            "symbol": symbol,
            "side": side,
            "entry": float(entry),
            "sl": float(sl),
            "tp1": float(tp1),
            "tp2": float(tp2),
            "opened_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "closed_at": None,
            "closed_price": None,
            "status": "OPEN",
            "result": "RUNNING",
            "pips": 0,
            "hit_tp1": False,
        }

        AUTO_TRADES[symbol] = new_trade
        PAPER_TRADE_HISTORY.append(new_trade.copy())

        if len(PAPER_TRADE_HISTORY) > 50:
            PAPER_TRADE_HISTORY.pop(0)

        print("PAPER OPENED:", new_trade)
        return

    if trade is None:
        return

    side = trade["side"]
    entry = trade["entry"]
    sl = trade["sl"]
    tp1 = trade["tp1"]
    tp2 = trade["tp2"]

    current_price = float(current_price)

    # UPDATE RUNNING PIPS
    trade["pips"] = calc_pips(symbol, side, entry, current_price)

    # MANAGE BUY
    current_low = result.get("current_low", current_price)
    current_high = result.get("current_high", current_price)

    if side == "BUY":
        if current_low <= trade["sl"]:
            trade["status"] = "CLOSED"
            trade["result"] = "LOSS"
        elif current_high >= trade["tp2"]:
            trade["status"] = "CLOSED"
            trade["result"] = "WIN"
        elif current_high >= trade["tp1"]:
            trade["result"] = "TP1 HIT"

    # MANAGE SELL
    if side == "SELL":
        if current_high >= trade["sl"]:
            trade["status"] = "CLOSED"
            trade["result"] = "LOSS"
        elif current_low <= trade["tp2"]:
            trade["status"] = "CLOSED"
            trade["result"] = "WIN"
        elif current_low <= trade["tp1"]:
            trade["result"] = "TP1 HIT"

    # UPDATE HISTORY WHILE RUNNING
    if PAPER_TRADE_HISTORY:
        PAPER_TRADE_HISTORY[-1] = trade.copy()

    # CLOSE TRADE
    if trade["status"] == "CLOSED":
        trade["closed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        trade["closed_price"] = current_price
        trade["pips"] = calc_pips(symbol, side, entry, current_price)

        if PAPER_TRADE_HISTORY:
            PAPER_TRADE_HISTORY[-1] = trade.copy()
        else:
            PAPER_TRADE_HISTORY.append(trade.copy())

        print("PAPER CLOSED:", trade)

        AUTO_TRADES[symbol] = None

def get_signal(data, htf_data, symbol):
    if data.empty or htf_data.empty or len(data) < 30 or len(htf_data) < 10:
        return {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (not enough data)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "STRUCTURE",
            "entry_quality": "WAIT",
            "entry_timing": "WAIT",
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
        decimals = 5
        buffer = 0.00020
        retest_buffer = 0.00030
        late_entry_distance = 0.00120
        no_retest_max_distance = 0.00075
        equal_tolerance = 0.00015
    else:
        decimals = 2
        buffer = 1.20
        retest_buffer = 2.50
        late_entry_distance = 6.00
        no_retest_max_distance = 3.50
        equal_tolerance = 1.0

    reasons = []
    liquidity_levels = detect_equal_levels(
    data.tail(30),
    equal_tolerance
)

    fvg = detect_fvg(data, symbol)
    session_name, session_active = get_session_state()

    displacement, displacement_score = detect_displacement(data, symbol)

    bullish_displacement = displacement in ["STRONG_BULLISH", "MEDIUM_BULLISH"]
    bearish_displacement = displacement in ["STRONG_BEARISH", "MEDIUM_BEARISH"]

    bullish_fvg = fvg is not None and fvg["type"] == "BULLISH"
    bearish_fvg = fvg is not None and fvg["type"] == "BEARISH"

    price_in_fvg = is_in_fvg_retest(c1, fvg)

    buy_side_liquidity = liquidity_levels["equal_highs"]
    sell_side_liquidity = liquidity_levels["equal_lows"]

    nearest_buy_liquidity = (
        min([lvl for lvl in buy_side_liquidity if lvl > c1], default=None)
    )

    nearest_sell_liquidity = (
        max([lvl for lvl in sell_side_liquidity if lvl < c1], default=None)
    )

    (
        swing_high,
        swing_low,
        prev_swing_high,
        prev_swing_low,
        swing_structure,
        smc_swings
    ) = detect_swing_liquidity(data)

    recent_high = (
        prev_swing_high
        if prev_swing_high is not None
        else high.iloc[-20:-1].max().item()
    )

    recent_low = (
        prev_swing_low
        if prev_swing_low is not None
        else low.iloc[-20:-1].min().item()
    )

    fake_breakout = detect_fake_breakout(
        c1, o1, h1, l1, recent_high, recent_low, symbol
    )

    htf_structure = detect_htf_structure(htf_data)
    mtf_bias, mtf_score = detect_mtf_confluence(data, htf_data, symbol)
        # =========================
    # 15M TREND FILTER
    # =========================
    htf_close = htf_data["Close"]

    htf_ema9 = htf_close.ewm(span=9).mean().iloc[-1].item()
    htf_ema21 = htf_close.ewm(span=21).mean().iloc[-1].item()
    htf_ema50 = htf_close.ewm(span=50).mean().iloc[-1].item()

    htf_bullish = (
        htf_ema9 > htf_ema21 > htf_ema50
    )

    htf_bearish = (
        htf_ema9 < htf_ema21 < htf_ema50
    )

    bullish_sweep = (
    (
        swing_low is not None
        and l1 < swing_low
        and c1 > swing_low
        and c1 > o1
    )
    or
    (
        nearest_sell_liquidity is not None
        and l1 < nearest_sell_liquidity
        and c1 > nearest_sell_liquidity
        and c1 > o1
    )
)
    bearish_sweep = (
    (
        swing_high is not None
        and h1 > swing_high
        and c1 < swing_high
        and c1 < o1
    )
    or
    (
        nearest_buy_liquidity is not None
        and h1 > nearest_buy_liquidity
        and c1 < nearest_buy_liquidity
        and c1 < o1
    )
)

    bullish_choch = (
        htf_structure in ["BEARISH", "NEUTRAL"]
        and h1 > recent_high
        and c1 > o1
    )

    bearish_choch = (
        htf_structure in ["BULLISH", "NEUTRAL"]
        and l1 < recent_low
        and c1 < o1
    )

    bullish_bos = (
        htf_structure == "BULLISH"
        and h1 > recent_high
        and c1 > o1
    )

    bearish_bos = (
        htf_structure == "BEARISH"
        and l1 < recent_low
        and c1 < o1
    )

        # =========================
    # RECENT BOS MEMORY AFTER RESTART
    # =========================
    recent_lookback = data.tail(8)

    recent_bull_break = False
    recent_bear_break = False

    for _, candle in recent_lookback.iterrows():
        if candle["High"] > recent_high and candle["Close"] > candle["Open"]:
            recent_bull_break = True

        if candle["Low"] < recent_low and candle["Close"] < candle["Open"]:
            recent_bear_break = True

    # Do NOT force BOS from tiny recent candles.
# BOS must come from real protected swing break only.
    if recent_bull_break and h1 > recent_high and c1 > recent_high:
        bullish_bos = True
        bullish_choch = True

    if recent_bear_break and l1 < recent_low and c1 < recent_low:
        bearish_bos = True
        bearish_choch = True

    buy_retest = (
        swing_low is not None
        and l1 <= swing_low + retest_buffer
        and c1 > swing_low
        and c1 > o1
    )

    sell_retest = (
        swing_high is not None
        and h1 >= swing_high - retest_buffer
        and c1 < swing_high
        and c1 < o1
    )

    final_signal = "WAIT"
    buy_score = 0
    sell_score = 0
    confidence = 0
    entry_quality = "WAIT"
    entry_timing = "WAIT"
    plan_type = "WAIT FOR BOS"
    plan_side = "WAIT"

    if bullish_sweep:
        reasons.append("Bullish liquidity sweep detected")

    if bearish_sweep:
        reasons.append("Bearish liquidity sweep detected")

    if fake_breakout != "NONE":
      reasons.append(fake_breakout)

    state = SMC_STATE[symbol]
        # =========================
    # 🔄 STRUCTURE FLIP RESET
    # =========================
    if state["pending"] == "SELL" and (bullish_choch or bullish_bos):
        state["pending"] = "BUY"
        state["bos_level"] = recent_high
        state["sweep_level"] = swing_low
        state["stage"] = "BUY_READY"
        reasons.append("State flipped from SELL to BUY")

    if state["pending"] == "BUY" and (bearish_choch or bearish_bos):
        state["pending"] = "SELL"
        state["bos_level"] = recent_low
        state["sweep_level"] = swing_high
        state["stage"] = "SELL_READY"
        reasons.append("State flipped from BUY to SELL")

    # SAVE BOS / CHOCH FIRST
    # SAVE BOS / CHOCH FIRST
    if state["pending"] is None:

        if bullish_choch or bullish_bos:
            state["pending"] = "BUY"
            state["bos_level"] = recent_high
            state["sweep_level"] = swing_low
            reasons.append("Bullish CHOCH/BOS saved")

        elif bearish_choch or bearish_bos:
            state["pending"] = "SELL"
            state["bos_level"] = recent_low
            state["sweep_level"] = swing_high
            reasons.append("Bearish CHOCH/BOS saved")

        # =========================
    # ENTRY TIMING ENGINE
    # =========================

    if state["pending"] == "BUY" and state["bos_level"] is not None:
        in_buy_retest_zone = (
    c1 > state["bos_level"] - retest_buffer
)
        buy_displacement = (
                c1 > o1
                and c1 > state["bos_level"] - (retest_buffer * 0.3)
            )

        buy_no_retest_momentum = (
            c1 > o1
            and c1 > state["bos_level"]
            and (c1 - state["bos_level"]) <= no_retest_max_distance
            and (h1 - l1) > 0
            and ((c1 - o1) / (h1 - l1)) >= 0.60
        )

        if in_buy_retest_zone:

            if (
                bullish_choch
                or bullish_bos
                or buy_displacement
                or recent_pressure == "BULLISH"
            ):

                final_signal = "BUY"

                buy_score = 88
                sell_score = 8
                confidence = 82

                entry_quality = "SMC"
                entry_timing = "CHOCH/BOS RETEST"

                plan_type = "SMC BUY"
                plan_side = "BUY"

                state["pending"] = None
                state["stage"] = "BUY_ACTIVE"

            else:
                state["stage"] = "BUY_READY"

                plan_type = "BUY READY"
                plan_side = "WAIT"
                entry_timing = "BUY READY"

                buy_score = 65
                sell_score = 10
                confidence = 55

        elif (
            (
                (in_buy_retest_zone and buy_displacement)
                or buy_no_retest_momentum
                or
                (
                    state["stage"] == "BUY_READY"
                    and c1 > state["bos_level"]
                    and c1 > state["bos_level"]
                )
            )
           and not (
                    htf_structure == "BEARISH"
                    and recent_pressure != "BULLISH"
                    and momentum_strength == "WEAK"
                    and not bullish_bos
                    and not bullish_choch
                )
            ):
            distance_from_bos = c1 - state["bos_level"]
            # =========================
            # ANTI-FOMO FILTER
            # =========================

            recent_candle_range = h1 - l1
            recent_body = abs(c1 - o1)

            weak_breakout = (
                recent_body < (recent_candle_range * 0.35)
            )

            too_extended = (
                distance_from_bos > late_entry_distance
            )

            buying_into_liquidity = (
                nearest_buy_liquidity is not None
                and abs(nearest_buy_liquidity - c1) <= retest_buffer
            )

            if too_extended or weak_breakout or buying_into_liquidity:
                final_signal = "WAIT"
                buy_score = 75
                sell_score = 10
                confidence = 70
                entry_quality = "LATE"
                entry_timing = "WAIT PULLBACK"
                plan_type = "BUY HOLDING"
                plan_side = "WAIT"
                reasons.append("Bullish trend active but entry is late")
            else:
                if fake_breakout == "FAKE_BULL_BREAKOUT":
                    final_signal = "WAIT"
                    buy_score = 25
                    sell_score = 65
                    confidence = 72
                    entry_quality = "FAKE"
                    entry_timing = "FAKE BUY BREAKOUT"
                    plan_type = "FAKE BUY BREAKOUT"
                    plan_side = "WAIT"
                    reasons.append("Buy blocked: fake bullish breakout")

                elif mtf_bias == "BEARISH":
                    final_signal = "WAIT"
                    buy_score = 55
                    sell_score = 35
                    confidence = 58
                    entry_quality = "CONFLICT"
                    entry_timing = "WAIT MTF ALIGNMENT"
                    plan_type = "BUY CONFLICT WITH HTF"
                    plan_side = "WAIT"
                    reasons.append(f"BUY blocked by MTF bearish bias ({mtf_score})")

                elif not bullish_displacement and confidence < 55:
                    final_signal = "WAIT"
                    buy_score = 72
                    sell_score = 8
                    confidence = 66
                    entry_quality = "WEAK"
                    entry_timing = "WAIT DISPLACEMENT"
                    plan_type = "WAIT BULLISH DISPLACEMENT"
                    plan_side = "WAIT"
                    reasons.append(f"Weak bullish displacement: {displacement}")

                if not session_active:
                    final_signal = "WAIT"
                    buy_score = 70
                    sell_score = 8
                    confidence = 62
                    entry_quality = "WAIT"
                    entry_timing = "WAIT SESSION"
                    plan_type = "WAIT SESSION"
                    plan_side = "WAIT"
                    reasons.append(f"Session inactive: {session_name}")

                else:
                    final_signal = "BUY"
                    buy_score = 94 if bullish_fvg else 92
                    sell_score = 8
                    confidence = calculate_smart_confidence(
                        session_active,
                        bullish_fvg,
                        bearish_fvg,
                        bullish_displacement,
                        bearish_displacement,
                        fake_breakout,
                        mtf_bias,
                        "BUY"
                    )
                    entry_quality = "FVG" if bullish_fvg else "BOS"
                    entry_timing = "FVG BUY ENTRY" if bullish_fvg else "BOS BUY ENTRY"
                    plan_type = "FVG BUY" if bullish_fvg else "BOS BUY"
                    plan_side = "BUY"
                    state["pending"] = None
                    state["stage"] = "BUY_ACTIVE"
                    state["last_idea"] = "BUY"
        else:
            if state["stage"] in ["BUY_READY", "BUY_ACTIVE"] and c1 > state["bos_level"] - retest_buffer:
                plan_type = "BUY HOLDING"
                plan_side = "BUY"
                entry_timing = "BUY HOLDING"
                buy_score = 75
                sell_score = 10
                confidence = 70
            else:
                plan_type = "WAIT BUY RETEST"
                plan_side = "WAIT"
                entry_timing = "WAIT BUY RETEST"
                buy_score = 45
                sell_score = 15
                confidence = 35

    elif state["pending"] == "SELL" and state["bos_level"] is not None:
        in_sell_retest_zone = (
    c1 < state["bos_level"] + retest_buffer
)
        sell_displacement = (
            c1 < o1
            and c1 < state["bos_level"] + (retest_buffer * 0.3)
        )

        sell_no_retest_momentum = (
            c1 < o1
            and c1 < state["bos_level"]
            and (state["bos_level"] - c1) <= no_retest_max_distance
            and (h1 - l1) > 0
            and ((o1 - c1) / (h1 - l1)) >= 0.60
        )

        if in_sell_retest_zone:

            if (
                bearish_choch
                or bearish_bos
                or sell_displacement
                or recent_pressure == "BEARISH"
            ):

                final_signal = "SELL"

                buy_score = 8
                sell_score = 88
                confidence = 82

                entry_quality = "SMC"
                entry_timing = "CHOCH/BOS RETEST"

                plan_type = "SMC SELL"
                plan_side = "SELL"

                state["pending"] = None
                state["stage"] = "SELL_ACTIVE"

            else:
                state["stage"] = "SELL_READY"

                plan_type = "SELL READY"
                plan_side = "WAIT"
                entry_timing = "SELL READY"

                buy_score = 10
                sell_score = 65
                confidence = 55
        elif (
                (
                    (in_sell_retest_zone and sell_displacement)
                    or sell_no_retest_momentum
                    or (
                        state["stage"] == "SELL_READY"
                        and c1 < state["bos_level"]
                        and c1 < o1
                    )
                )
                and not (
                        htf_structure == "BULLISH"
                        and recent_pressure != "BEARISH"
                        and momentum_strength == "WEAK"
                        and not bearish_bos
                        and not bearish_choch
                    )
            ):
            distance_from_bos = state["bos_level"] - c1

            selling_into_liquidity = (
                nearest_sell_liquidity is not None
                and abs(c1 - nearest_sell_liquidity) <= retest_buffer
            )

            if distance_from_bos > late_entry_distance or selling_into_liquidity:
                final_signal = "WAIT"
                buy_score = 10
                sell_score = 75
                confidence = 70
                entry_quality = "LATE"
                entry_timing = "WAIT PULLBACK"
                plan_type = "SELL HOLDING"
                plan_side = "WAIT"
                reasons.append("Bearish trend active but entry is late")
            else:
                if fake_breakout == "FAKE_BEAR_BREAKOUT":
                    final_signal = "WAIT"
                    buy_score = 65
                    sell_score = 25
                    confidence = 72
                    entry_quality = "FAKE"
                    entry_timing = "FAKE SELL BREAKOUT"
                    plan_type = "FAKE SELL BREAKOUT"
                    plan_side = "WAIT"
                    reasons.append("Sell blocked: fake bearish breakout")

                elif mtf_bias == "BULLISH" and not bearish_bos and not bearish_choch:
                    final_signal = "WAIT"
                    buy_score = 35
                    sell_score = 55
                    confidence = 58
                    entry_quality = "CONFLICT"
                    entry_timing = "WAIT MTF ALIGNMENT"
                    plan_type = "SELL CONFLICT WITH HTF"
                    plan_side = "WAIT"
                    reasons.append(f"SELL blocked by MTF bullish bias ({mtf_score})")
                elif not bearish_displacement and confidence < 55 and not bearish_bos and not bearish_choch:
                    final_signal = "WAIT"
                    buy_score = 8
                    sell_score = 70
                    confidence = 62
                    entry_quality = "WAIT"
                    entry_timing = "WAIT DISPLACEMENT"
                    plan_type = "WAIT BEARISH DISPLACEMENT"
                    plan_side = "WAIT"
                    reasons.append(f"Weak bearish displacement: {displacement}")
                else:
                    final_signal = "SELL"
                    buy_score = 8
                    sell_score = 94 if bearish_fvg else 92
                    confidence = calculate_smart_confidence(
                        session_active,
                        bullish_fvg,
                        bearish_fvg,
                        bullish_displacement,
                        bearish_displacement,
                        fake_breakout,
                        mtf_bias,
                        "SELL"
                    )
                    entry_quality = "FVG" if bearish_fvg else "BOS"
                    entry_timing = "FVG SELL ENTRY" if bearish_fvg else "BOS SELL ENTRY"
                    plan_type = "FVG SELL" if bearish_fvg else "BOS SELL"
                    plan_side = "SELL"
                    state["pending"] = None
                    state["stage"] = "SELL_ACTIVE"
                    state["last_idea"] = "SELL"
        else:
            if state["stage"] in ["SELL_READY", "SELL_ACTIVE"] and c1 < state["bos_level"] + retest_buffer:
                plan_type = "SELL HOLDING"
                plan_side = "SELL"
                entry_timing = "SELL HOLDING"
                buy_score = 10
                sell_score = 75
                confidence = 70
            else:
                plan_type = "WAIT SELL RETEST"
                plan_side = "WAIT"
                entry_timing = "WAIT SELL RETEST"
                buy_score = 15
                sell_score = 45
                confidence = 35

    else:
            # =========================
        # KEEP ACTIVE TREND MEMORY
        # =========================

        if (
            state["stage"] in ["BUY_READY", "BUY_ACTIVE"]
            and state["bos_level"] is not None
            and c1 > state["bos_level"] - retest_buffer
        ):

            final_signal = "WAIT"

            buy_score = 75
            sell_score = 10
            confidence = 70

            entry_quality = "HOLDING"
            entry_timing = "BUY HOLDING"

            plan_type = "BUY HOLDING"
            plan_side = "WAIT"
            reasons.append("Bullish trend still active")

        elif (
            state["stage"] in ["SELL_READY", "SELL_ACTIVE"]
            and state["bos_level"] is not None
            and c1 < state["bos_level"] + retest_buffer
        ):
            final_signal = "SELL"

            buy_score = 10
            sell_score = 75
            confidence = 70

            entry_quality = "HOLDING"
            entry_timing = "SELL HOLDING"

            plan_type = "SELL HOLDING"
            plan_side = "WAIT"
            reasons.append("Bearish trend still active")

        else:

            if c1 > recent_low and c1 < recent_high:

                if c1 < ((recent_high + recent_low) / 2):

                    plan_type = "WAIT SELL BREAK"
                    entry_timing = "WAIT SELL BREAK"

                    sell_score = 45
                    buy_score = 15
                    confidence = 35

                    entry_quality = "BUY"

                    reasons.append("Price near support, waiting bearish BOS")

                else:

                    plan_type = "WAIT BUY BREAK"
                    entry_timing = "WAIT BUY BREAK"

                    buy_score = 45
                    sell_score = 15
                    confidence = 35

                    entry_quality = "WAIT"

                    reasons.append("Price near resistance, waiting bullish BOS")

            else:

                plan_type = "WAIT FOR BOS"
                entry_timing = "WAIT"

            plan_side = "WAIT"
    # =========================
    # EXIT IDEA LOGIC
    # =========================
    if state.get("last_idea") == "SELL" and (bullish_choch or bullish_bos):
        final_signal = "EXIT SELL"
        buy_score = 70
        sell_score = 20
        confidence = 85
        entry_quality = "EXIT"
        entry_timing = "EXIT SELL"
        plan_type = "EXIT SELL"
        plan_side = "EXIT"
        state["last_idea"] = None

    elif state.get("last_idea") == "BUY" and (bearish_choch or bearish_bos):
        final_signal = "EXIT BUY"
        buy_score = 20
        sell_score = 70
        confidence = 85
        entry_quality = "EXIT"
        entry_timing = "EXIT BUY"
        plan_type = "EXIT BUY"
        plan_side = "EXIT"
        state["last_idea"] = None

    if final_signal == "BUY" and swing_low:
        entry_price = round(c1, decimals)
        stop_loss = round(swing_low - buffer, decimals)
        risk = entry_price - stop_loss
        tp1 = round(entry_price + risk * 1.0, decimals)
        tp2 = round(entry_price + risk * 1.25, decimals)
        invalidation = "Exit if bearish CHOCH or break below SL"
        plan_reason = "BUY after CHOCH/BOS confirmation + retest"

    elif final_signal == "SELL" and swing_high:
        entry_price = round(c1, decimals)
        stop_loss = round(swing_high + buffer, decimals)
        risk = stop_loss - entry_price
        tp1 = round(entry_price - risk * 1.0, decimals)
        tp2 = round(entry_price - risk * 1.25, decimals)
        invalidation = "Exit if bullish CHOCH or break above SL"
        plan_reason = "SELL after CHOCH/BOS confirmation + retest"

    else:
        entry_price = "--"
        stop_loss = "--"
        tp1 = "--"
        tp2 = "--"
        invalidation = "Wait for CHOCH/BOS + retest"
        plan_reason = "No entry until structure confirms"

    # =========================
    # ✅ FINAL TRADE GATE — SOFTER SCALPER VERSION
    # =========================
    allow_trade = True

    # Confidence was too strict at 80. Scalper can start at 65.
    if confidence < 65:
        allow_trade = False
        reasons.append("Blocked: confidence below 65")

    # Fake breakout still blocks trade
    if fake_breakout != "NONE":
        allow_trade = False
        reasons.append("Blocked: fake breakout")

    # Displacement should warn, not always block
    if final_signal == "BUY" and not bullish_displacement:
        if confidence < 75:
            allow_trade = False
            reasons.append("Blocked: weak bullish displacement under 75 confidence")
        else:
            reasons.append("Warning: weak bullish displacement allowed by confidence")

    if final_signal == "SELL" and not bearish_displacement:
        if confidence < 75:
            allow_trade = False
            reasons.append("Blocked: weak bearish displacement under 75 confidence")
        else:
            reasons.append("Warning: weak bearish displacement allowed by confidence")

    # Inactive session should not fully kill SMC trades
    if not session_active:
        confidence = max(1, confidence - 8)
        reasons.append(f"Session inactive: confidence reduced ({session_name})")

    # MTF conflict should block only weak trades, not all trades
    if final_signal == "BUY" and mtf_bias == "BEARISH":
        if confidence < 78 and not bullish_bos and not bullish_choch:
            allow_trade = False
            reasons.append("Blocked: bearish MTF conflict")
        else:
            reasons.append("MTF bearish conflict allowed by strong BUY structure")

    if final_signal == "SELL" and mtf_bias == "BULLISH":
        if confidence < 78 and not bearish_bos and not bearish_choch:
            allow_trade = False
            reasons.append("Blocked: bullish MTF conflict")
        else:
            reasons.append("MTF bullish conflict allowed by strong SELL structure")

    if not allow_trade and final_signal in ["BUY", "SELL"]:
        final_signal = "WAIT"
        plan_side = "WAIT"
        entry_timing = "NO TRADE"

    if final_signal == "BUY":
        signal_text = f"BUY 🟢 ({confidence}% | {entry_timing})"
    elif final_signal == "SELL":
        signal_text = f"SELL 🔴 ({confidence}% | {entry_timing})"
    else:
        signal_text = f"WAIT ⚪ ({entry_timing})"

    structure_type = (
        "CHOCH BUY" if bullish_choch
        else "CHOCH SELL" if bearish_choch
        else "BOS BUY" if bullish_bos
        else "BOS SELL" if bearish_bos
        else "NEUTRAL"
    )

    result = {
        "signal": final_signal,
        "signal_text": signal_text,
        "buy_pct": buy_score,
        "sell_pct": sell_score,
        "confidence": confidence,
        "market_condition": "STRUCTURE",
        "entry_quality": entry_quality,
        "entry_timing": entry_timing,
        "fake_kill": False,
        "debug_reasons": reasons[-14:],
        "price": round(c1, decimals),
        "plan_type": plan_type,
        "plan_bias": plan_side,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
       "risk_reward": "1:1.25" if final_signal in ["BUY", "SELL"] else "--",
        "invalidation": invalidation,
        "plan_reason": plan_reason,
        "structure_trend": htf_structure,
        "structure_type": structure_type,
        "structure_next": plan_type,
        "structure_resistance": round(recent_high, decimals),
        "structure_support": round(recent_low, decimals),
        "smc_swings": smc_swings,
        "equal_highs": liquidity_levels["equal_highs"],
        "equal_lows": liquidity_levels["equal_lows"],
        "fvg": fvg,
        "session": session_name,
        "session_active": session_active,
        "displacement": displacement,
        "displacement_score": displacement_score,
        "fake_breakout": fake_breakout,
        "mtf_bias": mtf_bias,
        "mtf_score": mtf_score,
    }

    return result

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

    eurusd_closed = calendar_closed or _is_feed_stale(eurusd, max_age_minutes=90)
    gold_closed = calendar_closed or _is_feed_stale(gold, max_age_minutes=90)

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

    if not eurusd.empty:
       update_paper_trade("EURUSD", eurusd_result, eurusd["Close"].iloc[-1].item())

    if not gold.empty:
        update_paper_trade("GOLD", gold_result, gold["Close"].iloc[-1].item())

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
                "5m": _df_to_candles(eurusd, limit=5000),
                "15m": _df_to_candles(eurusd_htf, limit=5000),
                "1h": _df_to_candles(eurusd_1h, limit=5000),
            },
            "GOLD": {
                "5m": _df_to_candles(gold, limit=5000),
                "15m": _df_to_candles(gold_htf, limit=5000),
                "1h": _df_to_candles(gold_1h, limit=5000),
            }
        },
        "history": SIGNAL_HISTORY[-20:],
        "paper_trades": AUTO_TRADES,
        "paper_trade_history": PAPER_TRADE_HISTORY[-20:],
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