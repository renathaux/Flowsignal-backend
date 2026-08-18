"""Deriv-native 5-minute Binary strategy.

This module is intentionally isolated from FlowSignal Forex V1/V2 strategy and
cTrader execution. It reads Deriv public EURUSD 5-minute candles and answers a
single binary question: is the next 5-minute expiry more likely to finish above
or below the entry price?

Entry is intentionally price-action driven. It looks for the first/second strong
5-minute displacement candle of a new micro move, confirmed by an engulfing or
micro structure break. EMA, Forex SMC, SL, TP and RR are not used.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets

SYMBOL = "frxEURUSD"
GRANULARITY_SECONDS = 300
CANDLE_COUNT = 60
# Current Deriv Options public market-data WebSocket. This replaces the legacy
# ws.binaryws.com endpoint, which can reject the handshake with InvalidStatus.
PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


def _atr(candles: list[dict[str, float]], period: int = 10) -> float:
    if len(candles) < 2:
        return 0.0
    ranges: list[float] = []
    subset = candles[-(period + 1):]
    for previous, current in zip(subset[:-1], subset[1:]):
        high = current["high"]
        low = current["low"]
        prev_close = previous["close"]
        ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(ranges) / len(ranges) if ranges else 0.0


async def _fetch_candles_async() -> list[dict[str, float]]:
    async with websockets.connect(PUBLIC_WS_URL, open_timeout=10, close_timeout=5) as ws:
        await ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "end": "latest",
            "count": CANDLE_COUNT,
            "style": "candles",
            "granularity": GRANULARITY_SECONDS,
            "subscribe": 0,
            "req_id": 501,
        }))
        deadline = time.monotonic() + 12.0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Timed out waiting for Deriv 5m candles")
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                continue
            if payload.get("error"):
                error = payload.get("error") or {}
                raise RuntimeError(str(error.get("message") or error.get("code") or "Deriv candle error"))
            if payload.get("req_id") != 501:
                continue
            rows = payload.get("candles") or []
            candles: list[dict[str, float]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    candles.append({
                        "epoch": float(row["epoch"]),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                    })
                except (KeyError, TypeError, ValueError):
                    continue
            candles.sort(key=lambda item: item["epoch"])
            return candles


def _closed_candles(candles: list[dict[str, float]]) -> list[dict[str, float]]:
    if not candles:
        return []
    now = int(time.time())
    current_bucket = now - (now % GRANULARITY_SECONDS)
    return [candle for candle in candles if int(candle["epoch"]) < current_bucket]


def _bullish_engulfing(previous: dict[str, float], current: dict[str, float]) -> bool:
    return (
        previous["close"] < previous["open"]
        and current["close"] > current["open"]
        and min(current["open"], current["close"]) <= min(previous["open"], previous["close"])
        and max(current["open"], current["close"]) >= max(previous["open"], previous["close"])
    )


def _bearish_engulfing(previous: dict[str, float], current: dict[str, float]) -> bool:
    return (
        previous["close"] > previous["open"]
        and current["close"] < current["open"]
        and min(current["open"], current["close"]) <= min(previous["open"], previous["close"])
        and max(current["open"], current["close"]) >= max(previous["open"], previous["close"])
    )


def _direction(candle: dict[str, float]) -> int:
    if candle["close"] > candle["open"]:
        return 1
    if candle["close"] < candle["open"]:
        return -1
    return 0


def _evaluate(candles: list[dict[str, float]]) -> dict[str, Any]:
    closed = _closed_candles(candles)
    strategy_name = "DERIV_NATIVE_5M_V3_MICRO_MOMENTUM"
    if len(closed) < 14:
        return {
            "ok": True, "strategy": strategy_name, "symbol": SYMBOL,
            "timeframe": "5m", "expiry_minutes": 5, "signal": "WAIT",
            "confidence": 0, "reason": "INSUFFICIENT_CLOSED_5M_DATA", "signal_id": None,
        }

    last = closed[-1]
    previous = closed[-2]
    atr10 = _atr(closed, 10)
    atr_safe = max(atr10, 1e-9)

    bullish_engulfing = _bullish_engulfing(previous, last)
    bearish_engulfing = _bearish_engulfing(previous, last)

    micro_reference = closed[-4:-2]
    micro_high = max(c["high"] for c in micro_reference)
    micro_low = min(c["low"] for c in micro_reference)
    bos_buffer = max(atr10 * 0.02, 0.000003) if atr10 > 0 else 0.000003
    micro_break_up = last["close"] > micro_high + bos_buffer
    micro_break_down = last["close"] < micro_low - bos_buffer

    candle_range = max(last["high"] - last["low"], 1e-9)
    body = last["close"] - last["open"]
    body_abs = abs(body)
    body_ratio = body_abs / candle_range
    range_atr = candle_range / atr_safe
    body_atr = body_abs / atr_safe
    close_location = (last["close"] - last["low"]) / candle_range

    strong_bull_close = body > 0 and body_ratio >= 0.55 and close_location >= 0.72
    strong_bear_close = body < 0 and body_ratio >= 0.55 and close_location <= 0.28
    displacement = body_atr >= 0.45 or range_atr >= 0.75

    prior_dirs = [_direction(c) for c in closed[-5:-1]]
    consecutive_bulls = 0
    consecutive_bears = 0
    for d in reversed(prior_dirs):
        if d == 1:
            consecutive_bulls += 1
        else:
            break
    for d in reversed(prior_dirs):
        if d == -1:
            consecutive_bears += 1
        else:
            break
    bull_exhausted = consecutive_bulls >= 3
    bear_exhausted = consecutive_bears >= 3

    bull_trigger = strong_bull_close and displacement and (bullish_engulfing or micro_break_up)
    bear_trigger = strong_bear_close and displacement and (bearish_engulfing or micro_break_down)

    signal = "WAIT"
    reason = "WAIT_FOR_EARLY_MICRO_MOMENTUM"
    confidence = 0

    if bull_trigger and not bull_exhausted:
        signal = "RISE"
        reason = "EARLY_BULL_DISPLACEMENT"
        confidence = 68
        if bullish_engulfing:
            confidence += 7
        if micro_break_up:
            confidence += 8
        if bullish_engulfing and micro_break_up:
            confidence += 4
        if body_ratio >= 0.70:
            confidence += 4
        if close_location >= 0.85:
            confidence += 3
    elif bear_trigger and not bear_exhausted:
        signal = "FALL"
        reason = "EARLY_BEAR_DISPLACEMENT"
        confidence = 68
        if bearish_engulfing:
            confidence += 7
        if micro_break_down:
            confidence += 8
        if bearish_engulfing and micro_break_down:
            confidence += 4
        if body_ratio >= 0.70:
            confidence += 4
        if close_location <= 0.15:
            confidence += 3
    elif bull_trigger and bull_exhausted:
        reason = "BULL_MOVE_EXHAUSTION_GUARD"
    elif bear_trigger and bear_exhausted:
        reason = "BEAR_MOVE_EXHAUSTION_GUARD"

    confidence = min(confidence, 92)
    candle_epoch = int(last["epoch"])
    signal_id = f"{SYMBOL}:{candle_epoch}:{signal}" if signal != "WAIT" else None

    return {
        "ok": True,
        "strategy": strategy_name,
        "symbol": SYMBOL,
        "timeframe": "5m",
        "expiry_minutes": 5,
        "signal": signal,
        "confidence": confidence,
        "reason": reason,
        "signal_id": signal_id,
        "candle_epoch": candle_epoch,
        "entry_reference": last["close"],
        "setup": {
            "bullish_engulfing": bullish_engulfing,
            "bearish_engulfing": bearish_engulfing,
            "micro_break_up": micro_break_up,
            "micro_break_down": micro_break_down,
            "strong_bull_close": strong_bull_close,
            "strong_bear_close": strong_bear_close,
            "displacement": displacement,
            "bull_exhausted": bull_exhausted,
            "bear_exhausted": bear_exhausted,
            "prior_consecutive_bulls": consecutive_bulls,
            "prior_consecutive_bears": consecutive_bears,
            "micro_high": round(micro_high, 6),
            "micro_low": round(micro_low, 6),
            "bos_buffer": round(bos_buffer, 6),
        },
        "metrics": {
            "atr10": round(atr10, 6),
            "body_ratio": round(body_ratio, 3),
            "body_atr": round(body_atr, 3),
            "range_atr": round(range_atr, 3),
            "close_location": round(close_location, 3),
        },
    }


def binary_signal_snapshot() -> dict[str, Any]:
    try:
        candles = asyncio.run(_fetch_candles_async())
        return _evaluate(candles)
    except Exception as exc:
        return {
            "ok": False,
            "strategy": "DERIV_NATIVE_5M_V3_MICRO_MOMENTUM",
            "symbol": SYMBOL,
            "timeframe": "5m",
            "expiry_minutes": 5,
            "signal": "WAIT",
            "confidence": 0,
            "reason": f"MARKET_DATA_UNAVAILABLE:{type(exc).__name__}",
            "signal_id": None,
        }
