"""Deriv-native 5-minute Binary strategy.

This module is intentionally isolated from FlowSignal Forex V1/V2 strategy and
cTrader execution. It reads Deriv public EURUSD 5-minute candles and answers a
single binary question: is the next 5-minute expiry more likely to finish above
or below the entry price?

Outputs: RISE, FALL, or WAIT. No SL/TP/RR concepts are used here.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from typing import Any

import websockets

SYMBOL = "frxEURUSD"
GRANULARITY_SECONDS = 300
CANDLE_COUNT = 60
PUBLIC_WS_URL = "wss://ws.binaryws.com/websockets/v3"


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (period + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1.0 - alpha))
    return ema


def _rsi(values: list[float], period: int = 7) -> float:
    if len(values) < period + 1:
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    for previous, current in zip(values[-(period + 1):-1], values[-period:]):
        change = current - previous
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss <= 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


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


def _evaluate(candles: list[dict[str, float]]) -> dict[str, Any]:
    closed = _closed_candles(candles)
    if len(closed) < 20:
        return {
            "ok": True,
            "strategy": "DERIV_NATIVE_5M_V1",
            "symbol": SYMBOL,
            "timeframe": "5m",
            "expiry_minutes": 5,
            "signal": "WAIT",
            "confidence": 0,
            "reason": "INSUFFICIENT_CLOSED_5M_DATA",
            "signal_id": None,
        }

    closes = [c["close"] for c in closed]
    last = closed[-1]
    previous = closed[-2]
    ema_fast = _ema(closes[-20:], 5)
    ema_slow = _ema(closes[-30:], 12)
    rsi7 = _rsi(closes, 7)
    atr10 = _atr(closed, 10)

    last_range = max(last["high"] - last["low"], 1e-9)
    body = last["close"] - last["open"]
    body_ratio = abs(body) / last_range
    upper_wick = last["high"] - max(last["open"], last["close"])
    lower_wick = min(last["open"], last["close"]) - last["low"]

    momentum_1 = last["close"] - previous["close"]
    momentum_3 = last["close"] - closed[-4]["close"]
    normalized_momentum = (momentum_3 / atr10) if atr10 > 0 else 0.0

    recent_high = max(c["high"] for c in closed[-6:-1])
    recent_low = min(c["low"] for c in closed[-6:-1])
    breakout_up = last["close"] > recent_high
    breakout_down = last["close"] < recent_low

    rise_score = 0.0
    fall_score = 0.0
    reasons: list[str] = []

    if ema_fast > ema_slow:
        rise_score += 2.0
        reasons.append("FAST_EMA_ABOVE_SLOW")
    elif ema_fast < ema_slow:
        fall_score += 2.0
        reasons.append("FAST_EMA_BELOW_SLOW")

    if momentum_1 > 0:
        rise_score += 1.0
    elif momentum_1 < 0:
        fall_score += 1.0

    if normalized_momentum >= 0.35:
        rise_score += 1.5
        reasons.append("POSITIVE_3BAR_MOMENTUM")
    elif normalized_momentum <= -0.35:
        fall_score += 1.5
        reasons.append("NEGATIVE_3BAR_MOMENTUM")

    if body > 0 and body_ratio >= 0.55:
        rise_score += 1.25
        reasons.append("STRONG_BULLISH_CLOSE")
    elif body < 0 and body_ratio >= 0.55:
        fall_score += 1.25
        reasons.append("STRONG_BEARISH_CLOSE")

    if breakout_up:
        rise_score += 1.5
        reasons.append("5M_RANGE_BREAK_UP")
    elif breakout_down:
        fall_score += 1.5
        reasons.append("5M_RANGE_BREAK_DOWN")

    if lower_wick > abs(body) * 1.25 and last["close"] > last["open"]:
        rise_score += 0.75
        reasons.append("LOWER_WICK_REJECTION")
    if upper_wick > abs(body) * 1.25 and last["close"] < last["open"]:
        fall_score += 0.75
        reasons.append("UPPER_WICK_REJECTION")

    if 52 <= rsi7 <= 72:
        rise_score += 0.75
    elif 28 <= rsi7 <= 48:
        fall_score += 0.75
    elif rsi7 > 78:
        rise_score -= 0.75
    elif rsi7 < 22:
        fall_score -= 0.75

    dominant = max(rise_score, fall_score)
    opposing = min(rise_score, fall_score)
    edge = dominant - opposing

    signal = "WAIT"
    if rise_score >= 4.25 and edge >= 1.75:
        signal = "RISE"
    elif fall_score >= 4.25 and edge >= 1.75:
        signal = "FALL"

    confidence = 0
    if signal != "WAIT":
        confidence = int(round(min(92.0, max(55.0, 55.0 + dominant * 5.0 + edge * 2.5))))

    reason = "NO_CLEAR_5M_BINARY_EDGE"
    if signal != "WAIT":
        reason = ",".join(reasons[-5:]) or "5M_BINARY_EDGE"

    candle_epoch = int(last["epoch"])
    signal_id = f"{SYMBOL}:{candle_epoch}:{signal}" if signal != "WAIT" else None

    return {
        "ok": True,
        "strategy": "DERIV_NATIVE_5M_V1",
        "symbol": SYMBOL,
        "timeframe": "5m",
        "expiry_minutes": 5,
        "signal": signal,
        "confidence": confidence,
        "reason": reason,
        "signal_id": signal_id,
        "candle_epoch": candle_epoch,
        "entry_reference": last["close"],
        "metrics": {
            "ema5": round(ema_fast, 6),
            "ema12": round(ema_slow, 6),
            "rsi7": round(rsi7, 2),
            "atr10": round(atr10, 6),
            "body_ratio": round(body_ratio, 3),
            "momentum_3_atr": round(normalized_momentum, 3),
            "rise_score": round(rise_score, 2),
            "fall_score": round(fall_score, 2),
        },
    }


def binary_signal_snapshot() -> dict[str, Any]:
    try:
        candles = asyncio.run(_fetch_candles_async())
        return _evaluate(candles)
    except Exception as exc:
        return {
            "ok": False,
            "strategy": "DERIV_NATIVE_5M_V1",
            "symbol": SYMBOL,
            "timeframe": "5m",
            "expiry_minutes": 5,
            "signal": "WAIT",
            "confidence": 0,
            "reason": f"MARKET_DATA_UNAVAILABLE:{type(exc).__name__}",
            "signal_id": None,
        }
