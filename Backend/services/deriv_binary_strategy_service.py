"""Deriv-native 5-minute Binary strategy.

This module is intentionally isolated from FlowSignal Forex V1/V2 strategy and
cTrader execution. It reads Deriv public EURUSD 5-minute candles and answers a
single binary question: is the next 5-minute expiry more likely to finish above
or below the entry price?

Entry is intentionally price-action driven: bullish/bearish engulfing plus a
small 5-minute break of structure (tiny BOS). EMA, SL, TP and RR are not used.
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
PUBLIC_WS_URL = "wss://ws.binaryws.com/websockets/v3"


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
    previous_bearish = previous["close"] < previous["open"]
    current_bullish = current["close"] > current["open"]
    if not previous_bearish or not current_bullish:
        return False
    previous_body_low = min(previous["open"], previous["close"])
    previous_body_high = max(previous["open"], previous["close"])
    current_body_low = min(current["open"], current["close"])
    current_body_high = max(current["open"], current["close"])
    return current_body_low <= previous_body_low and current_body_high >= previous_body_high


def _bearish_engulfing(previous: dict[str, float], current: dict[str, float]) -> bool:
    previous_bullish = previous["close"] > previous["open"]
    current_bearish = current["close"] < current["open"]
    if not previous_bullish or not current_bearish:
        return False
    previous_body_low = min(previous["open"], previous["close"])
    previous_body_high = max(previous["open"], previous["close"])
    current_body_low = min(current["open"], current["close"])
    current_body_high = max(current["open"], current["close"])
    return current_body_low <= previous_body_low and current_body_high >= previous_body_high


def _evaluate(candles: list[dict[str, float]]) -> dict[str, Any]:
    closed = _closed_candles(candles)
    if len(closed) < 12:
        return {
            "ok": True,
            "strategy": "DERIV_NATIVE_5M_V2_PRICE_ACTION",
            "symbol": SYMBOL,
            "timeframe": "5m",
            "expiry_minutes": 5,
            "signal": "WAIT",
            "confidence": 0,
            "reason": "INSUFFICIENT_CLOSED_5M_DATA",
            "signal_id": None,
        }

    last = closed[-1]
    previous = closed[-2]
    atr10 = _atr(closed, 10)

    bullish_engulfing = _bullish_engulfing(previous, last)
    bearish_engulfing = _bearish_engulfing(previous, last)

    # Tiny BOS: the engulfing candle must CLOSE through the nearest micro
    # structure made by the prior two completed candles. A very small ATR
    # buffer prevents equality/one-tick noise from counting as a break.
    micro_reference = closed[-4:-2]
    micro_high = max(c["high"] for c in micro_reference)
    micro_low = min(c["low"] for c in micro_reference)
    bos_buffer = max(atr10 * 0.03, 0.000005) if atr10 > 0 else 0.000005
    tiny_bos_up = last["close"] > (micro_high + bos_buffer)
    tiny_bos_down = last["close"] < (micro_low - bos_buffer)

    last_range = max(last["high"] - last["low"], 1e-9)
    body = last["close"] - last["open"]
    body_ratio = abs(body) / last_range
    upper_wick = last["high"] - max(last["open"], last["close"])
    lower_wick = min(last["open"], last["close"]) - last["low"]
    momentum_1 = last["close"] - previous["close"]
    momentum_3 = last["close"] - closed[-4]["close"]
    momentum_3_atr = (momentum_3 / atr10) if atr10 > 0 else 0.0

    signal = "WAIT"
    reason = "WAIT_FOR_ENGULFING_AND_TINY_BOS"
    confidence = 0

    if bullish_engulfing and tiny_bos_up:
        signal = "RISE"
        reason = "BULLISH_ENGULFING+TINY_BOS_UP"
        confidence = 70
        if body_ratio >= 0.60:
            confidence += 7
        if momentum_1 > 0 and momentum_3_atr >= 0.25:
            confidence += 6
        if lower_wick > upper_wick:
            confidence += 4
    elif bearish_engulfing and tiny_bos_down:
        signal = "FALL"
        reason = "BEARISH_ENGULFING+TINY_BOS_DOWN"
        confidence = 70
        if body_ratio >= 0.60:
            confidence += 7
        if momentum_1 < 0 and momentum_3_atr <= -0.25:
            confidence += 6
        if upper_wick > lower_wick:
            confidence += 4

    confidence = min(confidence, 90)
    candle_epoch = int(last["epoch"])
    signal_id = f"{SYMBOL}:{candle_epoch}:{signal}" if signal != "WAIT" else None

    return {
        "ok": True,
        "strategy": "DERIV_NATIVE_5M_V2_PRICE_ACTION",
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
            "tiny_bos_up": tiny_bos_up,
            "tiny_bos_down": tiny_bos_down,
            "micro_high": round(micro_high, 6),
            "micro_low": round(micro_low, 6),
            "bos_buffer": round(bos_buffer, 6),
        },
        "metrics": {
            "atr10": round(atr10, 6),
            "body_ratio": round(body_ratio, 3),
            "momentum_1": round(momentum_1, 6),
            "momentum_3_atr": round(momentum_3_atr, 3),
        },
    }


def binary_signal_snapshot() -> dict[str, Any]:
    try:
        candles = asyncio.run(_fetch_candles_async())
        return _evaluate(candles)
    except Exception as exc:
        return {
            "ok": False,
            "strategy": "DERIV_NATIVE_5M_V2_PRICE_ACTION",
            "symbol": SYMBOL,
            "timeframe": "5m",
            "expiry_minutes": 5,
            "signal": "WAIT",
            "confidence": 0,
            "reason": f"MARKET_DATA_UNAVAILABLE:{type(exc).__name__}",
            "signal_id": None,
        }
