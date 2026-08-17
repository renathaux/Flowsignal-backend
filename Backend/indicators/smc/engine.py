"""Closed-candle Smart Money Concepts structure analysis.

Safety boundary: this module is pure analysis. It has no broker, execution,
LIVE/PAPER, risk, cooldown, or order-management imports.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

import pandas as pd


StructureBias = Literal["BULLISH", "BEARISH", "NEUTRAL"]
EventType = Literal["BOS", "CHOCH"]
SwingType = Literal["HIGH", "LOW"]


@dataclass(frozen=True)
class SwingPoint:
    swing_type: SwingType
    index: int
    confirmed_index: int
    timestamp: str
    confirmed_timestamp: str
    price: float


@dataclass(frozen=True)
class StructureEvent:
    event_type: EventType
    direction: Literal["BULLISH", "BEARISH"]
    timestamp: str
    close: float
    broken_swing_timestamp: str
    broken_level: float
    previous_bias: StructureBias
    new_bias: StructureBias


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close"])

    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(frame.columns):
        missing = ", ".join(sorted(required - set(frame.columns)))
        raise ValueError(f"SMC frame missing required columns: {missing}")

    data = frame.copy().sort_index()
    data = data.dropna(subset=["Open", "High", "Low", "Close"])
    data.index = pd.to_datetime(data.index, utc=True)
    return data


def detect_confirmed_swings(
    frame: pd.DataFrame,
    *,
    left_bars: int = 2,
    right_bars: int = 2,
) -> list[SwingPoint]:
    """Return pivots only after enough candles exist to confirm them.

    A swing at index i is not usable until i + right_bars, preventing
    look-ahead/repainting in simulation and future strategy integration.
    """
    if left_bars < 1 or right_bars < 1:
        raise ValueError("left_bars and right_bars must both be >= 1")

    data = _normalize_frame(frame)
    if len(data) < left_bars + right_bars + 1:
        return []

    highs = data["High"].astype(float).tolist()
    lows = data["Low"].astype(float).tolist()
    timestamps = list(data.index)
    swings: list[SwingPoint] = []

    for index in range(left_bars, len(data) - right_bars):
        high = highs[index]
        low = lows[index]
        left_highs = highs[index - left_bars:index]
        right_highs = highs[index + 1:index + 1 + right_bars]
        left_lows = lows[index - left_bars:index]
        right_lows = lows[index + 1:index + 1 + right_bars]
        confirmed_index = index + right_bars

        if high > max(left_highs) and high >= max(right_highs):
            swings.append(SwingPoint(
                swing_type="HIGH",
                index=index,
                confirmed_index=confirmed_index,
                timestamp=timestamps[index].isoformat(),
                confirmed_timestamp=timestamps[confirmed_index].isoformat(),
                price=float(high),
            ))

        if low < min(left_lows) and low <= min(right_lows):
            swings.append(SwingPoint(
                swing_type="LOW",
                index=index,
                confirmed_index=confirmed_index,
                timestamp=timestamps[index].isoformat(),
                confirmed_timestamp=timestamps[confirmed_index].isoformat(),
                price=float(low),
            ))

    return sorted(swings, key=lambda item: (item.confirmed_index, item.index, item.swing_type))


def analyze_structure(
    frame: pd.DataFrame,
    *,
    left_bars: int = 2,
    right_bars: int = 2,
) -> dict:
    """Build non-repainting swing/BOS/CHoCH state from closed candles only."""
    data = _normalize_frame(frame)
    swings = detect_confirmed_swings(
        data,
        left_bars=left_bars,
        right_bars=right_bars,
    )
    if data.empty:
        return {
            "bias": "NEUTRAL",
            "last_swing_high": None,
            "last_swing_low": None,
            "swings": [],
            "events": [],
        }

    swings_by_confirmation: dict[int, list[SwingPoint]] = {}
    for swing in swings:
        swings_by_confirmation.setdefault(swing.confirmed_index, []).append(swing)

    latest_high: SwingPoint | None = None
    latest_low: SwingPoint | None = None
    broken_high_key: tuple[str, float] | None = None
    broken_low_key: tuple[str, float] | None = None
    bias: StructureBias = "NEUTRAL"
    events: list[StructureEvent] = []

    closes = data["Close"].astype(float).tolist()
    timestamps = list(data.index)

    for index, close in enumerate(closes):
        for swing in swings_by_confirmation.get(index, []):
            if swing.swing_type == "HIGH":
                latest_high = swing
                broken_high_key = None
            else:
                latest_low = swing
                broken_low_key = None

        if latest_high is not None:
            high_key = (latest_high.timestamp, latest_high.price)
            if close > latest_high.price and broken_high_key != high_key:
                previous_bias = bias
                event_type: EventType = "CHOCH" if bias == "BEARISH" else "BOS"
                bias = "BULLISH"
                events.append(StructureEvent(
                    event_type=event_type,
                    direction="BULLISH",
                    timestamp=timestamps[index].isoformat(),
                    close=float(close),
                    broken_swing_timestamp=latest_high.timestamp,
                    broken_level=latest_high.price,
                    previous_bias=previous_bias,
                    new_bias=bias,
                ))
                broken_high_key = high_key

        if latest_low is not None:
            low_key = (latest_low.timestamp, latest_low.price)
            if close < latest_low.price and broken_low_key != low_key:
                previous_bias = bias
                event_type = "CHOCH" if bias == "BULLISH" else "BOS"
                bias = "BEARISH"
                events.append(StructureEvent(
                    event_type=event_type,
                    direction="BEARISH",
                    timestamp=timestamps[index].isoformat(),
                    close=float(close),
                    broken_swing_timestamp=latest_low.timestamp,
                    broken_level=latest_low.price,
                    previous_bias=previous_bias,
                    new_bias=bias,
                ))
                broken_low_key = low_key

    return {
        "bias": bias,
        "last_swing_high": asdict(latest_high) if latest_high else None,
        "last_swing_low": asdict(latest_low) if latest_low else None,
        "swings": [asdict(item) for item in swings],
        "events": [asdict(item) for item in events],
        "config": {
            "left_bars": left_bars,
            "right_bars": right_bars,
            "closed_candles_only": True,
            "repainting": False,
        },
    }
