"""Simple 5-minute EURUSD Binary signal.

The next five-minute contract follows the direction of the just-completed
five-minute candle. A true doji waits. No Forex SMC, BOS, ATR gate,
displacement gate, reversal-count gate, fundamentals, SL/TP, or RR.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable
from paths import DATA_DIR
from services import deriv_binary_v5_forward_validator as legacy

STRATEGY_VERSION = "DERIV_BINARY_SIMPLE_5M_1"
SYMBOL = "frxEURUSD"
GRANULARITY_SECONDS = 300
SIMPLE_SPEC = {"minimum_absolute_body": 0.0, "prediction": "SAME_AS_COMPLETED_CANDLE"}
SPEC_SHA256 = hashlib.sha256(json.dumps(SIMPLE_SPEC, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
EXPECTED_SPEC_SHA256 = "755ea79159b49281b0671846d0883ec34702efa55c2cf397aafac47058f8e3cb"
DEFAULT_DB_PATH = DATA_DIR / "deriv_simple_5m_validation.sqlite3"
if SPEC_SHA256 != EXPECTED_SPEC_SHA256:
    raise RuntimeError("Simple Binary specification hash mismatch")

legacy.STRATEGY_VERSION = STRATEGY_VERSION
legacy.FROZEN_THRESHOLDS = SIMPLE_SPEC
legacy.SPEC_SHA256 = SPEC_SHA256
legacy.EXPECTED_SPEC_SHA256 = EXPECTED_SPEC_SHA256
record_tick = legacy.record_tick
record_observation = legacy.record_observation
settle_from_recorded_ticks = legacy.settle_from_recorded_ticks
cleanup_raw_ticks = legacy.cleanup_raw_ticks
forward_report = legacy.forward_report

def initialize_database(db_path: str | Path = DEFAULT_DB_PATH, *, collection_start_timestamp: int | None = None) -> dict[str, Any]:
    state = legacy.initialize_database(db_path, collection_start_timestamp=collection_start_timestamp)
    start = int(state["collection_start_timestamp"])
    state["first_eligible_entry_timestamp"] = ((start // GRANULARITY_SECONDS) + 1) * GRANULARITY_SECONDS
    return state

def evaluate_completed_candle(candles: Iterable[dict[str, Any]], ticks: Iterable[dict[str, Any]], entry_timestamp: int) -> dict[str, Any]:
    normalized=[{"epoch":int(float(c["epoch"])),"open":float(c["open"]),"high":float(c["high"]),"low":float(c["low"]),"close":float(c["close"])} for c in candles]
    normalized.sort(key=lambda item:item["epoch"])
    decision_epoch=int(entry_timestamp)-GRANULARITY_SECONDS
    eligible=[c for c in normalized if c["epoch"]<=decision_epoch]
    if not eligible or eligible[-1]["epoch"]!=decision_epoch:
        raise ValueError("One aligned closed 5-minute candle is required")
    decision=eligible[-1]
    candle_ticks=sorted(({"epoch":int(t["epoch"]),"quote":float(t["quote"])} for t in ticks if decision_epoch<=int(t["epoch"])<int(entry_timestamp)),key=lambda item:item["epoch"])
    if len(candle_ticks)<2:
        raise ValueError("Decision candle tick path is unavailable")
    if int(entry_timestamp)-candle_ticks[-1]["epoch"]>2:
        raise ValueError("Entry quote is more than two seconds before the boundary")
    body=decision["close"]-decision["open"]
    candle_direction="BULLISH" if body>0 else "BEARISH" if body<0 else "DOJI"
    qualified=body!=0
    predicted_direction="RISE" if body>0 else "FALL" if body<0 else None
    candle_range=max(decision["high"]-decision["low"],1e-12)
    body_ratio=abs(body)/candle_range
    upper_wick=decision["high"]-max(decision["open"],decision["close"])
    lower_wick=min(decision["open"],decision["close"])-decision["low"]
    final_ticks=[t for t in candle_ticks if t["epoch"]>=int(entry_timestamp)-60]
    if len(final_ticks)<2: final_ticks=candle_ticks[-2:]
    observation_id=f"{STRATEGY_VERSION}:{SYMBOL}:{int(entry_timestamp)}"
    signal_id=f"{STRATEGY_VERSION}:{SYMBOL}:{int(entry_timestamp)}:{predicted_direction}" if predicted_direction else None
    return {"observation_id":observation_id,"signal_id":signal_id,"strategy_version":STRATEGY_VERSION,"spec_sha256":SPEC_SHA256,"symbol":SYMBOL,"decision_candle_epoch":decision_epoch,"entry_timestamp":int(entry_timestamp),"settlement_timestamp":int(entry_timestamp)+GRANULARITY_SECONDS,"entry_price":candle_ticks[-1]["quote"],"entry_quote_epoch":candle_ticks[-1]["epoch"],"candle":decision,"candle_direction":candle_direction,"net_displacement":body,"body_ratio":body_ratio,"upper_wick":upper_wick,"lower_wick":lower_wick,"qualified":qualified,"predicted_direction":predicted_direction,"reason":"FOLLOW_COMPLETED_CANDLE" if qualified else "DOJI_WAIT","_signal_evidence":{"decision_candle":decision,"candle_direction":candle_direction,"predicted_direction":predicted_direction,"body_ratio":body_ratio,"upper_wick":upper_wick,"lower_wick":lower_wick,"final_60_second_ticks":final_ticks}}
