"""Use the backend SMC indicator as the single BOS/CHoCH authority.

The visual switch in the browser must never control this module. The strategy
calls this adapter on every normal strategy evaluation, so hiding the overlay
only changes presentation.
"""
from __future__ import annotations

from indicators.smc import analyze_structure


AUTHORITY_SOURCE = "backend_smc_indicator"
AUTHORITY_CANDLE_LIMIT = 250
STRATEGY_DIAGNOSTIC_SWING_LIMIT = 20


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _indicator_swings(analysis):
    output = []
    swings = (analysis or {}).get("swings") or []
    for swing in swings[-STRATEGY_DIAGNOSTIC_SWING_LIMIT:]:
        if not isinstance(swing, dict):
            continue
        output.append({
            "type": swing.get("type"),
            "price": swing.get("price"),
            "index": swing.get("index"),
            "time": swing.get("timestamp"),
            "confirmed_index": swing.get("confirmed_index"),
            "confirmed_time": swing.get("confirmed_timestamp"),
            "indicator_source": AUTHORITY_SOURCE,
        })
    return output


def _structure_context(analysis):
    analysis = analysis if isinstance(analysis, dict) else {}
    current = (
        analysis.get("current_structure")
        if isinstance(analysis.get("current_structure"), dict)
        else {}
    )
    bias = str(analysis.get("bias") or current.get("bias") or "NEUTRAL").upper()
    pattern = "HH_HL" if bias == "BULLISH" else "LH_LL" if bias == "BEARISH" else "NEUTRAL"
    return {
        "pattern": pattern,
        "bias": bias,
        "reason": "SMC_INDICATOR_AUTHORITY",
        "indicator_source": AUTHORITY_SOURCE,
        "protected_high": current.get("protected_high"),
        "protected_low": current.get("protected_low"),
        "bullish_continuation_frontier": current.get("bullish_continuation_frontier"),
        "bearish_continuation_frontier": current.get("bearish_continuation_frontier"),
    }


def _analysis_summary(analysis, candle_count):
    analysis = analysis if isinstance(analysis, dict) else {}
    events = analysis.get("events") or []
    swings = analysis.get("swings") or []
    latest_event = events[-1] if events and isinstance(events[-1], dict) else None
    return {
        "source": AUTHORITY_SOURCE,
        "candle_limit": AUTHORITY_CANDLE_LIMIT,
        "candle_count": candle_count,
        "bias": analysis.get("bias"),
        "event_count": len(events),
        "swing_count": len(swings),
        "latest_event": latest_event,
    }


def _base_result(analysis=None, candle_count=0):
    swings = _indicator_swings(analysis)
    return {
        "side": "WAIT",
        "level": None,
        "break_time": None,
        "break_close_time": None,
        "break_close": None,
        "remembered": False,
        "reason": "WAIT_NO_FRESH_15M_SMC_BREAK",
        "swings": swings,
        "raw_swings": swings,
        "structure": _structure_context(analysis),
        "breakouts": [],
        "bos_buffer": None,
        "event_invalidation_swing": None,
        "indicator_authority": True,
        "indicator_source": AUTHORITY_SOURCE,
        "indicator_summary": _analysis_summary(analysis, candle_count),
    }


def _marked_remembered_breakout(strict_trader_module, symbol, side, current_close_time, current_close):
    key = strict_trader_module.get_watch_key(symbol, side)
    watch = strict_trader_module.shared.FIFTEEN_M_SWING_WATCH.get(key)
    if not isinstance(watch, dict) or watch.get("source") != AUTHORITY_SOURCE:
        return None
    remembered = strict_trader_module.remembered_breakout(
        symbol,
        side,
        current_close_time=current_close_time,
        current_close=current_close,
    )
    if not isinstance(remembered, dict):
        return None
    remembered["event_invalidation_swing"] = watch.get("event_invalidation_swing")
    remembered["indicator_authority"] = True
    remembered["indicator_source"] = AUTHORITY_SOURCE
    return remembered


def evaluate_indicator_breakout(
    data_15m,
    symbol,
    execution_settings=None,
    *,
    strict_trader_module,
):
    """Return strict-trader breakout data using SMC indicator structure events.

    The indicator owns event existence, direction and BOS-vs-CHoCH
    classification. Existing strict safety requirements remain in force: a
    100-point structural leg, buffered 15m close, later 5m confirmation, EMA,
    consolidation, SL/TP, risk, duplicate, position and broker gates.

    A fixed 250-closed-candle authority window is used so chart and strategy see
    the same market-structure history and so strategy payloads remain bounded.
    """
    if data_15m is None or len(data_15m) < 10:
        result = _base_result(candle_count=0 if data_15m is None else len(data_15m))
        result["reason"] = "WAIT_NOT_ENOUGH_15M_DATA"
        return result

    authority_frame = data_15m.tail(AUTHORITY_CANDLE_LIMIT).copy()
    normalized_symbol = strict_trader_module.shared.normalize_symbol(symbol)
    configured = execution_settings or strict_trader_module.get_cached_execution_settings()
    required_buffer = strict_trader_module.bos_buffer(
        authority_frame,
        normalized_symbol,
        configured.get("bos_buffer_points", strict_trader_module.BOS_MIN_BUFFER_POINTS),
    )
    analysis = analyze_structure(
        authority_frame,
        timeframe="15m",
        point_size=strict_trader_module.point_size(normalized_symbol),
    )
    result = _base_result(analysis, candle_count=len(authority_frame))
    result["bos_buffer"] = required_buffer

    last_index = len(authority_frame) - 1
    last_close = float(authority_frame.iloc[-1]["Close"])
    last_close_time = strict_trader_module.candle_close_time(authority_frame.index[-1], 15)
    fresh_events = [
        event
        for event in (analysis.get("events") or [])
        if isinstance(event, dict)
        and int(event.get("break_index", -1)) == last_index
        and str(event.get("direction") or "").upper() in {"BULLISH", "BEARISH"}
    ]

    if fresh_events:
        event = fresh_events[-1]
        direction = str(event.get("direction") or "").upper()
        side = "BUY" if direction == "BULLISH" else "SELL"
        level = _as_float(event.get("broken_level"))
        break_close = _as_float(event.get("close"))
        invalidation = (
            event.get("event_invalidation_swing")
            if isinstance(event.get("event_invalidation_swing"), dict)
            else None
        )
        invalidation_price = _as_float((invalidation or {}).get("price"))
        swing_size = (
            abs(level - invalidation_price)
            if level is not None and invalidation_price is not None
            else None
        )
        minimum_swing = strict_trader_module.minimum_swing_size(normalized_symbol)

        result["indicator_event"] = event
        result["indicator_event_type"] = str(event.get("event_type") or "BOS").upper()
        result["indicator_structural_leg_size"] = swing_size
        result["minimum_structural_leg_size"] = minimum_swing

        if level is None or break_close is None:
            result["reason"] = "WAIT_INVALID_INDICATOR_SMC_EVENT"
            return result
        if swing_size is None or swing_size < minimum_swing:
            result["reason"] = "WAIT_NO_VALID_100_POINT_SWING"
            return result

        buffered = (
            break_close > level + required_buffer
            if side == "BUY"
            else break_close < level - required_buffer
        )
        if not buffered:
            result["reason"] = "WAIT_WEAK_15M_BOS"
            return result

        swing_type = "HIGH" if side == "BUY" else "LOW"
        broken_swing = {
            "type": swing_type,
            "price": level,
            "index": event.get("structure_start_index"),
            "time": event.get("broken_swing_timestamp"),
            "swing_size": swing_size,
            "valid": True,
            "valid_reason": "indicator_100_point_structure",
            "indicator_source": AUTHORITY_SOURCE,
            "indicator_event_invalidation_swing": invalidation,
        }
        break_time = event.get("timestamp")
        candidate = {
            "side": side,
            "level": level,
            "break_time": break_time,
            "break_close_time": strict_trader_module.candle_close_time(break_time, 15),
            "break_close": break_close,
            "remembered": False,
            "bos_buffer": required_buffer,
            "swing": broken_swing,
            "break_type": str(event.get("event_type") or "BOS").upper(),
            "invalidation_level": invalidation_price,
            "event_invalidation_swing": invalidation,
            "indicator_authority": True,
            "indicator_source": AUTHORITY_SOURCE,
            "indicator_event": event,
        }
        strict_trader_module.clear_opposite_watch(
            normalized_symbol,
            side,
            "opposite SMC indicator event",
        )
        result.update(candidate)
        result["breakouts"] = [candidate]
        result["swings"] = [broken_swing]
        result["raw_swings"] = [broken_swing]
        result["reason"] = f"SMC_INDICATOR_{candidate['break_type']}"
        return result

    remembered_candidates = []
    for side in ("BUY", "SELL"):
        remembered = _marked_remembered_breakout(
            strict_trader_module,
            normalized_symbol,
            side,
            current_close_time=last_close_time,
            current_close=last_close,
        )
        if remembered:
            remembered_candidates.append(remembered)

    if remembered_candidates:
        remembered = remembered_candidates[-1]
        remembered["structure"] = result["structure"]
        remembered["bos_buffer"] = float(remembered.get("bos_buffer") or required_buffer)
        remembered["indicator_summary"] = result["indicator_summary"]
        result.update(remembered)
        result["breakouts"] = remembered_candidates
        result["reason"] = f"SMC_INDICATOR_REMEMBERED_{str(remembered.get('break_type') or 'BOS').upper()}"
        return result

    return result


def mark_indicator_breakout_watch(
    strict_trader_module,
    original_save_function,
    *args,
    **kwargs,
):
    """Persist the authority marker and structural invalidation with a watch."""
    result = original_save_function(*args, **kwargs)
    symbol = args[0] if len(args) > 0 else kwargs.get("symbol")
    side = args[1] if len(args) > 1 else kwargs.get("side")
    swing = args[8] if len(args) > 8 else kwargs.get("swing")
    if not isinstance(swing, dict) or swing.get("indicator_source") != AUTHORITY_SOURCE:
        return result
    key = strict_trader_module.get_watch_key(symbol, side)
    watch = strict_trader_module.shared.FIFTEEN_M_SWING_WATCH.get(key)
    if isinstance(watch, dict):
        watch["source"] = AUTHORITY_SOURCE
        watch["event_invalidation_swing"] = swing.get("indicator_event_invalidation_swing")
        strict_trader_module.shared.save_fifteen_m_swing_watch()
    return result


def build_chart_structure(frame, symbol, timeframe, *, strict_trader_module):
    bounded_frame = (
        frame.tail(AUTHORITY_CANDLE_LIMIT).copy()
        if frame is not None
        else frame
    )
    analysis = analyze_structure(
        bounded_frame,
        timeframe=timeframe,
        point_size=strict_trader_module.point_size(symbol),
    )
    return {
        **analysis,
        "symbol": strict_trader_module.shared.normalize_symbol(symbol),
        "timeframe": str(timeframe).lower(),
        "closed_candle_count": len(bounded_frame) if bounded_frame is not None else 0,
        "authority_candle_limit": AUTHORITY_CANDLE_LIMIT,
        "source": AUTHORITY_SOURCE,
        "observation_only": False,
        "affects_strategy": str(timeframe).lower() == "15m",
        "strategy_authority": str(timeframe).lower() == "15m",
    }
