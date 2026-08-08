"""Best-effort durable strategy-cycle audit records.

This module observes an already-computed strategy result. It must never decide,
permit, block, or execute a trade.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import os
import uuid

from sqlalchemy import delete, select

from db import SessionLocal
from models import StrategyCycleDiagnostic


SESSION_ID = os.getenv("RENDER_INSTANCE_ID") or f"boot-{uuid.uuid4()}"
RETENTION_DAYS = max(1, int(os.getenv("STRATEGY_DIAGNOSTICS_RETENTION_DAYS", "30")))
RETENTION_CLEANUP_INTERVAL_SECONDS = max(
    300,
    int(os.getenv("STRATEGY_DIAGNOSTICS_CLEANUP_INTERVAL_SECONDS", "3600")),
)
_last_cleanup_at = None


def _utc_datetime(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_safe(value):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return str(value)


def _latest_closed_timestamp(frame, minutes, now=None):
    if frame is None or getattr(frame, "empty", True):
        return None
    current = now or datetime.now(timezone.utc)
    for index in reversed(list(frame.index)):
        opened = _utc_datetime(index)
        if opened is not None and opened + timedelta(minutes=minutes) <= current:
            return opened + timedelta(minutes=minutes)
    return None


def _latest_swing(swings, kind):
    matching = [item for item in swings if str(item.get("type") or "").upper() == kind]
    return matching[-1] if matching else None


def _swing_record(swing, minimum_size=None):
    if not isinstance(swing, dict):
        return None
    qualified = bool(swing.get("valid"))
    reason = swing.get("valid_reason")
    if not reason:
        reason = "PASSED_MINIMUM_SWING" if qualified else "SWING_UNDER_MINIMUM"
    return {
        "type": swing.get("type"),
        "timestamp": swing.get("time"),
        "price": swing.get("price"),
        "size": swing.get("swing_size"),
        "minimum_size": minimum_size,
        "qualified": qualified,
        "reason": reason,
        "reference_swing": swing.get("reference_swing"),
    }


def _progress_snapshot(result):
    """Mirror the current five-stage UI formula; do not improve it here."""
    stages = result.get("strategy_stage_states") or {}
    first_five = [
        ("bos_choch", "fifteen_m_bos"),
        ("structure_break", "fifteen_m_bos"),
        ("m15_close", "fifteen_m_close"),
        ("m5_confirmation", "five_m_confirmation"),
        ("swing_sl", "swing_sl"),
    ]
    components = [
        {
            "name": name,
            "stage": stage,
            "state": str(stages.get(stage) or "NOT_EVALUATED").upper(),
            "complete": str(stages.get(stage) or "").upper() == "PASSED",
        }
        for name, stage in first_five
    ]
    rr_state = str(stages.get("tp_rr") or "NOT_EVALUATED").upper()
    reached_rr = all(item["complete"] for item in components) or rr_state == "BLOCKED"
    if reached_rr:
        components.append({
            "name": "tp_rr_validation",
            "stage": "tp_rr",
            "state": rr_state,
            "complete": rr_state == "PASSED",
        })
    weight = 100.0 / max(1, len(components))
    for component in components:
        component["weight"] = round(weight, 6)
        component["contribution"] = round(weight if component["complete"] else 0.0, 6)
    progress = round(sum(item["contribution"] for item in components))
    return {
        "displayed_percent": progress,
        "formula": "completed_visible_components / visible_components",
        "components": components,
        "missing_required_stages": [
            item["name"] for item in components if not item["complete"]
        ],
    }


def _decision(result):
    signal = str(result.get("final_signal") or result.get("signal") or "WAIT").upper()
    if signal in {"BUY", "SELL"}:
        return f"{signal}_READY"
    reason = str(
        result.get("blocked_reason")
        or result.get("block_reason")
        or result.get("blocked_by")
        or "WAIT"
    ).upper()
    hard_blocks = (
        "EMA", "CONSOLIDATION", "STALE", "COOLDOWN", "ACTIVE_POSITION",
        "NEWS", "RISK", "SL_", "INVALID_RR", "BEFORE_PREVIOUS_CLOSE",
        "MARKET_CLOSED", "CTRADER", "FEED",
    )
    return "BLOCKED" if any(token in reason for token in hard_blocks) else "WAIT"


def build_snapshot(symbol, result, data_5m=None, data_15m=None, source_state=None, now=None):
    now = now or datetime.now(timezone.utc)
    cycle_id = str(uuid.uuid4())
    breakout = result.get("fifteen_m_swing_break") or {}
    trend = result.get("trend_15m") or {}
    confirmation = result.get("confirmation_5m") or {}
    consolidation = result.get("consolidation") or {}
    breakout = breakout if isinstance(breakout, dict) else {}
    trend = trend if isinstance(trend, dict) else {}
    confirmation = confirmation if isinstance(confirmation, dict) else {}
    consolidation = consolidation if isinstance(consolidation, dict) else {}
    raw_swings = breakout.get("raw_swings") or []
    qualified_swings = breakout.get("swings") or []
    minimum_size = 1.0 if str(symbol).upper() == "XAUUSD" else 0.001
    watched_high = _latest_swing(qualified_swings, "HIGH")
    watched_low = _latest_swing(qualified_swings, "LOW")
    latest_rejected_high = _latest_swing([s for s in raw_swings if not s.get("valid")], "HIGH")
    latest_rejected_low = _latest_swing([s for s in raw_swings if not s.get("valid")], "LOW")
    side = str(breakout.get("side") or "WAIT").upper()
    level = breakout.get("level")
    buffer_value = breakout.get("bos_buffer")
    required_price = None
    try:
        if side == "BUY":
            required_price = float(level) + float(buffer_value or 0)
        elif side == "SELL":
            required_price = float(level) - float(buffer_value or 0)
    except (TypeError, ValueError):
        required_price = None
    prior_close = _utc_datetime(result.get("previous_position_closed_at"))
    bos_close_time = _utc_datetime(breakout.get("break_close_time"))
    bos_fresh = None if not bos_close_time else (prior_close is None or bos_close_time > prior_close)
    progress = _progress_snapshot(result)
    reason = (
        result.get("blocked_reason")
        or result.get("block_reason")
        or result.get("blocked_by")
        or breakout.get("reason")
        or result.get("plan_reason")
    )
    gates = {
        "ema": "EMA" in str(reason or "").upper(),
        "consolidation": "CONSOLIDATION" in str(reason or "").upper(),
        "stale_data": any(token in str(reason or "").upper() for token in ("STALE", "FEED")),
        "confirmation": "5M" in str(reason or "").upper(),
        "risk": any(token in str(reason or "").upper() for token in ("RISK", "SL_", "RR")),
        "cooldown": "COOLDOWN" in str(reason or "").upper(),
        "active_position": "ACTIVE_POSITION" in str(reason or "").upper(),
        "other": None,
    }
    gates["other"] = bool(reason) and not any(value for key, value in gates.items() if key != "other")
    ema_slope = consolidation.get("ema9_three_candle_slope")
    noise_metrics = {
        key: value for key, value in consolidation.items()
        if key not in {"is_consolidation", "reason", "symbol"}
    }
    risk_percent = result.get("final_risk_percent", result.get("risk_percent"))
    sl_debug = result.get("swing_sl_debug") or {}
    sl_used_price = sl_debug.get("sl_swing_used")
    sl_source_swing = next(
        (
            swing for swing in reversed(qualified_swings)
            if sl_used_price is not None
            and abs(float(swing.get("price")) - float(sl_used_price)) < 1e-9
        ),
        None,
    )
    snapshot = {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "session_id": SESSION_ID,
        "symbol": str(symbol).upper(),
        "timing": {
            "evaluation_timestamp": now.isoformat(),
            "latest_closed_m15_timestamp": (
                _latest_closed_timestamp(data_15m, 15, now) or
                _utc_datetime((result.get("strategy_cycle") or {}).get("evaluated_m15_close_time"))
            ),
            "latest_closed_m5_timestamp": _latest_closed_timestamp(data_5m, 5, now),
            "backend_session_id": SESSION_ID,
        },
        "trend": {
            "ema_fast": trend.get("ema_fast"),
            "ema_slow": trend.get("ema_slow"),
            "ema_slope": ema_slope,
            "close": trend.get("close"),
            "buy_allowed": bool(trend.get("buy_allowed")),
            "sell_allowed": bool(trend.get("sell_allowed")),
            "classification": trend.get("trend") or "NEUTRAL",
            "reason": trend.get("reason"),
        },
        "swings": {
            "watched_high": _swing_record(watched_high, minimum_size),
            "watched_low": _swing_record(watched_low, minimum_size),
            "latest_rejected_high": _swing_record(latest_rejected_high, minimum_size),
            "latest_rejected_low": _swing_record(latest_rejected_low, minimum_size),
            "selected": _swing_record(breakout.get("swing"), minimum_size),
            "raw_count": len(raw_swings),
            "qualified_count": len(qualified_swings),
        },
        "bos": {
            "watched_level": level,
            "watched_buy_level": watched_high.get("price") if watched_high else None,
            "watched_sell_level": watched_low.get("price") if watched_low else None,
            "direction": side,
            "buffer": buffer_value,
            "required_break_price": required_price,
            "actual_closed_candle_price": (
                breakout.get("break_close")
                if breakout.get("break_close") is not None
                else trend.get("close")
            ),
            "status": (result.get("strategy_stage_states") or {}).get("fifteen_m_bos"),
            "candle_timestamp": breakout.get("break_close_time") or breakout.get("break_time"),
            "fresh": bos_fresh,
            "remembered": bool(breakout.get("remembered")),
            "reason": breakout.get("reason") or reason,
        },
        "m5_confirmation": {
            "required_direction": side,
            "status": (result.get("strategy_stage_states") or {}).get("five_m_confirmation"),
            "candle_timestamp": confirmation.get("confirmation_close_time"),
            "price": confirmation.get("close"),
            "reason": confirmation.get("reason") or "NOT_EVALUATED",
        },
        "noise_consolidation": {
            "metrics": noise_metrics,
            "score": consolidation.get("conditions_met"),
            "classification": "CONSOLIDATION" if consolidation.get("is_consolidation") else "STRUCTURE",
            "blocked": bool(consolidation.get("is_consolidation")),
            "block_reason": consolidation.get("reason"),
        },
        "progress": progress,
        "trade_plan": {
            "direction": signal if (signal := str(result.get("final_signal") or "WAIT").upper()) in {"BUY", "SELL"} else side,
            "entry": result.get("entry_price"),
            "sl": result.get("stop_loss"),
            "sl_source_swing": sl_source_swing or sl_used_price,
            "sl_buffer": sl_debug.get("sl_buffer", sl_debug.get("buffer")),
            "sl_distance": sl_debug.get("risk", sl_debug.get("distance")),
            "sl_distance_points": sl_debug.get("sl_distance_points", sl_debug.get("distance_points")),
            "tp1": result.get("tp1"),
            "tp2": result.get("tp2"),
            "r_multiple": result.get("risk_reward_ratio"),
            "risk_amount": result.get("risk_amount"),
            "risk_percent": risk_percent,
            "validation_result": (result.get("strategy_stage_states") or {}).get("tp_rr"),
        },
        "final_decision": {
            "decision": _decision(result),
            "reason": reason,
            "prevented_by": gates,
        },
        "source_state": source_state or {},
    }
    return _json_safe(snapshot)


def _cleanup_if_due(db, now):
    global _last_cleanup_at
    if _last_cleanup_at and (now - _last_cleanup_at).total_seconds() < RETENTION_CLEANUP_INTERVAL_SECONDS:
        return
    cutoff = now - timedelta(days=RETENTION_DAYS)
    db.execute(delete(StrategyCycleDiagnostic).where(
        StrategyCycleDiagnostic.evaluation_timestamp < cutoff
    ))
    _last_cleanup_at = now


def persist_cycle_safely(symbol, result, data_5m=None, data_15m=None, source_state=None):
    """Persist without ever changing or interrupting the supplied result."""
    try:
        snapshot = build_snapshot(symbol, result, data_5m, data_15m, source_state)
        timing = snapshot["timing"]
        now = _utc_datetime(timing["evaluation_timestamp"])
        row = StrategyCycleDiagnostic(
            cycle_id=snapshot["cycle_id"],
            session_id=snapshot["session_id"],
            symbol=snapshot["symbol"],
            evaluation_timestamp=now,
            latest_closed_m15_timestamp=_utc_datetime(timing["latest_closed_m15_timestamp"]),
            latest_closed_m5_timestamp=_utc_datetime(timing["latest_closed_m5_timestamp"]),
            decision=snapshot["final_decision"]["decision"],
            block_reason=snapshot["final_decision"]["reason"],
            progress_percent=snapshot["progress"]["displayed_percent"],
            snapshot_json=snapshot,
            created_at=now,
        )
        db = SessionLocal()
        try:
            db.add(row)
            _cleanup_if_due(db, now)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
        result["audit_diagnostics"] = snapshot
        return snapshot
    except Exception as exc:
        print("STRATEGY_DIAGNOSTICS_PERSISTENCE_WARNING =", {
            "symbol": symbol,
            "error_type": type(exc).__name__,
            "error": str(exc),
        })
        return None


def query_cycles(symbol=None, start=None, end=None, decision=None, block_reason=None, limit=100, offset=0):
    statement = select(StrategyCycleDiagnostic)
    if symbol:
        statement = statement.where(StrategyCycleDiagnostic.symbol == str(symbol).upper())
    if start:
        statement = statement.where(StrategyCycleDiagnostic.evaluation_timestamp >= _utc_datetime(start))
    if end:
        statement = statement.where(StrategyCycleDiagnostic.evaluation_timestamp <= _utc_datetime(end))
    if decision:
        statement = statement.where(StrategyCycleDiagnostic.decision == str(decision).upper())
    if block_reason:
        statement = statement.where(StrategyCycleDiagnostic.block_reason == block_reason)
    statement = statement.order_by(StrategyCycleDiagnostic.evaluation_timestamp.desc()).offset(max(0, offset)).limit(min(500, max(1, limit)))
    db = SessionLocal()
    try:
        return [row.snapshot_json for row in db.execute(statement).scalars().all()]
    finally:
        db.close()


def update_execution_outcome_safely(cycle_id, decision, reason=None, details=None):
    """Attach the later execution-gate outcome to the same coherent cycle."""
    if not cycle_id:
        return False
    try:
        db = SessionLocal()
        try:
            row = db.execute(
                select(StrategyCycleDiagnostic)
                .where(StrategyCycleDiagnostic.cycle_id == str(cycle_id))
                .with_for_update()
            ).scalar_one_or_none()
            if row is None:
                return False
            snapshot = dict(row.snapshot_json or {})
            final = dict(snapshot.get("final_decision") or {})
            final.update({
                "decision": str(decision).upper(),
                "reason": reason,
                "execution_details": _json_safe(details or {}),
                "execution_updated_at": datetime.now(timezone.utc).isoformat(),
            })
            snapshot["final_decision"] = final
            row.snapshot_json = snapshot
            row.decision = final["decision"]
            row.block_reason = reason
            db.commit()
            return True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    except Exception as exc:
        print("STRATEGY_DIAGNOSTICS_EXECUTION_UPDATE_WARNING =", {
            "cycle_id": cycle_id,
            "error_type": type(exc).__name__,
            "error": str(exc),
        })
        return False
