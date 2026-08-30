"""Best-effort Forex lifecycle and immutable submission observability.

All public persistence entry points swallow failures.  They observe finalized
production values and never return a value consumed by trading decisions.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import logging
import os
import uuid

from sqlalchemy import delete, select

from db import SessionLocal
from models import ForexExecutionSnapshot, ForexLifecycleEvaluation
from services.forex_shadow_freshness_service import evaluate_shadow_policies


logger = logging.getLogger(__name__)
SESSION_ID = os.getenv("RENDER_INSTANCE_ID") or f"boot-{uuid.uuid4()}"
DEPLOYMENT_SHA = (
    os.getenv("RENDER_GIT_COMMIT")
    or os.getenv("GIT_COMMIT")
    or os.getenv("SOURCE_VERSION")
    or "unknown"
)
DEFAULT_LIFECYCLE_RETENTION_DAYS = 180
LIFECYCLE_RETENTION_DAYS = max(
    120,
    int(os.getenv("FOREX_LIFECYCLE_RETENTION_DAYS", str(DEFAULT_LIFECYCLE_RETENTION_DAYS))),
)
_last_cleanup_at = None


def _utc(value):
    if value in (None, ""):
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


def point_size(symbol):
    return Decimal("0.01") if str(symbol).upper() == "XAUUSD" else Decimal("0.00001")


def _normalized_price(symbol, value):
    if value in (None, ""):
        return None
    try:
        units = (Decimal(str(value)) / point_size(symbol)).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        return None
    return str(units)


def _canonical_time(value):
    parsed = _utc(value)
    return parsed.isoformat(timespec="seconds") if parsed else None


def _fingerprint(payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _json_safe(value):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _canonical_time(value)
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


def deterministic_event_id(symbol, event_type, direction, close_time, broken_level,
                           invalidation_swing_time, invalidation_swing_price):
    if not direction or not close_time or broken_level in (None, ""):
        return None
    return _fingerprint({
        "symbol": str(symbol).upper(),
        "event_type": str(event_type or "BOS").upper(),
        "direction": str(direction).upper(),
        "event_close_time": _canonical_time(close_time),
        "broken_level_points": _normalized_price(symbol, broken_level),
        "invalidation_swing_time": _canonical_time(invalidation_swing_time),
        "invalidation_swing_price_points": _normalized_price(symbol, invalidation_swing_price),
    })


def deterministic_confirmation_id(symbol, event_id, close_time, direction, close_price):
    if not event_id or not close_time or close_price in (None, ""):
        return None
    return _fingerprint({
        "symbol": str(symbol).upper(),
        "event_id": event_id,
        "m5_close_time": _canonical_time(close_time),
        "direction": str(direction or "").upper(),
        "m5_close_points": _normalized_price(symbol, close_price),
    })


def displacement(symbol, entry_price, confirmation_close):
    if entry_price in (None, "") or confirmation_close in (None, ""):
        return (None, None)
    try:
        price = abs(Decimal(str(entry_price)) - Decimal(str(confirmation_close)))
        return (float(price), float(price / point_size(symbol)))
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        return (None, None)


def _event_from_snapshot(snapshot):
    symbol = snapshot.get("symbol")
    bos = snapshot.get("bos") or {}
    swings = snapshot.get("swings") or {}
    invalidation = swings.get("event_invalidation") or {}
    direction = str(bos.get("direction") or "").upper()
    close_time = bos.get("candle_timestamp")
    level = bos.get("watched_level")
    event_type = str(bos.get("classification") or "BOS").upper()
    swing_time = bos.get("event_invalidation_swing_time") or invalidation.get("timestamp")
    swing_price = bos.get("event_invalidation_swing_price") or invalidation.get("price")
    event_id = deterministic_event_id(
        symbol, event_type, direction, close_time, level, swing_time, swing_price
    ) if direction in {"BUY", "SELL"} else None
    return {
        "event_id": event_id,
        "event_type": event_type if event_id else None,
        "event_direction": direction if event_id else None,
        "event_close_time": _utc(close_time),
        "event_broken_level": level,
        "event_invalidation_swing_time": _utc(swing_time),
        "event_invalidation_swing_price": swing_price,
        "event_age_candles": bos.get("event_age_candles"),
    }


def _previous_for_scope(db, symbol, account_scope):
    return db.execute(
        select(ForexLifecycleEvaluation)
        .where(
            ForexLifecycleEvaluation.symbol == symbol,
            ForexLifecycleEvaluation.account_scope == account_scope,
        )
        .order_by(ForexLifecycleEvaluation.evaluated_at.desc(), ForexLifecycleEvaluation.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def _first_confirmation_seen(db, confirmation_id):
    if not confirmation_id:
        return None
    return db.execute(
        select(ForexLifecycleEvaluation.confirmation_first_observed_at)
        .where(ForexLifecycleEvaluation.confirmation_id == confirmation_id)
        .order_by(ForexLifecycleEvaluation.evaluated_at.asc())
        .limit(1)
    ).scalar_one_or_none()


def _cleanup_if_due(db, now):
    global _last_cleanup_at
    if _last_cleanup_at and (now - _last_cleanup_at).total_seconds() < 3600:
        return
    db.execute(delete(ForexLifecycleEvaluation).where(
        ForexLifecycleEvaluation.evaluated_at < now - timedelta(days=LIFECYCLE_RETENTION_DAYS)
    ))
    _last_cleanup_at = now


def build_lifecycle_values(snapshot, previous=None, first_confirmation_seen=None, now=None,
                           account_scope="FOREX_DEFAULT"):
    frozen = copy.deepcopy(snapshot or {})
    now = _utc(now) or _utc((frozen.get("timing") or {}).get("evaluation_timestamp")) or datetime.now(timezone.utc)
    symbol = str(frozen.get("symbol") or "").upper()
    event = _event_from_snapshot(frozen)
    current_event_id = event["event_id"]
    previous_event_id = getattr(previous, "event_id", None)
    previous_present = bool(getattr(previous, "setup_present", False))
    setup_present = bool(current_event_id)

    if setup_present and current_event_id == previous_event_id:
        revival = not previous_present
        revival_count = int(getattr(previous, "revival_count", 0) or 0) + int(revival)
        setup_generation = int(getattr(previous, "setup_generation", 1) or 1) + int(revival)
        absent_since = None
        reappeared_at = now if revival else getattr(previous, "setup_last_reappeared_at", None)
        absent_seconds = (
            max(0.0, (now - _utc(getattr(previous, "setup_absent_since", None))).total_seconds())
            if revival and _utc(getattr(previous, "setup_absent_since", None)) else None
        )
    elif setup_present:
        revival_count, setup_generation = 0, 1
        absent_since, reappeared_at, absent_seconds = None, None, None
    else:
        event = {
            "event_id": previous_event_id,
            "event_type": getattr(previous, "event_type", None),
            "event_direction": getattr(previous, "event_direction", None),
            "event_close_time": getattr(previous, "event_close_time", None),
            "event_broken_level": getattr(previous, "event_broken_level", None),
            "event_invalidation_swing_time": getattr(previous, "event_invalidation_swing_time", None),
            "event_invalidation_swing_price": getattr(previous, "event_invalidation_swing_price", None),
            "event_age_candles": getattr(previous, "event_age_candles", None),
        }
        revival_count = int(getattr(previous, "revival_count", 0) or 0)
        setup_generation = int(getattr(previous, "setup_generation", 0) or 0)
        absent_since = (
            getattr(previous, "setup_absent_since", None)
            if previous and not previous_present else now
        )
        reappeared_at = getattr(previous, "setup_last_reappeared_at", None)
        absent_seconds = (
            max(0.0, (now - _utc(absent_since)).total_seconds()) if absent_since else None
        )

    confirmation = frozen.get("m5_confirmation") or {}
    confirmation_time = _utc(confirmation.get("candle_timestamp"))
    confirmation_close = confirmation.get("price")
    confirmation_id = deterministic_confirmation_id(
        symbol, event.get("event_id"), confirmation_time,
        event.get("event_direction"), confirmation_close,
    )
    if not setup_present and previous is not None:
        confirmation_id = getattr(previous, "confirmation_id", None)
        confirmation_time = _utc(getattr(previous, "confirmation_time", None))
        confirmation_close = getattr(previous, "confirmation_close", None)
        first_confirmation_seen = getattr(
            previous, "confirmation_first_observed_at", first_confirmation_seen
        )
    first_observed = _utc(first_confirmation_seen) or (now if confirmation_id else None)
    confirmation_age = (
        max(0.0, (now - confirmation_time).total_seconds()) if confirmation_time else None
    )
    previous_confirmation_id = getattr(previous, "confirmation_id", None)
    confirmation_generation = int(getattr(previous, "confirmation_generation", 0) or 0)
    if confirmation_id and confirmation_id != previous_confirmation_id:
        confirmation_generation += 1
    confirmation_reused = bool(
        setup_present and revival_count > 0 and confirmation_id
        and confirmation_id == previous_confirmation_id
    )
    new_after_revival = bool(
        confirmation_time and reappeared_at and confirmation_time > _utc(reappeared_at)
    )
    setup_was_absent_after_confirmation = bool(
        revival_count > 0 and confirmation_time and reappeared_at
        and confirmation_time <= _utc(reappeared_at)
    )
    trade_plan = frozen.get("trade_plan") or {}
    latest_closed_m15 = _utc((frozen.get("timing") or {}).get("latest_closed_m15_timestamp"))
    event_close = _utc(event.get("event_close_time"))
    if latest_closed_m15 and event_close:
        event["event_age_candles"] = max(
            0,
            int((latest_closed_m15 - event_close).total_seconds() // (15 * 60)),
        )
    displacement_price, displacement_points = displacement(
        symbol, trade_plan.get("entry"), confirmation_close
    )
    final = frozen.get("final_decision") or {}
    signal_ready = str(final.get("decision") or "").endswith("_READY")
    values = {
        **event,
        "strategy_evaluation_id": str(frozen.get("cycle_id") or uuid.uuid4()),
        "evaluated_at": now,
        "session_id": str(frozen.get("session_id") or SESSION_ID),
        "deployment_sha": DEPLOYMENT_SHA,
        "symbol": symbol,
        "account_scope": account_scope,
        "setup_present": setup_present,
        "setup_status": "PRESENT" if setup_present else "ABSENT",
        "setup_invalid_reason": None if setup_present else final.get("reason"),
        "setup_generation": setup_generation,
        "revival_count": revival_count,
        "setup_absent_since": _utc(absent_since),
        "setup_last_reappeared_at": _utc(reappeared_at),
        "time_setup_was_absent_seconds": absent_seconds,
        "confirmation_id": confirmation_id,
        "confirmation_time": confirmation_time,
        "confirmation_close": confirmation_close,
        "confirmation_first_observed_at": first_observed,
        "confirmation_age_seconds": confirmation_age,
        "confirmation_generation": confirmation_generation,
        "confirmation_reused": confirmation_reused,
        "new_confirmation_after_revival": new_after_revival,
        "ema_state": (frozen.get("trend") or {}).get("classification"),
        "entry_candidate_price": trade_plan.get("entry"),
        "displacement_points": displacement_points,
        "rr_at_evaluation": trade_plan.get("r_multiple"),
        "signal_ready": signal_ready,
        "final_block_reason": final.get("reason"),
        "order_attempted": False,
        "order_id": None,
        "position_id": None,
        "shadow_only": False,
        "details_json": {
            "displacement_price": displacement_price,
            "point_size": float(point_size(symbol)),
            "setup_was_absent_after_confirmation": setup_was_absent_after_confirmation,
            "shadow_outcome_tracking": "ELIGIBILITY_ONLY",
        },
    }
    policy_input = {
        **values,
        "setup_was_absent_after_confirmation": setup_was_absent_after_confirmation,
    }
    try:
        values["shadow_policy_results"] = evaluate_shadow_policies(policy_input)
    except Exception as exc:
        logger.exception("FOREX_SHADOW_FRESHNESS_EVALUATION_FAILED")
        values["shadow_policy_results"] = {
            "shadow_only": True,
            "error": type(exc).__name__,
            "reason": "SHADOW_EVALUATION_FAILED_PRODUCTION_UNCHANGED",
        }
    return values


def persist_lifecycle_evaluation_safely(snapshot, source_state=None):
    db = None
    try:
        frozen = copy.deepcopy(snapshot or {})
        source = copy.deepcopy(source_state or {})
        symbol = str(frozen.get("symbol") or "").upper()
        account_scope = str(
            source.get("account_scope") or source.get("account_id")
            or os.getenv("ACTIVE_CTRADER_ACCOUNT_ID")
            or os.getenv("CTRADER_ACCOUNT_ID") or "FOREX_DEFAULT"
        )
        db = SessionLocal()
        previous = _previous_for_scope(db, symbol, account_scope)
        provisional = build_lifecycle_values(frozen, previous, None, account_scope=account_scope)
        first_seen = _first_confirmation_seen(db, provisional.get("confirmation_id"))
        values = build_lifecycle_values(
            frozen, previous, first_seen, account_scope=account_scope
        )
        row = ForexLifecycleEvaluation(**values)
        db.add(row)
        _cleanup_if_due(db, values["evaluated_at"])
        db.commit()
        return copy.deepcopy(values)
    except Exception:
        if db is not None:
            db.rollback()
        logger.exception("FOREX_LIFECYCLE_OBSERVER_FAILED")
        return None
    finally:
        if db is not None:
            db.close()


def persist_execution_snapshot_safely(*, symbol, direction, trade_payload, plan,
                                      quote, risk_size, gate_results):
    """Insert one immutable pre-submit snapshot; never update an existing row."""
    db = None
    try:
        attempted_at = datetime.now(timezone.utc)
        db = SessionLocal()
        lifecycle = db.execute(
            select(ForexLifecycleEvaluation)
            .where(ForexLifecycleEvaluation.symbol == str(symbol).upper())
            .order_by(ForexLifecycleEvaluation.evaluated_at.desc(), ForexLifecycleEvaluation.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        payload = copy.deepcopy(trade_payload or {})
        plan_copy = copy.deepcopy(plan or {})
        gates = copy.deepcopy(gate_results or {})
        quote_copy = copy.deepcopy(quote or {})
        snapshot_id = str(uuid.uuid4())
        broker_quote = quote_copy.get("ask") if str(direction).upper() == "BUY" else quote_copy.get("bid")
        confirmation_close = getattr(lifecycle, "confirmation_close", None)
        displacement_price, _ = displacement(symbol, broker_quote, confirmation_close)
        sl_debug = plan_copy.get("swing_sl_debug") or payload.get("swing_sl_debug") or {}
        risk_settings = gates.get("loss_limit_status") or {}
        snapshot = {
            "snapshot_id": snapshot_id,
            "snapshot_version": 1,
            "immutable": True,
            "production_sha": DEPLOYMENT_SHA,
            "backend_session_id": SESSION_ID,
            "symbol": str(symbol).upper(),
            "account_id": (
                payload.get("account_id") or os.getenv("ACTIVE_CTRADER_ACCOUNT_ID")
                or os.getenv("CTRADER_ACCOUNT_ID")
            ),
            "broker_environment": (
                payload.get("broker_environment") or os.getenv("ACTIVE_CTRADER_ACCOUNT_ENV")
                or payload.get("mode")
            ),
            "direction": str(direction).upper(),
            "event_id": getattr(lifecycle, "event_id", None),
            "event_type": getattr(lifecycle, "event_type", None),
            "event_direction": getattr(lifecycle, "event_direction", None),
            "event_close_time": _canonical_time(getattr(lifecycle, "event_close_time", None)),
            "event_broken_level": getattr(lifecycle, "event_broken_level", None),
            "event_age_candles": getattr(lifecycle, "event_age_candles", None),
            "event_invalidation_swing_type": sl_debug.get("sl_source_type"),
            "event_invalidation_swing_time": _canonical_time(getattr(lifecycle, "event_invalidation_swing_time", None)),
            "event_invalidation_swing_price": getattr(lifecycle, "event_invalidation_swing_price", None),
            "confirmation_id": getattr(lifecycle, "confirmation_id", None),
            "confirmation_time": _canonical_time(getattr(lifecycle, "confirmation_time", None)),
            "confirmation_close": confirmation_close,
            "confirmation_age_seconds": getattr(lifecycle, "confirmation_age_seconds", None),
            "confirmation_generation": getattr(lifecycle, "confirmation_generation", 0),
            "confirmation_reused": bool(getattr(lifecycle, "confirmation_reused", False)),
            "setup_revival_count": getattr(lifecycle, "revival_count", 0),
            "setup_last_reappeared_at": _canonical_time(getattr(lifecycle, "setup_last_reappeared_at", None)),
            "time_setup_was_absent_seconds": getattr(lifecycle, "time_setup_was_absent_seconds", None),
            "new_confirmation_after_revival": bool(getattr(lifecycle, "new_confirmation_after_revival", False)),
            "candidate_entry": payload.get("entry"),
            "broker_quote_at_submission": broker_quote,
            "price_displacement_from_confirmation": displacement_price,
            "sl_source_type": sl_debug.get("sl_source_type") or sl_debug.get("source"),
            "sl_source_time": sl_debug.get("sl_source_time") or sl_debug.get("swing_time"),
            "sl_source_price": sl_debug.get("sl_swing_used") or sl_debug.get("swing_price"),
            "sl_buffer": sl_debug.get("sl_buffer") or sl_debug.get("buffer"),
            "sl": payload.get("sl"), "tp1": payload.get("tp1"), "tp2": payload.get("tp2"),
            "risk_percent": risk_size.get("risk_percent"),
            "risk_amount": risk_size.get("risk_amount"),
            "volume": payload.get("volume_units") or payload.get("volume"),
            "ema_state": gates.get("ema_state"),
            "consolidation_result": gates.get("consolidation_result"),
            "news_decision": gates.get("news_decision"),
            "market_data_freshness_result": gates.get("market_data_freshness_result"),
            "cooldown_result": gates.get("cooldown_result"),
            "active_position_result": gates.get("active_position_result"),
            "daily_loss_cap_result": {
                "enabled": risk_settings.get("daily_limit_enabled"),
                "limit": risk_settings.get("maxDailyLoss"),
                "current_pl": risk_settings.get("daily_total_pl"),
                "blocked": risk_settings.get("blocked") and risk_settings.get("limit_type") == "daily",
            },
            "weekly_loss_cap_result": {
                "enabled": risk_settings.get("weekly_limit_enabled"),
                "limit": risk_settings.get("maxWeeklyLoss"),
                "current_pl": risk_settings.get("weekly_total_pl"),
                "blocked": risk_settings.get("blocked") and risk_settings.get("limit_type") == "weekly",
            },
            "configured_max_daily_loss": risk_settings.get("maxDailyLoss"),
            "configured_max_weekly_loss": risk_settings.get("maxWeeklyLoss"),
            "daily_loss_current": risk_settings.get("daily_total_pl"),
            "weekly_loss_current": risk_settings.get("weekly_total_pl"),
            "daily_cap_allowed": not bool(
                risk_settings.get("blocked") and risk_settings.get("limit_type") == "daily"
            ),
            "weekly_cap_allowed": not bool(
                risk_settings.get("blocked") and risk_settings.get("limit_type") == "weekly"
            ),
            "risk_recalculation_result": gates.get("risk_recalculation_result"),
            "order_attempted_at": attempted_at.isoformat(),
            "client_order_id": payload.get("client_order_id") or payload.get("signal_setup_id"),
            "broker_order_id": None, "position_id": None, "broker_response_at": None,
        }
        snapshot = _json_safe(snapshot)
        row = ForexExecutionSnapshot(
            snapshot_id=snapshot_id, snapshot_version=1,
            production_sha=DEPLOYMENT_SHA, backend_session_id=SESSION_ID,
            symbol=snapshot["symbol"], account_id=snapshot["account_id"],
            broker_environment=snapshot["broker_environment"], direction=snapshot["direction"],
            event_id=snapshot["event_id"], confirmation_id=snapshot["confirmation_id"],
            order_attempted_at=attempted_at, client_order_id=snapshot["client_order_id"],
            broker_order_id=None, position_id=None, broker_response_at=None,
            snapshot_json=snapshot, created_at=attempted_at,
        )
        db.add(row)
        if lifecycle is not None:
            lifecycle.order_attempted = True
            policies = copy.deepcopy(lifecycle.shadow_policy_results or {})
            for result in policies.values():
                if isinstance(result, dict):
                    result["production_executed"] = True
            lifecycle.shadow_policy_results = policies
        db.commit()
        return copy.deepcopy(snapshot)
    except Exception:
        if db is not None:
            db.rollback()
        logger.exception("FOREX_EXECUTION_SNAPSHOT_FAILED")
        return None
    finally:
        if db is not None:
            db.close()


def record_execution_response_safely(symbol, broker_result):
    """Link response IDs to the mutable evaluation row, not immutable snapshot."""
    db = None
    try:
        db = SessionLocal()
        row = db.execute(
            select(ForexLifecycleEvaluation)
            .where(
                ForexLifecycleEvaluation.symbol == str(symbol).upper(),
                ForexLifecycleEvaluation.order_attempted.is_(True),
            )
            .order_by(ForexLifecycleEvaluation.evaluated_at.desc(), ForexLifecycleEvaluation.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return
        result = copy.deepcopy(broker_result or {})
        row.order_id = result.get("order_id") or result.get("broker_order_id")
        row.position_id = result.get("position_id") or result.get("broker_position_id")
        db.commit()
    except Exception:
        if db is not None:
            db.rollback()
        logger.exception("FOREX_EXECUTION_RESPONSE_LINK_FAILED")
    finally:
        if db is not None:
            db.close()
