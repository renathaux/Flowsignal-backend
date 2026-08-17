"""Read-only hotfixes for V1 outcome reconciliation.

Handles persisted execution-detail shapes, a 60-day broker-history recovery
window, and partial-close aggregation without ever submitting/modifying orders.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import math
import time

from sqlalchemy import select

from ctrader_connector import fetch_ctrader_closed_deals, get_ctrader_config, get_open_positions
from db import SessionLocal
from models import StrategyCycleDiagnostic, StrategyShadowEvaluation, StrategyShadowTrade

logger = logging.getLogger(__name__)
_CACHE_SECONDS = 60
_RECOVERY_DAYS = 60
_last_at = 0.0
_last_result = None


def _id(value):
    return None if value in (None, "") else str(value)


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _nested(mapping, *path):
    value = mapping
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value if isinstance(value, dict) else None


def _execution_result(snapshot):
    """Read only broker execution payloads, never active-position overlays."""
    if not isinstance(snapshot, dict):
        return None
    candidates = [
        _nested(snapshot, "execution", "result"),
        _nested(snapshot, "final_decision", "execution_details"),
        _nested(snapshot, "execution_outcome", "details"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if any(candidate.get(key) is not None for key in (
            "position_id", "broker_position_id", "order_id", "broker_order_id"
        )):
            return candidate
    for value in snapshot.values():
        if isinstance(value, dict):
            found = _execution_result(value)
            if found:
                return found
    return None


def _walk_value(value, keys):
    if isinstance(value, dict):
        for key in keys:
            if value.get(key) not in (None, ""):
                return value.get(key)
        for child in value.values():
            found = _walk_value(child, keys)
            if found not in (None, ""):
                return found
    elif isinstance(value, list):
        for child in value:
            found = _walk_value(child, keys)
            if found not in (None, ""):
                return found
    return None


def _same_direction(evaluation, execution, snapshot):
    expected = str(evaluation.direction or "").upper()
    actual = str(
        execution.get("direction") or execution.get("side")
        or _walk_value(snapshot, ("direction", "signal", "final_signal")) or ""
    ).upper()
    return not expected or not actual or expected == actual


def _propagate(db, evaluation):
    trades = db.execute(select(StrategyShadowTrade).where(
        StrategyShadowTrade.v1_evaluation_id == evaluation.id
    )).scalars().all()
    for trade in trades:
        trade.v1_order_id = evaluation.v1_order_id
        trade.v1_position_id = evaluation.v1_position_id
        trade.v1_outcome_json = evaluation.v1_outcome_json


def _repair_links(db, since):
    diagnostics = db.execute(select(StrategyCycleDiagnostic).where(
        StrategyCycleDiagnostic.evaluation_timestamp >= since
    ).order_by(StrategyCycleDiagnostic.evaluation_timestamp.asc())).scalars().all()
    repaired = 0
    for diagnostic in diagnostics:
        snapshot = diagnostic.snapshot_json or {}
        execution = _execution_result(snapshot)
        if not execution:
            continue
        position_id = _id(execution.get("position_id") or execution.get("broker_position_id"))
        order_id = _id(execution.get("order_id") or execution.get("broker_order_id"))
        if not position_id and not order_id:
            continue
        linked = db.execute(select(StrategyShadowEvaluation.id).where(
            (StrategyShadowEvaluation.v1_position_id == position_id)
            if position_id else (StrategyShadowEvaluation.v1_order_id == order_id)
        ).limit(1)).scalar_one_or_none()
        if linked:
            continue
        lower = diagnostic.evaluation_timestamp - timedelta(minutes=15)
        upper = diagnostic.evaluation_timestamp + timedelta(minutes=15)
        candidates = db.execute(select(StrategyShadowEvaluation).where(
            StrategyShadowEvaluation.symbol == diagnostic.symbol,
            StrategyShadowEvaluation.v1_decision == "TRADE",
            StrategyShadowEvaluation.evaluated_at >= lower,
            StrategyShadowEvaluation.evaluated_at <= upper,
            StrategyShadowEvaluation.v1_position_id.is_(None),
            StrategyShadowEvaluation.v1_order_id.is_(None),
        )).scalars().all()
        candidates = [row for row in candidates if _same_direction(row, execution, snapshot)]
        if not candidates:
            continue
        candidates.sort(key=lambda row: abs((row.evaluated_at - diagnostic.evaluation_timestamp).total_seconds()))
        candidate = candidates[0]
        if len(candidates) > 1:
            gaps = [abs((row.evaluated_at - diagnostic.evaluation_timestamp).total_seconds()) for row in candidates[:2]]
            if abs(gaps[1] - gaps[0]) < 30:
                continue
        candidate.v1_position_id = position_id
        candidate.v1_order_id = order_id
        payload = dict(candidate.v1_outcome_json or {})
        payload.update({
            "closed": False,
            "execution_linked": True,
            "position_id": position_id,
            "order_id": order_id,
            "entry": execution.get("entry") or execution.get("entry_price"),
            "sl": execution.get("sl") or execution.get("stop_loss"),
            "tp": execution.get("tp") or execution.get("tp2") or execution.get("take_profit"),
            "source": "strategy_cycle_diagnostics",
        })
        candidate.v1_outcome_json = payload
        _propagate(db, candidate)
        repaired += 1
    return repaired


def _active_position_ids():
    try:
        payload = get_open_positions() or []
    except Exception:
        return set()
    found = set()
    def walk(value):
        if isinstance(value, dict):
            pid = value.get("position_id") or value.get("positionId") or value.get("broker_position_id")
            if pid not in (None, ""):
                found.add(str(pid))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(payload)
    return found


def _deal_groups(deals):
    by_position = {}
    by_order = {}
    for deal in deals or []:
        position_id = _id(deal.get("position_id") or deal.get("broker_position_id"))
        order_id = _id(deal.get("order_id") or deal.get("broker_order_id"))
        if position_id:
            by_position.setdefault(position_id, []).append(deal)
        if order_id:
            by_order.setdefault(order_id, []).append(deal)
    return by_position, by_order


def _closed_payload(group, evaluation):
    pnl_values = []
    for deal in group:
        pnl = _number(deal.get("broker_realized_profit") if deal.get("broker_realized_profit") is not None else deal.get("pnl"))
        if pnl is not None:
            pnl_values.append(pnl)
    total_pnl = sum(pnl_values) if pnl_values else None
    outcome = "WIN" if total_pnl is not None and total_pnl > 0 else "LOSS" if total_pnl is not None and total_pnl < 0 else "BREAKEVEN" if total_pnl == 0 else "CLOSED"
    latest = max(group, key=lambda item: _number(item.get("closed_at")) or 0)
    payload = dict(evaluation.v1_outcome_json or {})
    payload.update({
        "closed": True,
        "outcome": outcome,
        "realized_pnl": round(total_pnl, 2) if total_pnl is not None else None,
        "closed_at": latest.get("closed_at"),
        "close_price": latest.get("close_price"),
        "position_id": _id(latest.get("position_id") or latest.get("broker_position_id")),
        "order_id": _id(latest.get("order_id") or latest.get("broker_order_id")),
        "deal_ids": [_id(item.get("deal_id")) for item in group if _id(item.get("deal_id"))],
        "closing_deal_count": len(group),
        "source": "ctrader_closed_history_aggregated",
    })
    return payload


def _apply_closed(db, deals, active_ids):
    by_position, by_order = _deal_groups(deals)
    rows = db.execute(select(StrategyShadowEvaluation).where(
        (StrategyShadowEvaluation.v1_position_id.is_not(None)) |
        (StrategyShadowEvaluation.v1_order_id.is_not(None))
    )).scalars().all()
    updated = 0
    for row in rows:
        position_id = _id(row.v1_position_id)
        if position_id and position_id in active_ids:
            continue
        group = by_position.get(position_id) if position_id else None
        if not group and row.v1_order_id:
            group = by_order.get(str(row.v1_order_id))
        if not group:
            continue
        payload = _closed_payload(group, row)
        if payload != (row.v1_outcome_json or {}):
            row.v1_outcome_json = payload
            _propagate(db, row)
            updated += 1
    return updated


def reconcile_v1_outcomes_safely(force=False):
    global _last_at, _last_result
    now_mono = time.monotonic()
    if not force and _last_result is not None and now_mono - _last_at < _CACHE_SECONDS:
        return _last_result
    db = None
    try:
        db = SessionLocal()
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=_RECOVERY_DAYS)
        repaired = _repair_links(db, since)
        config = get_ctrader_config()
        deals = []
        if config:
            deals = fetch_ctrader_closed_deals(
                config,
                int(since.timestamp() * 1000),
                int(now.timestamp() * 1000),
                max_rows=1000,
            ) or []
        active_ids = _active_position_ids()
        closed = _apply_closed(db, deals, active_ids)
        db.commit()
        result = {
            "ok": True,
            "execution_links_repaired": repaired,
            "closed_outcomes_updated": closed,
            "broker_closed_deals_read": len(deals),
            "active_positions_checked": len(active_ids),
            "recovery_days": _RECOVERY_DAYS,
            "partial_close_safe": True,
            "read_only_broker": True,
        }
    except Exception as exc:
        if db is not None:
            db.rollback()
        logger.warning("V1_OUTCOME_RECONCILIATION_HOTFIX_WARNING error=%s", str(exc))
        result = {"ok": False, "error": str(exc), "read_only_broker": True}
    finally:
        if db is not None:
            db.close()
    _last_at = now_mono
    _last_result = result
    return result
