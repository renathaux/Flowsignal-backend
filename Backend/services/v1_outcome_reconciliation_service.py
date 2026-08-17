"""Best-effort V1 execution/outcome reconciliation for V1-vs-V2 observability.

This module is deliberately outside the V2 shadow evaluator's safety boundary.
It reads cTrader closed-deal history and writes only shadow comparison metadata.
It never submits, modifies, closes, or retries a broker order.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import math
import time

from sqlalchemy import select

from ctrader_connector import get_closed_deals_for_current_month
from db import SessionLocal
from models import StrategyCycleDiagnostic, StrategyShadowEvaluation, StrategyShadowTrade


logger = logging.getLogger(__name__)
_RECONCILE_CACHE_SECONDS = 60
_last_reconcile_at = 0.0
_last_reconcile_result = None


def _id(value):
    if value in (None, ""):
        return None
    return str(value)


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _epoch_to_iso(value):
    number = _number(value)
    if number is None:
        return value if isinstance(value, str) else None
    if number > 10_000_000_000:
        number /= 1000.0
    try:
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def _execution_result(snapshot):
    """Return only a broker execution result, never an active-position overlay."""
    if not isinstance(snapshot, dict):
        return None

    execution = snapshot.get("execution")
    if isinstance(execution, dict):
        result = execution.get("result")
        if isinstance(result, dict) and (
            result.get("position_id") is not None
            or result.get("order_id") is not None
        ):
            return result

    # Older diagnostics sometimes nested the execution block one level deeper.
    for value in snapshot.values():
        if isinstance(value, dict):
            found = _execution_result(value)
            if found:
                return found
    return None


def _snapshot_value(snapshot, *keys):
    if not isinstance(snapshot, dict):
        return None
    for key in keys:
        if snapshot.get(key) not in (None, ""):
            return snapshot.get(key)
    for value in snapshot.values():
        if isinstance(value, dict):
            found = _snapshot_value(value, *keys)
            if found not in (None, ""):
                return found
    return None


def _same_direction(evaluation, execution, snapshot):
    expected = str(evaluation.direction or "").upper()
    actual = str(
        execution.get("direction")
        or execution.get("side")
        or _snapshot_value(snapshot, "direction", "signal", "final_signal")
        or ""
    ).upper()
    return not expected or not actual or expected == actual


def _repair_execution_links(db, since):
    """Recover missing V1 broker IDs from durable execution diagnostics.

    Matching is intentionally conservative: same symbol, V1 TRADE decision,
    same direction when available, and a maximum 15-minute timestamp gap.
    """
    diagnostics = db.execute(
        select(StrategyCycleDiagnostic)
        .where(StrategyCycleDiagnostic.evaluation_timestamp >= since)
        .order_by(StrategyCycleDiagnostic.evaluation_timestamp.asc())
    ).scalars().all()

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

        already_linked = db.execute(
            select(StrategyShadowEvaluation.id).where(
                (StrategyShadowEvaluation.v1_position_id == position_id)
                if position_id
                else (StrategyShadowEvaluation.v1_order_id == order_id)
            ).limit(1)
        ).scalar_one_or_none()
        if already_linked:
            continue

        lower = diagnostic.evaluation_timestamp - timedelta(minutes=15)
        upper = diagnostic.evaluation_timestamp + timedelta(minutes=15)
        candidates = db.execute(
            select(StrategyShadowEvaluation)
            .where(
                StrategyShadowEvaluation.symbol == diagnostic.symbol,
                StrategyShadowEvaluation.v1_decision == "TRADE",
                StrategyShadowEvaluation.evaluated_at >= lower,
                StrategyShadowEvaluation.evaluated_at <= upper,
                StrategyShadowEvaluation.v1_position_id.is_(None),
                StrategyShadowEvaluation.v1_order_id.is_(None),
            )
        ).scalars().all()
        candidates = [
            row for row in candidates
            if _same_direction(row, execution, snapshot)
        ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda row: abs((row.evaluated_at - diagnostic.evaluation_timestamp).total_seconds())
        )
        candidate = candidates[0]
        if len(candidates) > 1:
            first_gap = abs((candidate.evaluated_at - diagnostic.evaluation_timestamp).total_seconds())
            second_gap = abs((candidates[1].evaluated_at - diagnostic.evaluation_timestamp).total_seconds())
            # Avoid guessing when two evaluations are effectively equally plausible.
            if abs(second_gap - first_gap) < 30:
                continue

        candidate.v1_position_id = position_id
        candidate.v1_order_id = order_id
        existing = dict(candidate.v1_outcome_json or {})
        existing.update({
            "closed": False,
            "execution_linked": True,
            "position_id": position_id,
            "order_id": order_id,
            "entry": execution.get("entry") or execution.get("entry_price"),
            "sl": execution.get("sl") or execution.get("stop_loss"),
            "tp": execution.get("tp") or execution.get("tp2") or execution.get("take_profit"),
            "source": "strategy_cycle_diagnostics",
        })
        candidate.v1_outcome_json = existing
        _propagate_to_shadow_trade(db, candidate)
        repaired += 1
    return repaired


def _propagate_to_shadow_trade(db, evaluation):
    trades = db.execute(
        select(StrategyShadowTrade).where(
            StrategyShadowTrade.v1_evaluation_id == evaluation.id
        )
    ).scalars().all()
    for trade in trades:
        trade.v1_order_id = evaluation.v1_order_id
        trade.v1_position_id = evaluation.v1_position_id
        trade.v1_outcome_json = evaluation.v1_outcome_json


def _closed_payload(deal, evaluation):
    pnl = _number(
        deal.get("broker_realized_profit")
        if deal.get("broker_realized_profit") is not None
        else deal.get("pnl")
    )
    outcome = str(deal.get("result") or deal.get("status") or "").upper()
    if outcome not in {"WIN", "LOSS", "BREAKEVEN"}:
        if pnl is not None:
            outcome = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "BREAKEVEN"
        else:
            outcome = "CLOSED"
    existing = dict(evaluation.v1_outcome_json or {})
    existing.update({
        "closed": True,
        "outcome": outcome,
        "realized_pnl": pnl,
        "closed_at": _epoch_to_iso(deal.get("closed_at")),
        "close_price": deal.get("close_price"),
        "position_id": _id(deal.get("position_id") or deal.get("broker_position_id")),
        "order_id": _id(deal.get("order_id") or deal.get("broker_order_id")),
        "deal_id": _id(deal.get("deal_id")),
        "source": "ctrader_closed_history",
    })
    return existing


def _apply_closed_deals(db, deals):
    by_position = {}
    by_order = {}
    for deal in deals or []:
        position_id = _id(deal.get("position_id") or deal.get("broker_position_id"))
        order_id = _id(deal.get("order_id") or deal.get("broker_order_id"))
        if position_id:
            by_position[position_id] = deal
        if order_id:
            by_order[order_id] = deal

    evaluations = db.execute(
        select(StrategyShadowEvaluation).where(
            (StrategyShadowEvaluation.v1_position_id.is_not(None))
            | (StrategyShadowEvaluation.v1_order_id.is_not(None))
        )
    ).scalars().all()
    updated = 0
    for evaluation in evaluations:
        deal = None
        if evaluation.v1_position_id:
            deal = by_position.get(str(evaluation.v1_position_id))
        if deal is None and evaluation.v1_order_id:
            deal = by_order.get(str(evaluation.v1_order_id))
        if deal is None:
            continue
        payload = _closed_payload(deal, evaluation)
        if payload != (evaluation.v1_outcome_json or {}):
            evaluation.v1_outcome_json = payload
            _propagate_to_shadow_trade(db, evaluation)
            updated += 1
    return updated


def reconcile_v1_outcomes_safely(force=False):
    """Persist V1 broker links and closed outcomes without touching execution."""
    global _last_reconcile_at, _last_reconcile_result
    now_monotonic = time.monotonic()
    if (
        not force
        and _last_reconcile_result is not None
        and now_monotonic - _last_reconcile_at < _RECONCILE_CACHE_SECONDS
    ):
        return _last_reconcile_result

    db = None
    try:
        db = SessionLocal()
        since = datetime.now(timezone.utc) - timedelta(days=35)
        repaired = _repair_execution_links(db, since)
        deals = get_closed_deals_for_current_month(max_rows=500) or []
        closed_updated = _apply_closed_deals(db, deals)
        db.commit()
        result = {
            "ok": True,
            "execution_links_repaired": repaired,
            "closed_outcomes_updated": closed_updated,
            "broker_closed_deals_read": len(deals),
            "read_only_broker": True,
        }
    except Exception as exc:
        if db is not None:
            db.rollback()
        logger.warning("V1_OUTCOME_RECONCILIATION_WARNING error=%s", str(exc))
        result = {
            "ok": False,
            "error": str(exc),
            "read_only_broker": True,
        }
    finally:
        if db is not None:
            db.close()
    _last_reconcile_at = now_monotonic
    _last_reconcile_result = result
    return result


def get_v1_actual_metrics(symbol=None):
    db = SessionLocal()
    try:
        statement = select(StrategyShadowEvaluation).where(
            StrategyShadowEvaluation.v1_decision == "TRADE",
            (StrategyShadowEvaluation.v1_position_id.is_not(None))
            | (StrategyShadowEvaluation.v1_order_id.is_not(None)),
        )
        if symbol:
            statement = statement.where(
                StrategyShadowEvaluation.symbol == str(symbol).upper()
            )
        rows = db.execute(
            statement.order_by(StrategyShadowEvaluation.evaluated_at.asc())
        ).scalars().all()

        # One broker position is one actual V1 trade, even if duplicate
        # observability rows ever exist.
        unique = {}
        for row in rows:
            key = row.v1_position_id or row.v1_order_id or f"evaluation:{row.id}"
            unique[str(key)] = row
        rows = list(unique.values())

        wins = losses = breakeven = closed = 0
        net_pnl = 0.0
        pnl_count = 0
        for row in rows:
            outcome = dict(row.v1_outcome_json or {})
            if not outcome.get("closed"):
                continue
            closed += 1
            result = str(outcome.get("outcome") or "").upper()
            if result == "WIN":
                wins += 1
            elif result == "LOSS":
                losses += 1
            elif result == "BREAKEVEN":
                breakeven += 1
            pnl = _number(outcome.get("realized_pnl"))
            if pnl is not None:
                net_pnl += pnl
                pnl_count += 1

        decided = wins + losses + breakeven
        return {
            "actual_trades": len(rows),
            "closed": closed,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "pending": len(rows) - closed,
            "win_rate": round(100.0 * wins / decided, 2) if decided else 0.0,
            "net_pnl": round(net_pnl, 2) if pnl_count else None,
            "outcome_source": "ctrader_closed_history",
        }
    finally:
        db.close()
