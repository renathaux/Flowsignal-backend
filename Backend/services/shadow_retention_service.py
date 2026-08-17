"""Bound storage used by V1/V2 shadow comparison history.

Historical comparison rows are retained for 60 days. Runtime state is not
purged because it is a fixed-size per-symbol record and is required for safe
restart continuity. OPEN shadow trades are also preserved until they close;
after closure they become eligible for normal 60-day retention cleanup.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import time

from sqlalchemy import delete, func

from db import SessionLocal
from models import StrategyShadowEvaluation, StrategyShadowTrade


logger = logging.getLogger(__name__)
RETENTION_DAYS = 60
_CLEANUP_INTERVAL_SECONDS = 60 * 60
_last_cleanup_at = 0.0
_last_cleanup_result = None


def cleanup_shadow_history_safely(force=False):
    """Best-effort retention cleanup; never touches broker or V1 execution."""
    global _last_cleanup_at, _last_cleanup_result

    now_monotonic = time.monotonic()
    if (
        not force
        and _last_cleanup_result is not None
        and now_monotonic - _last_cleanup_at < _CLEANUP_INTERVAL_SECONDS
    ):
        return _last_cleanup_result

    db = None
    try:
        db = SessionLocal()
        cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

        # Evaluation rows are pure historical observations. They can be removed
        # at the retention boundary without affecting strategy or broker state.
        evaluations_deleted = db.execute(
            delete(StrategyShadowEvaluation).where(
                StrategyShadowEvaluation.evaluated_at < cutoff
            )
        ).rowcount or 0

        # Never remove an OPEN simulated trade. Once it is closed, keep it for
        # 60 days from its close time (or entry time if no close time exists).
        trades_deleted = db.execute(
            delete(StrategyShadowTrade).where(
                StrategyShadowTrade.status != "OPEN",
                func.coalesce(
                    StrategyShadowTrade.exit_timestamp,
                    StrategyShadowTrade.entry_timestamp,
                ) < cutoff,
            )
        ).rowcount or 0

        db.commit()
        result = {
            "ok": True,
            "retention_days": RETENTION_DAYS,
            "evaluations_deleted": int(evaluations_deleted),
            "closed_shadow_trades_deleted": int(trades_deleted),
            "runtime_state_retained": True,
            "open_shadow_trades_retained": True,
        }
    except Exception as exc:
        if db is not None:
            db.rollback()
        logger.warning("SHADOW_RETENTION_CLEANUP_WARNING error=%s", str(exc))
        result = {
            "ok": False,
            "retention_days": RETENTION_DAYS,
            "error": str(exc),
        }
    finally:
        if db is not None:
            db.close()

    _last_cleanup_at = now_monotonic
    _last_cleanup_result = result
    return result
