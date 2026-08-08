from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from fundamentals.config import FUNDAMENTAL_INGEST_INTERVAL_SECONDS
from fundamentals.repositories.observations import provider_health


_INGEST_LOCK = threading.Lock()
_WORKER_LOCK = threading.Lock()
_WORKER_THREAD = None
_LAST_KICK_MONOTONIC = 0.0


def run_fundamental_ingestion_if_due(
    *, now=None, interval_seconds=FUNDAMENTAL_INGEST_INTERVAL_SECONDS, fetcher=None,
    health_reader=None,
):
    """Collect provider data without depending on a browser request.

    The latest durable successful fetch makes the due decision restart-safe.
    Failures are isolated from the trading loop and recorded by news_service.
    """
    current = now or datetime.now(timezone.utc)
    read_health = health_reader or provider_health
    try:
        health = read_health(now=current)
    except Exception as exc:
        health = {"last_successful_provider_fetch": None, "health_error": str(exc)}
    last_attempt = health.get("last_provider_fetch_attempt") or health.get("last_successful_provider_fetch")
    if last_attempt is not None:
        if last_attempt.tzinfo is None:
            last_attempt = last_attempt.replace(tzinfo=timezone.utc)
        age = max(0.0, (current - last_attempt).total_seconds())
        if age < interval_seconds:
            return {"status": "NOT_DUE", "age_seconds": age}
    if not _INGEST_LOCK.acquire(blocking=False):
        return {"status": "ALREADY_RUNNING"}
    try:
        if fetcher is None:
            from services.news_service import fetch_calendar_events

            fetcher = fetch_calendar_events
        events = fetcher(force=True, timeout=8, now=current)
        return {"status": "FETCHED", "event_count": len(events or [])}
    except Exception as exc:
        print("FUNDAMENTAL_BACKGROUND_INGEST_ERROR =", str(exc))
        return {"status": "FAILED", "error": str(exc)}
    finally:
        _INGEST_LOCK.release()


def kick_fundamental_ingestion():
    """Start a non-blocking provider check so trading-cycle timing is unaffected."""
    global _LAST_KICK_MONOTONIC, _WORKER_THREAD
    with _WORKER_LOCK:
        current_monotonic = time.monotonic()
        if current_monotonic - _LAST_KICK_MONOTONIC < 60.0:
            return {"status": "NOT_DUE"}
        if _WORKER_THREAD is not None and _WORKER_THREAD.is_alive():
            return {"status": "ALREADY_RUNNING"}
        _LAST_KICK_MONOTONIC = current_monotonic
        _WORKER_THREAD = threading.Thread(
            target=run_fundamental_ingestion_if_due,
            name="flowsignal-fundamental-ingestion",
            daemon=True,
        )
        _WORKER_THREAD.start()
        return {"status": "STARTED", "thread_id": _WORKER_THREAD.ident}
