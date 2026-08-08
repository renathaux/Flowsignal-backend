"""Database-backed coordination for Fundamental Engine maintenance jobs."""

from __future__ import annotations

import threading
from contextlib import contextmanager

from sqlalchemy import text


# Fixed signed-bigint keys; changing them would break cross-process locking.
LIVE_INGESTION_LOCK_KEY = 2_026_080_701
HISTORICAL_BACKFILL_LOCK_KEY = 2_026_080_702

_LOCAL_GUARD = threading.Lock()
_LOCAL_LOCKS: dict[int, threading.Lock] = {}


@contextmanager
def advisory_lock(lock_key, *, bind=None):
    """Yield whether a non-blocking advisory lock was acquired.

    PostgreSQL uses a session-level advisory lock, which works across Render
    workers. SQLite uses a process-local lock solely for local tests.
    """
    if bind is None:
        from db import engine as bind

    if bind.dialect.name != "postgresql":
        with _LOCAL_GUARD:
            local_lock = _LOCAL_LOCKS.setdefault(int(lock_key), threading.Lock())
        acquired = local_lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                local_lock.release()
        return

    # Session advisory locks do not require a transaction. AUTOCOMMIT keeps
    # HTTP latency from holding an idle database transaction open.
    connection = bind.connect().execution_options(isolation_level="AUTOCOMMIT")
    acquired = False
    try:
        acquired = bool(connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_key)"),
            {"lock_key": int(lock_key)},
        ).scalar())
        yield acquired
    finally:
        if acquired:
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": int(lock_key)},
            )
        connection.close()
