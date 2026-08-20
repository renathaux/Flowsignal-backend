"""One-way authenticated delivery of persisted V5 signals.

The research database is authoritative. Delivery failures only update the
outbox and never modify observations, signals, paper outcomes, or retention.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from services.deriv_binary_simple_strategy import DEFAULT_DB_PATH

RELAY_URL = os.getenv("BINARY_V5_RELAY_URL", "").strip()
RELAY_SECRET = os.getenv("BINARY_V5_RELAY_SECRET", "").strip()
MAX_ATTEMPTS_PER_DRAIN = 10


def sign_relay_payload(body: bytes, timestamp: int, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), str(timestamp).encode("ascii") + b"." + body,
        hashlib.sha256,
    ).hexdigest()


def _post(url: str, body: bytes, headers: dict[str, str]) -> int:
    request = Request(url, data=body, headers=headers, method="POST")
    with urlopen(request, timeout=12) as response:  # noqa: S310 - configured HTTPS URL
        return int(response.status)


def deliver_pending_relays(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    now: float | None = None,
    post: Callable[[str, bytes, dict[str, str]], int] = _post,
    relay_url: str | None = None,
    secret: str | None = None,
) -> dict[str, int]:
    """Deliver due outbox rows; safe to retry after any process restart."""
    clock = float(now if now is not None else time.time())
    target = RELAY_URL if relay_url is None else relay_url
    key = RELAY_SECRET if secret is None else secret
    if not target or not key:
        return {"delivered": 0, "failed": 0, "disabled": 1}
    connection = sqlite3.connect(str(db_path), timeout=15)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT signal_id, payload_json, attempts FROM relay_outbox
               WHERE status='PENDING' AND next_attempt_at <= ?
               ORDER BY created_at LIMIT ?""",
            (clock, MAX_ATTEMPTS_PER_DRAIN),
        ).fetchall()
    finally:
        connection.close()
    delivered = failed = 0
    for row in rows:
        body = row["payload_json"].encode("utf-8")
        timestamp = int(clock)
        headers = {
            "Content-Type": "application/json",
            "X-FlowSignal-Relay-Timestamp": str(timestamp),
            "X-FlowSignal-Relay-Signature": sign_relay_payload(body, timestamp, key),
        }
        try:
            status = post(target, body, headers)
            if status < 200 or status >= 300:
                raise RuntimeError(f"relay HTTP {status}")
        except Exception as exc:
            failed += 1
            attempts = int(row["attempts"]) + 1
            delay = min(3600, 2 ** min(attempts, 11))
            with sqlite3.connect(str(db_path), timeout=15) as update:
                update.execute(
                    """UPDATE relay_outbox SET attempts=?, next_attempt_at=?, last_error=?
                       WHERE signal_id=? AND status='PENDING'""",
                    (attempts, clock + delay, str(exc)[:500], row["signal_id"]),
                )
            continue
        with sqlite3.connect(str(db_path), timeout=15) as update:
            update.execute(
                """UPDATE relay_outbox SET status='DELIVERED', attempts=attempts+1,
                       delivered_at=?, last_error=NULL WHERE signal_id=? AND status='PENDING'""",
                (clock, row["signal_id"]),
            )
        delivered += 1
    return {"delivered": delivered, "failed": failed, "disabled": 0}
