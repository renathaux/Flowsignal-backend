"""Standalone public-data collector for the frozen V5 forward validation."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

import websockets

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.deriv_binary_v5_forward_validator import (  # noqa: E402
    DEFAULT_DB_PATH,
    GRANULARITY_SECONDS,
    SYMBOL,
    cleanup_raw_ticks,
    evaluate_completed_candle,
    forward_report,
    initialize_database,
    record_observation,
    record_tick,
    settle_from_recorded_ticks,
)
from services.deriv_v5_signal_relay import deliver_pending_relays  # noqa: E402

PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


async def _one_request(payload: dict[str, Any]) -> dict[str, Any]:
    async with websockets.connect(PUBLIC_WS_URL, open_timeout=15, close_timeout=5) as ws:
        await ws.send(json.dumps(payload))
        deadline = time.monotonic() + 20
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Timed out waiting for Deriv public history")
            message = json.loads(await asyncio.wait_for(ws.recv(), remaining))
            if message.get("error") or message.get("errors"):
                raise RuntimeError(str(message.get("error") or message.get("errors")))
            if message.get("req_id") == payload.get("req_id"):
                return message


async def _fetch_candles(entry_timestamp: int) -> list[dict[str, Any]]:
    response = await _one_request({
        "ticks_history": SYMBOL,
        "end": str(entry_timestamp - 1),
        "count": 20,
        "style": "candles",
        "granularity": GRANULARITY_SECONDS,
        "req_id": 9501,
    })
    return list(response.get("candles") or [])


async def _fetch_ticks(start: int, end: int) -> list[dict[str, Any]]:
    response = await _one_request({
        "ticks_history": SYMBOL,
        "start": int(start),
        "end": int(end),
        "style": "ticks",
        "req_id": 9502,
    })
    history = response.get("history") or {}
    return [
        {"epoch": int(epoch), "quote": float(quote)}
        for epoch, quote in zip(history.get("times") or [], history.get("prices") or [])
    ]


async def process_boundary(entry_timestamp: int, db_path: Path) -> dict[str, Any]:
    candles, ticks = await asyncio.gather(
        _fetch_candles(entry_timestamp),
        _fetch_ticks(entry_timestamp - GRANULARITY_SECONDS, entry_timestamp - 1),
    )
    for tick in ticks:
        record_tick(tick, db_path)
    observation = evaluate_completed_candle(candles, ticks, entry_timestamp)
    persisted = record_observation(observation, db_path)
    return {**persisted, "entry_timestamp": entry_timestamp, "qualified": observation["qualified"]}


async def _backfill_due_settlements(db_path: Path) -> None:
    # Pending settlement timestamps are discovered through the report database,
    # while public history supplies the exact first quote at/after each target.
    import sqlite3
    connection = sqlite3.connect(str(db_path))
    targets = [row[0] for row in connection.execute(
        "SELECT DISTINCT settlement_timestamp FROM signals WHERE status='PENDING' AND settlement_timestamp <= ?",
        (int(time.time()),),
    )]
    connection.close()
    for target in targets:
        ticks = await _fetch_ticks(int(target), int(target) + 5)
        for tick in ticks:
            record_tick(tick, db_path)
    settle_from_recorded_ticks(db_path)


async def catch_up(db_path: Path) -> None:
    state = initialize_database(db_path)
    import sqlite3
    connection = sqlite3.connect(str(db_path))
    row = connection.execute("SELECT MAX(entry_timestamp) FROM observations").fetchone()
    connection.close()
    first = int(state["first_eligible_entry_timestamp"])
    next_boundary = int(row[0]) + GRANULARITY_SECONDS if row and row[0] else first
    current_boundary = int(time.time()) // GRANULARITY_SECONDS * GRANULARITY_SECONDS
    while next_boundary <= current_boundary:
        result = await process_boundary(next_boundary, db_path)
        print(json.dumps({"event": "V5_BOUNDARY_RECORDED", **result}), flush=True)
        next_boundary += GRANULARITY_SECONDS
    await _backfill_due_settlements(db_path)


async def run_forever(db_path: Path) -> None:
    await catch_up(db_path)
    await asyncio.to_thread(deliver_pending_relays, db_path)
    cleanup_raw_ticks(db_path)
    next_cleanup = time.time() + 86400
    next_relay_retry = time.time() + 10
    next_boundary = (int(time.time()) // GRANULARITY_SECONDS + 1) * GRANULARITY_SECONDS
    async with websockets.connect(PUBLIC_WS_URL, open_timeout=15, close_timeout=5) as ws:
        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1, "req_id": 9601}))
        while True:
            message = json.loads(await ws.recv())
            tick = message.get("tick")
            if not isinstance(tick, dict):
                continue
            record_tick(tick, db_path)
            settle_from_recorded_ticks(db_path, now_epoch=int(tick["epoch"]))
            if time.time() >= next_relay_retry:
                try:
                    await asyncio.to_thread(deliver_pending_relays, db_path)
                except Exception as exc:
                    print(json.dumps({"event": "V5_RELAY_ERROR", "error": str(exc)[:300]}), flush=True)
                next_relay_retry = time.time() + 10
            if time.time() >= next_cleanup:
                result = cleanup_raw_ticks(db_path)
                print(json.dumps({"event": "V5_RETENTION_CLEANUP", **result}), flush=True)
                next_cleanup = time.time() + 86400
            while int(tick["epoch"]) >= next_boundary:
                result = await process_boundary(next_boundary, db_path)
                print(json.dumps({"event": "V5_BOUNDARY_RECORDED", **result}), flush=True)
                next_boundary += GRANULARITY_SECONDS


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--once", action="store_true", help="Catch up and exit")
    args = parser.parse_args()
    if args.report:
        print(json.dumps(forward_report(args.db), indent=2))
        return
    if args.once:
        asyncio.run(catch_up(args.db))
        print(json.dumps(forward_report(args.db), indent=2))
        return
    asyncio.run(run_forever(args.db))


if __name__ == "__main__":
    main()
