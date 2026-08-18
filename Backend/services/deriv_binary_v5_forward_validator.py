"""Research-only forward validator for the frozen V5 Binary hypothesis.

This module has no broker or production-strategy integration. It records public
Deriv market observations and paper outcomes in a dedicated SQLite database.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from paths import DATA_DIR

STRATEGY_VERSION = "DERIV_BINARY_V5_NOISY_REVERSAL_FROZEN_1"
SYMBOL = "frxEURUSD"
GRANULARITY_SECONDS = 300

# Frozen in one canonical object. Changing any value changes SPEC_SHA256 and
# invalidates the integrity test intentionally.
FROZEN_THRESHOLDS = {
    "previous_boundary_candles": 6,
    "minimum_body_ratio": 0.55,
    "maximum_range_atr10": 2.00,
    "maximum_path_efficiency": 0.35,
    "minimum_nonzero_direction_reversals": 20,
    "final_window_seconds": 60,
    "final_window_max_directional_atr": 0.0,
    "prediction": "OPPOSITE_COMPLETED_CANDLE",
}
SPEC_SHA256 = hashlib.sha256(
    json.dumps(FROZEN_THRESHOLDS, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
EXPECTED_SPEC_SHA256 = "fab52bb80f7f4dd9150adb2f90d7e090816915ff70e6b368518e7fb39444b249"

DEFAULT_DB_PATH = DATA_DIR / "deriv_v5_forward_validation.sqlite3"
PAYOUT_SCENARIOS = (0.70, 0.75, 0.80, 0.85, 0.90)
RAW_TICK_RETENTION_DAYS = 60
RAW_TICK_RETENTION_SECONDS = RAW_TICK_RETENTION_DAYS * 86400
RETENTION_WARNING_THRESHOLDS = (70.0, 80.0, 90.0)


@contextmanager
def _connect(db_path: str | Path = DEFAULT_DB_PATH):
    connection = sqlite3.connect(str(db_path), timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_database(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    collection_start_timestamp: int | None = None,
) -> dict[str, Any]:
    if SPEC_SHA256 != EXPECTED_SPEC_SHA256:
        raise RuntimeError("Frozen V5 threshold specification hash mismatch")
    with _connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ticks (
                epoch INTEGER PRIMARY KEY,
                quote REAL NOT NULL,
                bid REAL,
                ask REAL,
                spread REAL,
                received_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS observations (
                observation_id TEXT PRIMARY KEY,
                strategy_version TEXT NOT NULL,
                symbol TEXT NOT NULL,
                entry_timestamp INTEGER NOT NULL,
                qualified INTEGER NOT NULL,
                predicted_direction TEXT,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                observation_id TEXT NOT NULL UNIQUE,
                strategy_version TEXT NOT NULL,
                symbol TEXT NOT NULL,
                entry_timestamp INTEGER NOT NULL,
                settlement_timestamp INTEGER NOT NULL,
                predicted_direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                settlement_price REAL,
                settlement_quote_epoch INTEGER,
                outcome TEXT,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                settled_at REAL,
                FOREIGN KEY(observation_id) REFERENCES observations(observation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_v5_signals_status_time
            ON signals(status, settlement_timestamp);
            CREATE TABLE IF NOT EXISTS signal_evidence (
                signal_id TEXT PRIMARY KEY,
                strategy_version TEXT NOT NULL,
                spec_sha256 TEXT NOT NULL,
                decision_context_json TEXT NOT NULL,
                final_60_second_ticks_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES signals(signal_id)
            );
            CREATE TABLE IF NOT EXISTS relay_outbox (
                signal_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL,
                last_error TEXT,
                created_at REAL NOT NULL,
                delivered_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_v5_relay_due
            ON relay_outbox(status, next_attempt_at);
            """
        )
        started = int(collection_start_timestamp or time.time())
        connection.execute(
            "INSERT OR IGNORE INTO metadata(key, value) VALUES('collection_start_timestamp', ?)",
            (str(started),),
        )
        existing_version = connection.execute(
            "SELECT value FROM metadata WHERE key='strategy_version'"
        ).fetchone()
        existing_hash = connection.execute(
            "SELECT value FROM metadata WHERE key='spec_sha256'"
        ).fetchone()
        if existing_version and existing_version["value"] != STRATEGY_VERSION:
            raise RuntimeError("Forward database strategy version mismatch")
        if existing_hash and existing_hash["value"] != SPEC_SHA256:
            raise RuntimeError("Forward database frozen specification mismatch")
        connection.execute(
            "INSERT OR IGNORE INTO metadata(key, value) VALUES('strategy_version', ?)",
            (STRATEGY_VERSION,),
        )
        connection.execute(
            "INSERT OR IGNORE INTO metadata(key, value) VALUES('spec_sha256', ?)",
            (SPEC_SHA256,),
        )
        row = connection.execute(
            "SELECT value FROM metadata WHERE key='collection_start_timestamp'"
        ).fetchone()
    start = int(row["value"])
    # The first decision candle must begin after collection starts.
    first_entry = ((start // GRANULARITY_SECONDS) + 2) * GRANULARITY_SECONDS
    return {
        "strategy_version": STRATEGY_VERSION,
        "spec_sha256": SPEC_SHA256,
        "collection_start_timestamp": start,
        "first_eligible_entry_timestamp": first_entry,
    }


def record_tick(
    tick: dict[str, Any], db_path: str | Path = DEFAULT_DB_PATH
) -> bool:
    epoch = int(tick["epoch"])
    quote = float(tick["quote"])
    bid = float(tick["bid"]) if tick.get("bid") is not None else None
    ask = float(tick["ask"]) if tick.get("ask") is not None else None
    spread = ask - bid if ask is not None and bid is not None else None
    with _connect(db_path) as connection:
        cursor = connection.execute(
            """INSERT OR IGNORE INTO ticks(epoch, quote, bid, ask, spread, received_at)
               VALUES(?, ?, ?, ?, ?, ?)""",
            (epoch, quote, bid, ask, spread, time.time()),
        )
    return cursor.rowcount == 1


def _atr(candles: list[dict[str, float]], period: int = 10) -> float:
    ranges: list[float] = []
    for previous, current in zip(candles[-(period + 1):-1], candles[-period:]):
        ranges.append(
            max(
                current["high"] - current["low"],
                abs(current["high"] - previous["close"]),
                abs(current["low"] - previous["close"]),
            )
        )
    return sum(ranges) / len(ranges) if ranges else 0.0


def evaluate_completed_candle(
    candles: Iterable[dict[str, Any]],
    ticks: Iterable[dict[str, Any]],
    entry_timestamp: int,
) -> dict[str, Any]:
    """Evaluate the frozen rule without recording or executing anything.

    Entry is the final available quote strictly before ``entry_timestamp``.
    Settlement is handled separately at exactly entry_timestamp + 300 seconds.
    """
    normalized = [
        {
            "epoch": int(float(c["epoch"])),
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
        }
        for c in candles
    ]
    normalized.sort(key=lambda item: item["epoch"])
    decision_epoch = int(entry_timestamp) - GRANULARITY_SECONDS
    eligible = [c for c in normalized if c["epoch"] <= decision_epoch]
    if len(eligible) < 11 or eligible[-1]["epoch"] != decision_epoch:
        raise ValueError("Eleven aligned closed candles are required")
    decision = eligible[-1]
    previous_six = eligible[-7:-1]
    candle_ticks = sorted(
        (
            {"epoch": int(t["epoch"]), "quote": float(t["quote"])}
            for t in ticks
            if decision_epoch <= int(t["epoch"]) < int(entry_timestamp)
        ),
        key=lambda item: item["epoch"],
    )
    if len(candle_ticks) < 2:
        raise ValueError("Decision candle tick path is unavailable")
    if int(entry_timestamp) - candle_ticks[-1]["epoch"] > 2:
        raise ValueError("Entry quote is more than two seconds before the boundary")

    open_price = decision["open"]
    close_price = decision["close"]
    body = close_price - open_price
    candle_direction = "BULLISH" if body > 0 else "BEARISH" if body < 0 else "DOJI"
    direction_sign = 1 if body > 0 else -1 if body < 0 else 0
    candle_range = max(decision["high"] - decision["low"], 1e-12)
    body_ratio = abs(body) / candle_range
    atr10 = _atr(eligible, 10)
    atr_safe = max(atr10, 1e-12)
    range_atr = candle_range / atr_safe
    quotes = [tick["quote"] for tick in candle_ticks]
    changes = [current - previous for previous, current in zip(quotes[:-1], quotes[1:])]
    nonzero_signs = [1 if change > 0 else -1 for change in changes if change != 0]
    reversals = sum(
        current != previous
        for previous, current in zip(nonzero_signs[:-1], nonzero_signs[1:])
    )
    path_length = sum(abs(change) for change in changes)
    path_efficiency = abs(body) / path_length if path_length > 0 else 0.0
    final_start = int(entry_timestamp) - FROZEN_THRESHOLDS["final_window_seconds"]
    final_ticks = [tick for tick in candle_ticks if tick["epoch"] >= final_start]
    if len(final_ticks) < 2:
        raise ValueError("Final 60-second tick path is unavailable")
    final_move = final_ticks[-1]["quote"] - final_ticks[0]["quote"]
    final_move_atr = final_move / atr_safe
    prior_high = max(c["high"] for c in previous_six)
    prior_low = min(c["low"] for c in previous_six)
    directional_breakout = (
        close_price > prior_high
        if direction_sign > 0
        else close_price < prior_low if direction_sign < 0 else False
    )
    final_stalled_or_opposed = direction_sign * final_move_atr <= 0.0

    qualified = bool(
        direction_sign
        and directional_breakout
        and body_ratio >= FROZEN_THRESHOLDS["minimum_body_ratio"]
        and range_atr <= FROZEN_THRESHOLDS["maximum_range_atr10"]
        and path_efficiency <= FROZEN_THRESHOLDS["maximum_path_efficiency"]
        and reversals >= FROZEN_THRESHOLDS["minimum_nonzero_direction_reversals"]
        and final_stalled_or_opposed
    )
    predicted_direction = (
        "FALL" if qualified and direction_sign > 0
        else "RISE" if qualified and direction_sign < 0
        else None
    )
    observation_id = f"{STRATEGY_VERSION}:{SYMBOL}:{int(entry_timestamp)}"
    signal_id = (
        f"{STRATEGY_VERSION}:{SYMBOL}:{int(entry_timestamp)}:{predicted_direction}"
        if predicted_direction else None
    )
    return {
        "observation_id": observation_id,
        "signal_id": signal_id,
        "strategy_version": STRATEGY_VERSION,
        "spec_sha256": SPEC_SHA256,
        "symbol": SYMBOL,
        "decision_candle_epoch": decision_epoch,
        "entry_timestamp": int(entry_timestamp),
        "settlement_timestamp": int(entry_timestamp) + GRANULARITY_SECONDS,
        "entry_price": candle_ticks[-1]["quote"],
        "entry_quote_epoch": candle_ticks[-1]["epoch"],
        "candle": decision,
        "atr10": atr10,
        "body_ratio": body_ratio,
        "range_atr": range_atr,
        "total_absolute_tick_path": path_length,
        "path_efficiency": path_efficiency,
        "nonzero_direction_reversals": reversals,
        "final_60_second_net_move": final_move,
        "final_60_second_move_atr": final_move_atr,
        "candle_direction": candle_direction,
        "previous_six_high": prior_high,
        "previous_six_low": prior_low,
        "directional_breakout": directional_breakout,
        "net_displacement": body,
        "qualified": qualified,
        "predicted_direction": predicted_direction,
        "_signal_evidence": {
            "decision_candle": decision,
            "previous_six_candles": previous_six,
            "atr10": atr10,
            "body_ratio": body_ratio,
            "range_atr": range_atr,
            "net_displacement": body,
            "total_absolute_tick_path": path_length,
            "path_efficiency": path_efficiency,
            "nonzero_direction_reversals": reversals,
            "final_60_second_net_move": final_move,
            "final_60_second_move_atr": final_move_atr,
            "candle_direction": candle_direction,
            "predicted_direction": predicted_direction,
            "previous_six_high": prior_high,
            "previous_six_low": prior_low,
            "directional_breakout": directional_breakout,
            "final_60_second_ticks": final_ticks,
        },
    }


def record_observation(
    observation: dict[str, Any], db_path: str | Path = DEFAULT_DB_PATH
) -> dict[str, bool]:
    durable_observation = {
        key: value for key, value in observation.items() if key != "_signal_evidence"
    }
    payload = json.dumps(durable_observation, sort_keys=True, separators=(",", ":"))
    now = time.time()
    with _connect(db_path) as connection:
        cursor = connection.execute(
            """INSERT OR IGNORE INTO observations(
                   observation_id, strategy_version, symbol, entry_timestamp,
                   qualified, predicted_direction, payload_json, created_at
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                observation["observation_id"], STRATEGY_VERSION, SYMBOL,
                int(observation["entry_timestamp"]), int(observation["qualified"]),
                observation.get("predicted_direction"), payload, now,
            ),
        )
        observation_created = cursor.rowcount == 1
        signal_created = False
        if observation.get("qualified"):
            signal_cursor = connection.execute(
                """INSERT OR IGNORE INTO signals(
                       signal_id, observation_id, strategy_version, symbol,
                       entry_timestamp, settlement_timestamp, predicted_direction,
                       entry_price, status, payload_json, created_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)""",
                (
                    observation["signal_id"], observation["observation_id"],
                    STRATEGY_VERSION, SYMBOL, int(observation["entry_timestamp"]),
                    int(observation["settlement_timestamp"]),
                    observation["predicted_direction"], float(observation["entry_price"]),
                    payload, now,
                ),
            )
            signal_created = signal_cursor.rowcount == 1
            evidence = observation.get("_signal_evidence")
            if not isinstance(evidence, dict):
                raise RuntimeError("Qualifying V5 signal is missing audit evidence")
            final_ticks = evidence.get("final_60_second_ticks")
            if not isinstance(final_ticks, list) or len(final_ticks) < 2:
                raise RuntimeError("Qualifying V5 signal has incomplete final-window evidence")
            decision_context = {
                key: value for key, value in evidence.items()
                if key != "final_60_second_ticks"
            }
            connection.execute(
                """INSERT OR IGNORE INTO signal_evidence(
                       signal_id, strategy_version, spec_sha256,
                       decision_context_json, final_60_second_ticks_json, created_at
                   ) VALUES(?, ?, ?, ?, ?, ?)""",
                (
                    observation["signal_id"], STRATEGY_VERSION, SPEC_SHA256,
                    json.dumps(decision_context, sort_keys=True, separators=(",", ":")),
                    json.dumps(final_ticks, sort_keys=True, separators=(",", ":")),
                    now,
                ),
            )
            relay_payload = {
                "strategy_version": STRATEGY_VERSION,
                "rule_hash": SPEC_SHA256,
                "signal_id": observation["signal_id"],
                "direction": observation["predicted_direction"],
                "symbol": SYMBOL,
                "decision_timestamp": int(observation["decision_candle_epoch"]),
                "entry_timestamp": int(observation["entry_timestamp"]),
                "entry_quote": float(observation["entry_price"]),
                "entry_quote_epoch": int(observation["entry_quote_epoch"]),
                "settlement_target_timestamp": int(observation["settlement_timestamp"]),
            }
            connection.execute(
                """INSERT OR IGNORE INTO relay_outbox(
                       signal_id, payload_json, status, attempts,
                       next_attempt_at, created_at
                   ) VALUES(?, ?, 'PENDING', 0, ?, ?)""",
                (
                    observation["signal_id"],
                    json.dumps(relay_payload, sort_keys=True, separators=(",", ":")),
                    now,
                    now,
                ),
            )
    return {"observation_created": observation_created, "signal_created": signal_created}


def settle_from_recorded_ticks(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    now_epoch: int | None = None,
) -> int:
    """Settle with the first recorded quote at/after each exact expiry."""
    ceiling = int(now_epoch or time.time())
    settled = 0
    with _connect(db_path) as connection:
        pending = connection.execute(
            """SELECT * FROM signals
               WHERE status='PENDING' AND settlement_timestamp <= ?
               ORDER BY settlement_timestamp""",
            (ceiling,),
        ).fetchall()
        for signal in pending:
            tick = connection.execute(
                """SELECT epoch, quote FROM ticks
                   WHERE epoch >= ? ORDER BY epoch ASC LIMIT 1""",
                (signal["settlement_timestamp"],),
            ).fetchone()
            if tick is None:
                continue
            entry = float(signal["entry_price"])
            settlement = float(tick["quote"])
            direction = signal["predicted_direction"]
            if settlement == entry:
                outcome = "TIE"
            elif (direction == "RISE" and settlement > entry) or (
                direction == "FALL" and settlement < entry
            ):
                outcome = "WIN"
            else:
                outcome = "LOSS"
            connection.execute(
                """UPDATE signals SET settlement_price=?, settlement_quote_epoch=?,
                       outcome=?, status='SETTLED', settled_at=?
                   WHERE signal_id=? AND status='PENDING'""",
                (settlement, int(tick["epoch"]), outcome, time.time(), signal["signal_id"]),
            )
            settled += 1
    return settled


def cleanup_raw_ticks(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    now_epoch: int | None = None,
) -> dict[str, Any]:
    """Prune only expired, unprotected raw ticks in one restart-safe transaction."""
    ceiling = int(now_epoch or time.time())
    cutoff = ceiling - RAW_TICK_RETENTION_SECONDS
    initialize_database(db_path)
    with _connect(db_path) as connection:
        before = connection.total_changes
        connection.execute(
            """DELETE FROM ticks
               WHERE epoch < ?
                 AND NOT EXISTS (
                     SELECT 1 FROM signals AS pending
                     WHERE pending.status='PENDING'
                       AND ticks.epoch >= pending.entry_timestamp - ?
                       AND ticks.epoch <= pending.settlement_timestamp + 5
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM signals AS incomplete
                     LEFT JOIN signal_evidence AS evidence
                       ON evidence.signal_id=incomplete.signal_id
                     WHERE evidence.signal_id IS NULL
                       AND ticks.epoch >= incomplete.entry_timestamp - ?
                       AND ticks.epoch <= incomplete.settlement_timestamp + 5
                 )""",
            (cutoff, GRANULARITY_SECONDS, GRANULARITY_SECONDS),
        )
        deleted = connection.total_changes - before
        previous = connection.execute(
            "SELECT value FROM metadata WHERE key='raw_ticks_deleted_total'"
        ).fetchone()
        total = int(previous["value"]) if previous else 0
        values = {
            "last_retention_cleanup_timestamp": str(ceiling),
            "raw_ticks_deleted_last_cleanup": str(deleted),
            "raw_ticks_deleted_total": str(total + deleted),
        }
        connection.executemany(
            """INSERT INTO metadata(key, value) VALUES(?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            values.items(),
        )
    # PASSIVE checkpoint avoids waiting for readers and makes freed pages reusable.
    with _connect(db_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
    return {"cutoff_timestamp": cutoff, "deleted": deleted, "total_deleted": total + deleted}


def _disk_status(db_path: str | Path) -> dict[str, Any]:
    path = Path(db_path)
    usage = shutil.disk_usage(path.parent)
    percent = 100.0 * usage.used / usage.total if usage.total else 0.0
    warning = (
        "CRITICAL" if percent >= RETENTION_WARNING_THRESHOLDS[2]
        else "ELEVATED" if percent >= RETENTION_WARNING_THRESHOLDS[1]
        else "WARNING" if percent >= RETENTION_WARNING_THRESHOLDS[0]
        else "OK"
    )
    return {
        "database_size_bytes": path.stat().st_size if path.exists() else 0,
        "disk_total_bytes": usage.total,
        "disk_used_bytes": usage.used,
        "disk_free_bytes": usage.free,
        "disk_used_percent": round(percent, 2),
        "warning_state": warning,
        "warning_thresholds_percent": {"warning": 70, "elevated": 80, "critical": 90},
    }


def _wilson_interval(wins: int, losses: int, z: float = 1.96) -> list[float] | None:
    total = wins + losses
    if not total:
        return None
    probability = wins / total
    denominator = 1 + z * z / total
    center = (probability + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(
        probability * (1 - probability) / total + z * z / (4 * total * total)
    ) / denominator
    return [round(100 * (center - margin), 2), round(100 * (center + margin), 2)]


def _summary(rows: list[sqlite3.Row]) -> dict[str, Any]:
    wins = sum(row["outcome"] == "WIN" for row in rows)
    losses = sum(row["outcome"] == "LOSS" for row in rows)
    ties = sum(row["outcome"] == "TIE" for row in rows)
    decisive = wins + losses
    win_rate = 100 * wins / decisive if decisive else None
    streak = maximum = 0
    for row in rows:
        if row["outcome"] == "LOSS":
            streak += 1
            maximum = max(maximum, streak)
        elif row["outcome"] == "WIN":
            streak = 0
    return {
        "signals": len(rows), "wins": wins, "losses": losses, "ties": ties,
        "decisive": decisive,
        "win_rate": round(win_rate, 2) if win_rate is not None else None,
        "confidence_interval_95": _wilson_interval(wins, losses),
        "difference_from_50": round(win_rate - 50, 2) if win_rate is not None else None,
        "difference_from_80pct_break_even": (
            round(win_rate - 55.56, 2) if win_rate is not None else None
        ),
        "current_losing_streak": streak, "maximum_losing_streak": maximum,
    }


def forward_report(db_path: str | Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    state = initialize_database(db_path)
    with _connect(db_path) as connection:
        signals = connection.execute(
            "SELECT * FROM signals ORDER BY entry_timestamp, signal_id"
        ).fetchall()
        tick_state = connection.execute(
            "SELECT COUNT(*) AS count, MIN(epoch) AS oldest, MAX(epoch) AS newest FROM ticks"
        ).fetchone()
        metadata = dict(connection.execute("SELECT key, value FROM metadata").fetchall())
    settled = [row for row in signals if row["status"] == "SETTLED"]
    decisive = [row for row in settled if row["outcome"] in {"WIN", "LOSS"}]
    elapsed_days = max((time.time() - state["collection_start_timestamp"]) / 86400, 1 / 86400)
    all_summary = _summary(settled)
    all_summary.update({
        "pending": len(signals) - len(settled),
        "rise": sum(row["predicted_direction"] == "RISE" for row in signals),
        "fall": sum(row["predicted_direction"] == "FALL" for row in signals),
        "signals_per_day": round(len(signals) / elapsed_days, 3),
    })
    economics = {}
    if all_summary["win_rate"] is not None:
        probability = all_summary["win_rate"] / 100
        for payout in PAYOUT_SCENARIOS:
            economics[f"{int(payout * 100)}pct"] = {
                "break_even_win_rate": round(100 / (1 + payout), 2),
                "expectancy_per_1_stake": round(probability * payout - (1 - probability), 4),
            }
    rolling = {}
    for size in (25, 50, 100):
        chosen = decisive[-size:]
        rolling[f"last_{size}"] = _summary(chosen) if len(chosen) == size else None
    milestones = {}
    for size in (100, 200, 300, 500):
        chosen = decisive[:size]
        milestones[str(size)] = _summary(chosen) if len(chosen) >= size else None
    return {
        **state,
        "thresholds": dict(FROZEN_THRESHOLDS),
        "total_qualifying_signals": len(signals),
        "settled_signals": len(settled),
        "all_forward": all_summary,
        "rolling": rolling,
        "milestones": milestones,
        "economics": economics,
        "historical_results_combined": False,
        "retention": {
            "period_days": RAW_TICK_RETENTION_DAYS,
            "oldest_raw_tick_timestamp": tick_state["oldest"],
            "newest_raw_tick_timestamp": tick_state["newest"],
            "raw_tick_count": tick_state["count"],
            "last_cleanup_timestamp": int(metadata["last_retention_cleanup_timestamp"])
                if metadata.get("last_retention_cleanup_timestamp") else None,
            "raw_ticks_deleted_last_cleanup": int(
                metadata.get("raw_ticks_deleted_last_cleanup", "0")
            ),
            "raw_ticks_deleted_total": int(metadata.get("raw_ticks_deleted_total", "0")),
            **_disk_status(db_path),
        },
    }
