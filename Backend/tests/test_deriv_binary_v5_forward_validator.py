import ast
import json
import math
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.deriv_binary_v5_forward_validator import (
    EXPECTED_SPEC_SHA256,
    FROZEN_THRESHOLDS,
    SPEC_SHA256,
    STRATEGY_VERSION,
    RAW_TICK_RETENTION_DAYS,
    cleanup_raw_ticks,
    evaluate_completed_candle,
    forward_report,
    initialize_database,
    record_observation,
    record_tick,
    settle_from_recorded_ticks,
)


def candles(bullish=True):
    base = 1.1000
    rows = []
    for index in range(10):
        price = base + index * 0.00001
        rows.append({"epoch": 1000 + index * 300, "open": price, "high": price + 0.0002,
                     "low": price - 0.0002, "close": price + 0.00001})
    epoch = 4000
    if bullish:
        rows.append({"epoch": epoch, "open": 1.1000, "high": 1.1007, "low": 1.0999, "close": 1.1006})
    else:
        rows.append({"epoch": epoch, "open": 1.1003, "high": 1.1004, "low": 1.0996, "close": 1.0997})
    return rows


def noisy_ticks(bullish=True):
    start = 4000
    initial = 1.1000 if bullish else 1.1003
    finish = 1.1006 if bullish else 1.0997
    values = [initial]
    # Alternation creates >=20 reversals and a long path, while trending to the
    # candle close before the frozen final-minute stall/opposition.
    for index in range(239):
        progress = (index + 1) / 239
        center = initial + (finish - initial) * progress
        offset = (0.00008 if index % 2 == 0 else -0.00008) * (1 if bullish else -1)
        values.append(center + offset)
    final_start = values[-1]
    for index in range(60):
        # Oppose the candle direction into the exact entry quote.
        progress = (index + 1) / 60
        value = final_start + ((finish - final_start) * progress)
        values.append(value)
    values[-1] = finish
    return [{"epoch": start + index, "quote": value} for index, value in enumerate(values)]


class FrozenV5Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "forward.sqlite3"
        initialize_database(self.db, collection_start_timestamp=3900)

    def tearDown(self):
        self.temp.cleanup()

    def test_frozen_specification_integrity(self):
        self.assertEqual(STRATEGY_VERSION, "DERIV_BINARY_V5_NOISY_REVERSAL_FROZEN_1")
        self.assertEqual(SPEC_SHA256, EXPECTED_SPEC_SHA256)
        self.assertEqual(FROZEN_THRESHOLDS["maximum_path_efficiency"], 0.35)
        self.assertEqual(FROZEN_THRESHOLDS["minimum_nonzero_direction_reversals"], 20)

    def test_bullish_qualification_predicts_fall(self):
        result = evaluate_completed_candle(candles(True), noisy_ticks(True), 4300)
        self.assertTrue(result["qualified"], result)
        self.assertEqual(result["predicted_direction"], "FALL")

    def test_bearish_qualification_predicts_rise(self):
        result = evaluate_completed_candle(candles(False), noisy_ticks(False), 4300)
        self.assertTrue(result["qualified"], result)
        self.assertEqual(result["predicted_direction"], "RISE")

    def test_persistent_deduplication_and_exact_settlement(self):
        result = evaluate_completed_candle(candles(True), noisy_ticks(True), 4300)
        first = record_observation(result, self.db)
        second = record_observation(result, self.db)
        self.assertEqual(first, {"observation_created": True, "signal_created": True})
        self.assertEqual(second, {"observation_created": False, "signal_created": False})
        record_tick({"epoch": 4599, "quote": result["entry_price"] - 0.001}, self.db)
        record_tick({"epoch": 4600, "quote": result["entry_price"] - 0.0001}, self.db)
        record_tick({"epoch": 4601, "quote": result["entry_price"] + 0.001}, self.db)
        self.assertEqual(settle_from_recorded_ticks(self.db, now_epoch=4600), 1)
        report = forward_report(self.db)
        self.assertEqual(report["total_qualifying_signals"], 1)
        self.assertEqual(report["all_forward"]["wins"], 1)

    def test_report_keeps_historical_results_separate(self):
        report = forward_report(self.db)
        self.assertFalse(report["historical_results_combined"])
        self.assertIn("milestones", report)
        self.assertIn("economics", report)

    def test_live_bid_ask_and_spread_are_observationally_persisted(self):
        record_tick({"epoch": 5000, "quote": 1.1001, "bid": 1.1000, "ask": 1.1002}, self.db)
        connection = sqlite3.connect(str(self.db))
        try:
            row = connection.execute(
                "SELECT quote, bid, ask, spread FROM ticks WHERE epoch=5000"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row[:3], (1.1001, 1.1000, 1.1002))
        self.assertTrue(math.isclose(row[3], 0.0002, abs_tol=1e-12))

    def test_collector_has_no_broker_execution_surface(self):
        paths = [
            BACKEND_DIR / "services" / "deriv_binary_v5_forward_validator.py",
            BACKEND_DIR / "scripts" / "run_deriv_binary_v5_forward_validator.py",
        ]
        forbidden_imports = {"ctrader_connector", "deriv_demo_execution_service"}
        forbidden_calls = {"buy", "proposal", "place_market_order", "execute_demo_signal"}
        for path in paths:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            imported = {
                alias.name for node in ast.walk(tree)
                if isinstance(node, ast.Import) for alias in node.names
            } | {
                node.module for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module
            }
            names = {
                node.func.id for node in ast.walk(tree)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            }
            self.assertTrue(imported.isdisjoint(forbidden_imports))
            self.assertTrue(names.isdisjoint(forbidden_calls))
            self.assertNotIn('"buy"', source)
            self.assertNotIn('"proposal"', source)

    def test_retention_prunes_only_expired_ordinary_ticks(self):
        now = 20_000_000
        cutoff = now - RAW_TICK_RETENTION_DAYS * 86400
        record_tick({"epoch": cutoff - 1, "quote": 1.0}, self.db)
        record_tick({"epoch": cutoff, "quote": 1.1}, self.db)
        record_tick({"epoch": cutoff + 1, "quote": 1.2}, self.db)
        result = cleanup_raw_ticks(self.db, now_epoch=now)
        self.assertEqual(result["deleted"], 1)
        with sqlite3.connect(str(self.db)) as connection:
            epochs = [row[0] for row in connection.execute("SELECT epoch FROM ticks")]
        self.assertEqual(epochs, [cutoff, cutoff + 1])

    def test_qualifying_evidence_and_completed_features_are_permanent(self):
        result = evaluate_completed_candle(candles(True), noisy_ticks(True), 4300)
        record_observation(result, self.db)
        now = 4300 + (RAW_TICK_RETENTION_DAYS + 1) * 86400
        for tick in noisy_ticks(True):
            record_tick(tick, self.db)
        cleanup_raw_ticks(self.db, now_epoch=now)
        with sqlite3.connect(str(self.db)) as connection:
            observation = connection.execute(
                "SELECT payload_json FROM observations WHERE observation_id=?",
                (result["observation_id"],),
            ).fetchone()
            signal = connection.execute(
                "SELECT entry_timestamp, settlement_timestamp, entry_price, status "
                "FROM signals WHERE signal_id=?", (result["signal_id"],)
            ).fetchone()
            evidence = connection.execute(
                "SELECT strategy_version, spec_sha256, decision_context_json, "
                "final_60_second_ticks_json FROM signal_evidence WHERE signal_id=?",
                (result["signal_id"],),
            ).fetchone()
        self.assertIsNotNone(observation)
        self.assertEqual(signal, (4300, 4600, result["entry_price"], "PENDING"))
        self.assertEqual(evidence[0], STRATEGY_VERSION)
        self.assertEqual(evidence[1], EXPECTED_SPEC_SHA256)
        context = json.loads(evidence[2])
        self.assertEqual(len(context["previous_six_candles"]), 6)
        self.assertIn("decision_candle", context)
        self.assertGreaterEqual(len(json.loads(evidence[3])), 2)

    def test_pending_signal_ticks_are_protected_and_settlement_survives(self):
        result = evaluate_completed_candle(candles(True), noisy_ticks(True), 4300)
        record_observation(result, self.db)
        record_tick({"epoch": 4000, "quote": result["entry_price"]}, self.db)
        record_tick({"epoch": 4600, "quote": result["entry_price"] - 0.0001}, self.db)
        cleanup_raw_ticks(
            self.db, now_epoch=4600 + (RAW_TICK_RETENTION_DAYS + 1) * 86400
        )
        with sqlite3.connect(str(self.db)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM ticks").fetchone()[0], 2)
        self.assertEqual(settle_from_recorded_ticks(self.db, now_epoch=4600), 1)
        cleanup_raw_ticks(
            self.db, now_epoch=4600 + (RAW_TICK_RETENTION_DAYS + 1) * 86400
        )
        with sqlite3.connect(str(self.db)) as connection:
            row = connection.execute(
                "SELECT entry_timestamp, settlement_timestamp, entry_price, "
                "settlement_price, settlement_quote_epoch, outcome, status FROM signals"
            ).fetchone()
        self.assertEqual(row[:3], (4300, 4600, result["entry_price"]))
        self.assertEqual(row[4:], (4600, "WIN", "SETTLED"))

    def test_cleanup_is_idempotent_restart_safe_and_keeps_deduplication(self):
        now = 20_000_000
        old = now - RAW_TICK_RETENTION_DAYS * 86400 - 1
        record_tick({"epoch": old, "quote": 1.0}, self.db)
        self.assertEqual(cleanup_raw_ticks(self.db, now_epoch=now)["deleted"], 1)
        self.assertEqual(cleanup_raw_ticks(self.db, now_epoch=now)["deleted"], 0)
        self.assertTrue(record_tick({"epoch": old, "quote": 1.0}, self.db))
        self.assertFalse(record_tick({"epoch": old, "quote": 1.0}, self.db))
        initialize_database(self.db, collection_start_timestamp=999999)
        report = forward_report(self.db)
        self.assertEqual(report["collection_start_timestamp"], 3900)
        self.assertEqual(report["retention"]["period_days"], 60)
        self.assertIn(report["retention"]["warning_state"], {
            "OK", "WARNING", "ELEVATED", "CRITICAL"
        })

    def test_cleanup_does_not_change_v5_decision_or_frozen_identity(self):
        before = evaluate_completed_candle(candles(False), noisy_ticks(False), 4300)
        cleanup_raw_ticks(self.db, now_epoch=20_000_000)
        after = evaluate_completed_candle(candles(False), noisy_ticks(False), 4300)
        self.assertEqual(before, after)
        self.assertEqual(STRATEGY_VERSION, "DERIV_BINARY_V5_NOISY_REVERSAL_FROZEN_1")
        self.assertEqual(SPEC_SHA256, EXPECTED_SPEC_SHA256)

    def test_report_exposes_retention_and_disk_monitoring(self):
        report = forward_report(self.db)["retention"]
        required = {
            "period_days", "oldest_raw_tick_timestamp", "newest_raw_tick_timestamp",
            "raw_tick_count", "last_cleanup_timestamp",
            "raw_ticks_deleted_last_cleanup", "raw_ticks_deleted_total",
            "database_size_bytes", "disk_total_bytes", "disk_used_bytes",
            "disk_free_bytes", "disk_used_percent", "warning_state",
            "warning_thresholds_percent",
        }
        self.assertTrue(required.issubset(report))
        self.assertEqual(
            report["warning_thresholds_percent"],
            {"warning": 70, "elevated": 80, "critical": 90},
        )


if __name__ == "__main__":
    unittest.main()
