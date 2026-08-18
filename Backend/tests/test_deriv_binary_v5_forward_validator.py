import ast
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


if __name__ == "__main__":
    unittest.main()
