import copy
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from db import Base
from models import StrategyCycleDiagnostic
from services import strategy_diagnostics_service as diagnostics


class StrategyDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        handle.close()
        self.path = handle.name
        self.engine = create_engine(
            f"sqlite:///{self.path}",
            connect_args={"check_same_thread": False},
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session_patch = patch.object(diagnostics, "SessionLocal", self.Session)
        self.session_patch.start()
        diagnostics._last_cleanup_at = None

    def tearDown(self):
        self.session_patch.stop()
        self.engine.dispose()
        os.unlink(self.path)

    @staticmethod
    def result():
        return {
            "signal": "WAIT",
            "final_signal": "WAIT",
            "blocked_reason": "WAIT_5M_CONFIRMATION",
            "trend_15m": {
                "trend": "BULLISH",
                "buy_allowed": True,
                "sell_allowed": False,
                "ema_fast": 1.105,
                "ema_slow": 1.104,
                "close": 1.106,
            },
            "fifteen_m_swing_break": {
                "side": "BUY",
                "level": 1.105,
                "bos_buffer": 0.0001,
                "break_close": 1.1052,
                "break_close_time": "2026-08-07T14:15:00+00:00",
                "reason": "15M_BOS_SWING_BREAK_CLOSED",
                "raw_swings": [
                    {"type": "LOW", "time": "2026-08-07T13:00:00+00:00", "price": 1.103, "swing_size": 0.0012, "valid": True, "valid_reason": "single_100_point_swing"},
                    {"type": "HIGH", "time": "2026-08-07T13:30:00+00:00", "price": 1.105, "swing_size": 0.002, "valid": True, "valid_reason": "single_100_point_swing"},
                ],
                "swings": [
                    {"type": "LOW", "time": "2026-08-07T13:00:00+00:00", "price": 1.103, "swing_size": 0.0012, "valid": True, "valid_reason": "single_100_point_swing"},
                    {"type": "HIGH", "time": "2026-08-07T13:30:00+00:00", "price": 1.105, "swing_size": 0.002, "valid": True, "valid_reason": "single_100_point_swing"},
                ],
            },
            "confirmation_5m": {
                "side": "WAIT",
                "close_confirmed": False,
                "reason": "WAIT_5M_CONFIRMATION",
            },
            "consolidation": {
                "is_consolidation": False,
                "conditions_met": 1,
                "high_overlap": True,
                "compressed_range": False,
                "ema_compressed": False,
                "overlap_pairs": 5,
                "atr14": 0.0007,
                "reason": None,
            },
            "strategy_stage_states": {
                "swing_detection": "PASSED",
                "fifteen_m_bos": "PASSED",
                "fifteen_m_close": "PASSED",
                "ema": "PASSED",
                "five_m_confirmation": "FAILED",
                "consolidation_gate": "PASSED",
                "swing_sl": "NOT_EVALUATED",
                "tp_rr": "NOT_EVALUATED",
                "execution": "BLOCKED",
            },
        }

    @staticmethod
    def frames():
        index_15 = pd.date_range("2026-08-07T13:00:00Z", periods=6, freq="15min")
        index_5 = pd.date_range("2026-08-07T14:00:00Z", periods=6, freq="5min")
        data = {"Open": [1.1] * 6, "High": [1.2] * 6, "Low": [1.0] * 6, "Close": [1.1] * 6}
        return pd.DataFrame(data, index=index_5), pd.DataFrame(data, index=index_15)

    def test_one_evaluation_creates_one_coherent_snapshot_and_survives_restart(self):
        result = self.result()
        data_5m, data_15m = self.frames()
        snapshot = diagnostics.persist_cycle_safely(
            "EURUSD", result, data_5m, data_15m
        )

        restarted_session = sessionmaker(bind=self.engine)
        with restarted_session() as db:
            rows = db.execute(select(StrategyCycleDiagnostic)).scalars().all()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].cycle_id, snapshot["cycle_id"])
        self.assertEqual(result["audit_diagnostics"]["cycle_id"], rows[0].cycle_id)
        self.assertEqual(rows[0].snapshot_json["final_decision"]["decision"], "WAIT")

    def test_decision_and_displayed_progress_are_from_same_cycle(self):
        result = self.result()
        snapshot = diagnostics.build_snapshot(
            "EURUSD",
            result,
            now=datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(snapshot["progress"]["displayed_percent"], 60)
        self.assertEqual(snapshot["final_decision"]["reason"], "WAIT_5M_CONFIRMATION")
        self.assertEqual(snapshot["cycle_id"], snapshot["cycle_id"])
        self.assertEqual(
            snapshot["progress"]["missing_required_stages"],
            ["m5_confirmation", "swing_sl"],
        )

    def test_persistence_failure_cannot_trigger_block_or_mutate_trade_fields(self):
        result = self.result()
        before = copy.deepcopy(result)
        with patch.object(diagnostics, "SessionLocal", side_effect=RuntimeError("db down")):
            snapshot = diagnostics.persist_cycle_safely("EURUSD", result)
        self.assertIsNone(snapshot)
        self.assertEqual(result, before)
        self.assertEqual(result["final_signal"], "WAIT")

    def test_diagnostic_observer_does_not_change_any_trading_rule_output(self):
        result = self.result()
        trading_keys = {
            key: copy.deepcopy(result.get(key))
            for key in (
                "signal", "final_signal", "trend_15m", "fifteen_m_swing_break",
                "confirmation_5m", "consolidation", "strategy_stage_states",
            )
        }
        diagnostics.persist_cycle_safely("XAUUSD", result)
        for key, value in trading_keys.items():
            self.assertEqual(result.get(key), value)
        self.assertIn("audit_diagnostics", result)

    def test_read_filter_uses_symbol_decision_and_reason(self):
        result = self.result()
        diagnostics.persist_cycle_safely("EURUSD", result)
        diagnostics.persist_cycle_safely("XAUUSD", result)
        rows = diagnostics.query_cycles(
            symbol="EURUSD",
            decision="WAIT",
            block_reason="WAIT_5M_CONFIRMATION",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "EURUSD")

    def test_execution_outcome_updates_same_cycle_instead_of_creating_another(self):
        result = self.result()
        snapshot = diagnostics.persist_cycle_safely("EURUSD", result)
        self.assertTrue(diagnostics.update_execution_outcome_safely(
            snapshot["cycle_id"],
            "BUY_EXECUTED",
            details={"broker_position_id": "123"},
        ))
        with self.Session() as db:
            rows = db.execute(select(StrategyCycleDiagnostic)).scalars().all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].decision, "BUY_EXECUTED")
        self.assertEqual(
            rows[0].snapshot_json["final_decision"]["execution_details"]["broker_position_id"],
            "123",
        )


if __name__ == "__main__":
    unittest.main()
