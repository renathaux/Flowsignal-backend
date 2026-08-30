import copy
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from db import Base
from models import AutoTradeStateAudit, StrategyCycleDiagnostic
from routes.diagnostics import strategy_cycles
from services import strategy_diagnostics_service as diagnostics
from services import forex_observability_service as forex_observability


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
        self.observability_session_patch = patch.object(
            forex_observability, "SessionLocal", self.Session
        )
        self.observability_session_patch.start()
        diagnostics._reset_runtime_state_for_tests()

    def tearDown(self):
        diagnostics._reset_runtime_state_for_tests()
        self.observability_session_patch.stop()
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

    def test_execution_gate_is_durable_without_overwriting_strategy_decision(self):
        snapshot = diagnostics.persist_cycle_safely(
            "XAUUSD",
            self.result(),
            *self.frames(),
        )
        self.assertTrue(diagnostics.record_execution_gate_safely(
            snapshot["cycle_id"],
            "XAUUSD",
            "setup-123",
            "PANEL_HANDOFF",
            "PASS",
            "VALIDATED_STRATEGY_PAYLOAD",
            {"missing_fields": []},
        ))
        self.assertTrue(diagnostics.record_execution_gate_safely(
            snapshot["cycle_id"],
            "XAUUSD",
            "setup-123",
            "PANEL_HANDOFF",
            "PASS",
            "VALIDATED_STRATEGY_PAYLOAD",
            {"missing_fields": []},
        ))

        with self.Session() as db:
            row = db.execute(
                select(StrategyCycleDiagnostic).where(
                    StrategyCycleDiagnostic.cycle_id == snapshot["cycle_id"]
                )
            ).scalar_one()
            gate = row.snapshot_json["execution_gates"][-1]
            self.assertEqual(len(row.snapshot_json["execution_gates"]), 1)
            self.assertEqual(gate["gate"], "PANEL_HANDOFF")
            self.assertEqual(gate["result"], "PASS")
            self.assertEqual(row.decision, "WAIT")
            self.assertEqual(
                row.snapshot_json["final_decision"]["decision"],
                "WAIT",
            )

    def test_first_cycle_persists_and_identical_cycle_is_skipped(self):
        first_at = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
        result = self.result()
        first = diagnostics.persist_cycle_safely("EURUSD", result, now=first_at)
        repeated = self.result()
        second = diagnostics.persist_cycle_safely(
            "EURUSD",
            repeated,
            now=first_at + timedelta(minutes=1),
        )

        with self.Session() as db:
            rows = db.execute(select(StrategyCycleDiagnostic)).scalars().all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(first["cycle_id"], second["cycle_id"])
        self.assertEqual(
            repeated["audit_diagnostics"]["cycle_id"],
            first["cycle_id"],
        )
        self.assertEqual(first["persistence"]["reason"], "FIRST_SNAPSHOT")
        self.assertEqual(
            diagnostics._diagnostic_counters["diagnostics_skipped_unchanged"],
            1,
        )

    def test_meaningful_state_change_persists_immediately(self):
        first_at = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
        diagnostics.persist_cycle_safely("EURUSD", self.result(), now=first_at)
        changed = self.result()
        changed["strategy_stage_states"]["five_m_confirmation"] = "PASSED"
        changed["confirmation_5m"] = {
            "side": "BUY",
            "close_confirmed": True,
            "confirmation_close_time": "2026-08-07T14:20:00+00:00",
            "reason": "PASSED",
        }
        snapshot = diagnostics.persist_cycle_safely(
            "EURUSD",
            changed,
            now=first_at + timedelta(seconds=30),
        )

        with self.Session() as db:
            rows = db.execute(
                select(StrategyCycleDiagnostic).order_by(StrategyCycleDiagnostic.id)
            ).scalars().all()
        self.assertEqual(len(rows), 2)
        self.assertEqual(snapshot["persistence"]["reason"], "STATE_CHANGE")

    def test_timestamp_and_price_only_changes_do_not_persist(self):
        first_at = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
        first = self.result()
        diagnostics.persist_cycle_safely("EURUSD", first, now=first_at)
        changed = self.result()
        changed["trend_15m"]["close"] = 1.10604
        changed["trend_15m"]["ema_fast"] = 1.10503
        changed["entry_price"] = 1.10605
        changed["source_debug_timestamp"] = "2026-08-07T15:01:00+00:00"
        diagnostics.persist_cycle_safely(
            "EURUSD",
            changed,
            now=first_at + timedelta(minutes=1),
        )

        with self.Session() as db:
            count = len(db.execute(select(StrategyCycleDiagnostic)).scalars().all())
        self.assertEqual(count, 1)

    def test_heartbeat_persists_at_five_minutes_but_not_before(self):
        first_at = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
        with patch.object(diagnostics, "HEARTBEAT_SECONDS", 300):
            diagnostics.persist_cycle_safely("EURUSD", self.result(), now=first_at)
            diagnostics.persist_cycle_safely(
                "EURUSD", self.result(), now=first_at + timedelta(seconds=299)
            )
            heartbeat = diagnostics.persist_cycle_safely(
                "EURUSD", self.result(), now=first_at + timedelta(seconds=300)
            )

        with self.Session() as db:
            rows = db.execute(
                select(StrategyCycleDiagnostic).order_by(StrategyCycleDiagnostic.id)
            ).scalars().all()
        self.assertEqual(len(rows), 2)
        self.assertEqual(heartbeat["persistence"]["reason"], "HEARTBEAT")
        self.assertEqual(
            diagnostics._diagnostic_counters["persisted_heartbeat"],
            1,
        )

    def test_symbols_have_independent_state_and_heartbeat(self):
        first_at = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
        diagnostics.persist_cycle_safely("EURUSD", self.result(), now=first_at)
        diagnostics.persist_cycle_safely("XAUUSD", self.result(), now=first_at)
        changed_eurusd = self.result()
        changed_eurusd["blocked_reason"] = "WAIT_CONSOLIDATION"
        diagnostics.persist_cycle_safely(
            "EURUSD", changed_eurusd, now=first_at + timedelta(minutes=1)
        )
        diagnostics.persist_cycle_safely(
            "XAUUSD", self.result(), now=first_at + timedelta(minutes=1)
        )

        with self.Session() as db:
            rows = db.execute(select(StrategyCycleDiagnostic)).scalars().all()
        self.assertEqual(sum(row.symbol == "EURUSD" for row in rows), 2)
        self.assertEqual(sum(row.symbol == "XAUUSD" for row in rows), 1)

    def test_restart_reconstructs_latest_state_and_avoids_duplicate(self):
        first_at = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)
        first = diagnostics.persist_cycle_safely(
            "EURUSD", self.result(), now=first_at
        )
        diagnostics._symbol_persistence_state.clear()
        diagnostics._restart_loaded_symbols.clear()

        restarted_result = self.result()
        restored = diagnostics.persist_cycle_safely(
            "EURUSD",
            restarted_result,
            now=first_at + timedelta(minutes=1),
        )

        with self.Session() as db:
            rows = db.execute(select(StrategyCycleDiagnostic)).scalars().all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(restored["cycle_id"], first["cycle_id"])
        self.assertEqual(
            restarted_result["audit_diagnostics"]["cycle_id"],
            first["cycle_id"],
        )

    def test_fingerprint_covers_required_semantic_gates(self):
        base = self.result()
        baseline = diagnostics.meaningful_state_fingerprint(
            diagnostics.build_snapshot("EURUSD", base)
        )
        variants = []

        plan = self.result()
        plan["plan_type"] = "BUY READY"
        variants.append(plan)

        signal = self.result()
        signal["signal"] = "BUY"
        signal["final_signal"] = "BUY"
        variants.append(signal)

        bos = self.result()
        bos["strategy_stage_states"]["fifteen_m_bos"] = "FAILED"
        bos["fifteen_m_swing_break"]["break_type"] = "CHOCH"
        variants.append(bos)

        ema = self.result()
        ema["trend_15m"]["buy_allowed"] = False
        ema["trend_15m"]["trend"] = "NEUTRAL"
        variants.append(ema)

        consolidation = self.result()
        consolidation["consolidation"]["is_consolidation"] = True
        consolidation["consolidation"]["reason"] = "WAIT_CONSOLIDATION"
        variants.append(consolidation)

        risk = self.result()
        risk["strategy_stage_states"]["tp_rr"] = "BLOCKED"
        variants.append(risk)

        news = self.result()
        news["news_trading"] = {
            "mode": "BLOCK_ONLY",
            "blocked": True,
            "reason": "WAIT_NEWS_BLOCK",
        }
        variants.append(news)

        execution = self.result()
        execution["strategy_setup_complete"] = True
        execution["strategy_stage_states"]["execution"] = "PASSED"
        variants.append(execution)

        position = self.result()
        position["trade_already_running"] = True
        variants.append(position)

        setup = self.result()
        setup["signal_setup_id"] = "setup-2"
        variants.append(setup)

        blocked = self.result()
        blocked["blocked_reason"] = "WAIT_NEWS_BLOCK"
        variants.append(blocked)

        for variant in variants:
            with self.subTest(variant=variant):
                fingerprint = diagnostics.meaningful_state_fingerprint(
                    diagnostics.build_snapshot("EURUSD", variant)
                )
                self.assertNotEqual(fingerprint, baseline)

    def test_default_retention_is_one_hundred_twenty_days(self):
        self.assertEqual(diagnostics.DEFAULT_RETENTION_DAYS, 120)
        self.assertEqual(diagnostics._configured_retention_days({}), 120)

    def test_retention_is_configurable_with_a_one_day_minimum(self):
        setting = "STRATEGY_DIAGNOSTICS_RETENTION_DAYS"
        self.assertEqual(
            diagnostics._configured_retention_days({setting: "12"}),
            12,
        )
        self.assertEqual(
            diagnostics._configured_retention_days({setting: "1"}),
            1,
        )
        self.assertEqual(
            diagnostics._configured_retention_days({setting: "0"}),
            1,
        )
        self.assertEqual(
            diagnostics._configured_retention_days({setting: "invalid"}),
            120,
        )

    def test_retention_cleanup_preserves_newer_rows_and_deletes_older_rows(self):
        now = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        with patch.object(diagnostics, "RETENTION_DAYS", 7):
            diagnostics.persist_cycle_safely(
                "EURUSD",
                self.result(),
                now=now - timedelta(days=8),
            )
            diagnostics.persist_cycle_safely(
                "EURUSD",
                self.result(),
                now=now - timedelta(days=6),
            )
            diagnostics.persist_cycle_safely(
                "EURUSD",
                self.result(),
                now=now,
            )

        with self.Session() as db:
            timestamps = [
                row.evaluation_timestamp.replace(tzinfo=timezone.utc)
                for row in db.execute(
                    select(StrategyCycleDiagnostic).order_by(
                        StrategyCycleDiagnostic.evaluation_timestamp
                    )
                ).scalars().all()
            ]
        self.assertEqual(
            timestamps,
            [now - timedelta(days=6), now],
        )

    def test_retention_cleanup_executes_at_most_hourly(self):
        now = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        db = MagicMock()
        diagnostics._cleanup_if_due(db, now)
        diagnostics._cleanup_if_due(db, now + timedelta(minutes=59))
        self.assertEqual(db.execute.call_count, 1)
        diagnostics._cleanup_if_due(db, now + timedelta(hours=1))
        self.assertEqual(db.execute.call_count, 2)

    def test_retention_cleanup_does_not_delete_trading_audit_history(self):
        now = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        old_at = now - timedelta(days=8)
        with self.Session() as db:
            db.add(AutoTradeStateAudit(
                trading_mode="LIVE",
                previous_enabled=False,
                new_enabled=True,
                updated_by="retention-test",
                active_broker_account="demo-account",
                broker_environment="demo",
                timestamp=old_at,
                request_source="test",
                reason="unrelated trading audit",
            ))
            db.commit()

        with patch.object(diagnostics, "RETENTION_DAYS", 7):
            diagnostics.persist_cycle_safely(
                "EURUSD",
                self.result(),
                now=old_at,
            )
            diagnostics.persist_cycle_safely(
                "EURUSD",
                self.result(),
                now=now,
            )

        with self.Session() as db:
            diagnostic_count = len(
                db.execute(select(StrategyCycleDiagnostic)).scalars().all()
            )
            audit_count = len(
                db.execute(select(AutoTradeStateAudit)).scalars().all()
            )
        self.assertEqual(diagnostic_count, 1)
        self.assertEqual(audit_count, 1)

    def test_diagnostic_endpoint_contract_remains_compatible(self):
        diagnostics.persist_cycle_safely("EURUSD", self.result())
        response = strategy_cycles(
            symbol="EURUSD",
            start=None,
            end=None,
            decision="WAIT",
            block_reason="WAIT_5M_CONFIRMATION",
            limit=100,
            offset=0,
        )
        self.assertEqual(response["count"], 1)
        self.assertEqual(response["items"][0]["symbol"], "EURUSD")
        self.assertEqual(response["limit"], 100)
        self.assertEqual(response["offset"], 0)
        self.assertTrue(response["read_only"])

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

    def test_xauusd_deep_swing_history_is_projected_without_recursion(self):
        result = self.result()
        prior = None
        swings = []
        for index in range(1200):
            swing = {
                "type": "HIGH" if index % 2 else "LOW",
                "time": f"2026-07-{(index % 28) + 1:02d}T12:00:00+00:00",
                "price": 2400.0 + index,
                "index": index,
                "swing_size": 1.5,
                "valid": True,
                "valid_reason": "single_100_point_swing",
                "reference_swing": prior,
            }
            swings.append(swing)
            prior = swing
        result["fifteen_m_swing_break"]["raw_swings"] = swings
        result["fifteen_m_swing_break"]["swings"] = swings
        result["fifteen_m_swing_break"]["swing"] = swings[-1]

        snapshot = diagnostics.build_snapshot("XAUUSD", result)

        selected = snapshot["swings"]["selected"]
        self.assertEqual(selected["type"], swings[-1]["type"])
        self.assertEqual(
            selected["reference_swing"]["price"],
            swings[-2]["price"],
        )
        self.assertNotIn(
            "reference_swing",
            selected["reference_swing"],
        )
        self.assertEqual(snapshot["symbol"], "XAUUSD")
        self.assertIn("bos", snapshot)
        self.assertIn("noise_consolidation", snapshot)
        self.assertIn("final_decision", snapshot)

    def test_cycle_guard_replaces_only_offending_diagnostic_subfield(self):
        result = self.result()
        source_state = {"symbol": "XAUUSD", "available": True}
        source_state["recursive_state"] = source_state

        with self.assertLogs(
            diagnostics.__name__, level="WARNING"
        ) as captured:
            snapshot = diagnostics.build_snapshot(
                "XAUUSD",
                result,
                source_state=source_state,
            )

        self.assertEqual(snapshot["source_state"]["symbol"], "XAUUSD")
        self.assertTrue(snapshot["source_state"]["available"])
        self.assertEqual(
            snapshot["source_state"]["recursive_state"],
            "CYCLE_DETECTED",
        )
        self.assertEqual(snapshot["final_decision"]["decision"], "WAIT")
        self.assertTrue(any(
            "$.source_state.recursive_state" in message
            for message in captured.output
        ))

    def test_eurusd_swing_projection_preserves_immediate_reference_fields(self):
        result = self.result()
        snapshot = diagnostics.build_snapshot("EURUSD", result)

        watched_high = snapshot["swings"]["watched_high"]
        self.assertEqual(watched_high["price"], 1.105)
        self.assertEqual(watched_high["size"], 0.002)
        self.assertTrue(watched_high["qualified"])
        self.assertIsNone(watched_high["reference_swing"])

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
        original_decision = snapshot["final_decision"]["decision"]
        self.assertTrue(diagnostics.update_execution_outcome_safely(
            snapshot["cycle_id"],
            "BLOCKED",
            "ACTIVE_TRADE_ALREADY_RUNNING",
            details={"broker_position_id": "123"},
        ))
        with self.Session() as db:
            rows = db.execute(select(StrategyCycleDiagnostic)).scalars().all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].decision, original_decision)
        self.assertEqual(
            rows[0].snapshot_json["final_decision"]["decision"],
            original_decision,
        )
        self.assertFalse(rows[0].snapshot_json["execution_outcome"]["execution_allowed"])
        self.assertEqual(
            rows[0].snapshot_json["execution_outcome"]["execution_block_reason"],
            "ACTIVE_TRADE_ALREADY_RUNNING",
        )
        self.assertEqual(
            rows[0].snapshot_json["final_decision"]["execution_details"]["broker_position_id"],
            "123",
        )


if __name__ == "__main__":
    unittest.main()
