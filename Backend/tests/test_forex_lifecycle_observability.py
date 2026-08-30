import ast
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from db import Base
from models import ForexExecutionSnapshot

from services.forex_observability_service import (
    build_lifecycle_values,
    deterministic_confirmation_id,
    deterministic_event_id,
    displacement,
    persist_execution_snapshot_safely,
)
from services.forex_shadow_freshness_service import evaluate_shadow_policies


NOW = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)


def snapshot(symbol="EURUSD", event_time=None, confirmation_time=None, signal_ready=True):
    event_time = event_time or NOW - timedelta(minutes=15)
    confirmation_time = confirmation_time or NOW - timedelta(minutes=10)
    return {
        "cycle_id": "cycle-1",
        "session_id": "session-a",
        "symbol": symbol,
        "timing": {"evaluation_timestamp": NOW.isoformat()},
        "bos": {
            "direction": "BUY",
            "classification": "CHOCH",
            "candle_timestamp": event_time.isoformat(),
            "watched_level": 1.1655 if symbol == "EURUSD" else 4620.0,
            "event_age_candles": 1,
            "event_invalidation_swing_time": (event_time - timedelta(minutes=30)).isoformat(),
            "event_invalidation_swing_price": 1.1640 if symbol == "EURUSD" else 4600.0,
        },
        "swings": {},
        "m5_confirmation": {
            "candle_timestamp": confirmation_time.isoformat(),
            "price": 1.1660 if symbol == "EURUSD" else 4621.0,
        },
        "trend": {"classification": "BULLISH"},
        "trade_plan": {
            "entry": 1.1665 if symbol == "EURUSD" else 4621.5,
            "r_multiple": 2.0,
        },
        "final_decision": {
            "decision": "BUY_READY" if signal_ready else "WAIT",
            "reason": None if signal_ready else "WAIT_CONFIRMATION",
        },
    }


class ForexLifecycleObservabilityTests(unittest.TestCase):
    def test_deterministic_event_id_uses_point_normalization(self):
        args = ("EURUSD", "CHOCH", "BUY", NOW, 1.16550, NOW - timedelta(minutes=30), 1.16400)
        self.assertEqual(deterministic_event_id(*args), deterministic_event_id(*args))
        self.assertEqual(
            deterministic_event_id(*args),
            deterministic_event_id("EURUSD", "CHOCH", "BUY", NOW.isoformat(), "1.1655000", (NOW - timedelta(minutes=30)).isoformat(), "1.164000"),
        )

    def test_deterministic_confirmation_id(self):
        event_id = deterministic_event_id("EURUSD", "BOS", "BUY", NOW, 1.1, NOW, 1.0)
        first = deterministic_confirmation_id("EURUSD", event_id, NOW, "BUY", 1.16600)
        second = deterministic_confirmation_id("EURUSD", event_id, NOW.isoformat(), "BUY", "1.166000")
        self.assertEqual(first, second)

    def test_eurusd_and_gold_displacement_points(self):
        self.assertEqual(displacement("EURUSD", 1.1665, 1.1660), (0.0005, 50.0))
        self.assertEqual(displacement("XAUUSD", 4621.5, 4621.0), (0.5, 50.0))

    def test_present_absent_same_event_increments_revival(self):
        present = build_lifecycle_values(snapshot(), now=NOW)
        previous_present = SimpleNamespace(**present)
        absent_snapshot = snapshot(signal_ready=False)
        absent_snapshot["bos"] = {"direction": "WAIT"}
        absent_snapshot["m5_confirmation"] = {}
        absent = build_lifecycle_values(absent_snapshot, previous_present, now=NOW + timedelta(minutes=5))
        revived = build_lifecycle_values(snapshot(), SimpleNamespace(**absent), now=NOW + timedelta(minutes=10))
        self.assertEqual(revived["revival_count"], 1)
        self.assertEqual(revived["setup_generation"], 2)

    def test_reused_confirmation_and_reference_survive_revival(self):
        present = build_lifecycle_values(snapshot(), now=NOW)
        absent_snapshot = snapshot(signal_ready=False)
        absent_snapshot["bos"] = {"direction": "WAIT"}
        absent_snapshot["m5_confirmation"] = {}
        absent = build_lifecycle_values(absent_snapshot, SimpleNamespace(**present), now=NOW + timedelta(minutes=5))
        revived = build_lifecycle_values(snapshot(), SimpleNamespace(**absent), now=NOW + timedelta(minutes=10))
        self.assertTrue(revived["confirmation_reused"])
        self.assertEqual(revived["event_id"], present["event_id"])

    def test_new_confirmation_after_revival(self):
        present = build_lifecycle_values(snapshot(), now=NOW)
        absent_snapshot = snapshot(signal_ready=False)
        absent_snapshot["bos"] = {"direction": "WAIT"}
        absent_snapshot["m5_confirmation"] = {}
        absent = build_lifecycle_values(absent_snapshot, SimpleNamespace(**present), now=NOW + timedelta(minutes=5))
        revived = build_lifecycle_values(snapshot(), SimpleNamespace(**absent), now=NOW + timedelta(minutes=10))
        later = snapshot(confirmation_time=NOW + timedelta(minutes=11))
        refreshed = build_lifecycle_values(later, SimpleNamespace(**revived), now=NOW + timedelta(minutes=12))
        self.assertTrue(refreshed["new_confirmation_after_revival"])
        self.assertFalse(refreshed["confirmation_reused"])

    def test_expiry_policies(self):
        for minutes in (15, 30, 45):
            policy = f"CONFIRMATION_EXPIRES_{minutes}M"
            eligible = evaluate_shadow_policies({"signal_ready": True, "confirmation_age_seconds": minutes * 60})
            expired = evaluate_shadow_policies({"signal_ready": True, "confirmation_age_seconds": minutes * 60 + 1})
            self.assertTrue(eligible[policy]["eligible"])
            self.assertFalse(expired[policy]["eligible"])

    def test_reconfirm_after_revival_policy(self):
        values = {
            "signal_ready": True, "revival_count": 1,
            "setup_last_reappeared_at": NOW,
            "confirmation_time": NOW - timedelta(minutes=5),
            "new_confirmation_after_revival": False,
        }
        self.assertFalse(evaluate_shadow_policies(values)["RECONFIRM_AFTER_REVIVAL"]["eligible"])
        values["confirmation_time"] = NOW + timedelta(minutes=5)
        self.assertTrue(evaluate_shadow_policies(values)["RECONFIRM_AFTER_REVIVAL"]["eligible"])

    def test_continuous_validity_policy(self):
        result = evaluate_shadow_policies({
            "signal_ready": True, "setup_was_absent_after_confirmation": True,
        })
        self.assertFalse(result["CONTINUOUS_VALIDITY"]["eligible"])

    def test_current_matches_production_eligibility(self):
        self.assertTrue(evaluate_shadow_policies({"signal_ready": True})["CURRENT"]["eligible"])
        self.assertFalse(evaluate_shadow_policies({"signal_ready": False})["CURRENT"]["eligible"])

    def test_shadow_does_not_mutate_input(self):
        original = {"signal_ready": True, "nested": {"value": 1}}
        before = copy.deepcopy(original)
        evaluate_shadow_policies(original)
        self.assertEqual(original, before)

    def test_shadow_has_no_broker_dependency(self):
        source = Path("Backend/services/forex_shadow_freshness_service.py").read_text()
        tree = ast.parse(source)
        imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
        rendered = " ".join(ast.unparse(node) for node in imports).lower()
        self.assertNotIn("ctrader", rendered)
        self.assertNotIn("broker", rendered)
        self.assertNotIn("api", rendered)

    def test_shadow_failure_is_contained_by_lifecycle_persistence(self):
        with patch("services.forex_observability_service.SessionLocal", side_effect=RuntimeError("db down")):
            from services.forex_observability_service import persist_lifecycle_evaluation_safely
            self.assertIsNone(persist_lifecycle_evaluation_safely(snapshot()))

    def test_shadow_failure_does_not_change_production_context(self):
        original = snapshot()
        before = copy.deepcopy(original)
        with patch(
            "services.forex_observability_service.evaluate_shadow_policies",
            side_effect=RuntimeError("shadow failed"),
        ):
            values = build_lifecycle_values(original, now=NOW)
        self.assertEqual(original, before)
        self.assertIn("error", values["shadow_policy_results"])

    def test_execution_snapshot_persistence_failure_is_contained(self):
        with patch("services.forex_observability_service.SessionLocal", side_effect=RuntimeError("db down")):
            self.assertIsNone(persist_execution_snapshot_safely(
                symbol="EURUSD", direction="BUY", trade_payload={}, plan={},
                quote={}, risk_size={}, gate_results={},
            ))

    def test_restart_session_does_not_change_ids(self):
        first = build_lifecycle_values(snapshot(), now=NOW)
        changed = snapshot()
        changed["session_id"] = "session-after-restart"
        second = build_lifecycle_values(changed, now=NOW)
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["confirmation_id"], second["confirmation_id"])

    def test_execution_snapshots_are_insert_only(self):
        handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        handle.close()
        engine = create_engine(f"sqlite:///{handle.name}")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        try:
            with patch("services.forex_observability_service.SessionLocal", Session):
                first = persist_execution_snapshot_safely(
                    symbol="EURUSD", direction="BUY",
                    trade_payload={"entry": 1.1, "sl": 1.09, "tp1": 1.11, "tp2": 1.12},
                    plan={}, quote={"ask": 1.1}, risk_size={}, gate_results={},
                )
                second = persist_execution_snapshot_safely(
                    symbol="EURUSD", direction="BUY",
                    trade_payload={"entry": 1.2, "sl": 1.19, "tp1": 1.21, "tp2": 1.22},
                    plan={}, quote={"ask": 1.2}, risk_size={}, gate_results={},
                )
            with Session() as db:
                rows = db.execute(select(ForexExecutionSnapshot).order_by(ForexExecutionSnapshot.id)).scalars().all()
            self.assertEqual(len(rows), 2)
            self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
            self.assertEqual(rows[0].snapshot_json["candidate_entry"], 1.1)
            self.assertIsNone(rows[0].broker_order_id)
        finally:
            engine.dispose()
            Path(handle.name).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
