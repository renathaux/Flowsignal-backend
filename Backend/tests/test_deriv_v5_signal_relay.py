import ast
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.deriv_binary_v5_forward_validator import (
    EXPECTED_SPEC_SHA256,
    SPEC_SHA256,
    STRATEGY_VERSION,
    evaluate_completed_candle,
    initialize_database,
    record_observation,
)
from services.deriv_v5_demo_relay_service import receive_signal
from services.deriv_v5_signal_relay import deliver_pending_relays, sign_relay_payload


def candles():
    rows = []
    for index in range(10):
        price = 1.1 + index * 0.00001
        rows.append({"epoch": 1000 + index * 300, "open": price,
                     "high": price + 0.0002, "low": price - 0.0002,
                     "close": price + 0.00001})
    rows.append({"epoch": 4000, "open": 1.1000, "high": 1.1007,
                 "low": 1.0999, "close": 1.1006})
    return rows


def ticks():
    values = [1.1000]
    for index in range(239):
        progress = (index + 1) / 239
        center = 1.1000 + 0.0006 * progress
        values.append(center + (0.00008 if index % 2 == 0 else -0.00008))
    start = values[-1]
    for index in range(60):
        values.append(start + (1.1006 - start) * ((index + 1) / 60))
    values[-1] = 1.1006
    return [{"epoch": 4000 + index, "quote": value} for index, value in enumerate(values)]


class V5RelayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.research_db = Path(self.temp.name) / "research.sqlite3"
        self.inbox_db = Path(self.temp.name) / "inbox.sqlite3"
        self.engine = create_engine(f"sqlite:///{self.inbox_db}")
        initialize_database(self.research_db, collection_start_timestamp=3900)
        self.observation = evaluate_completed_candle(candles(), ticks(), 4300)
        self.assertTrue(self.observation["qualified"])

    def tearDown(self):
        self.engine.dispose()
        self.temp.cleanup()

    def _queued_body(self):
        with sqlite3.connect(str(self.research_db)) as connection:
            return connection.execute(
                "SELECT payload_json FROM relay_outbox WHERE signal_id=?",
                (self.observation["signal_id"],),
            ).fetchone()[0].encode()

    def test_signal_is_persisted_before_one_way_relay_is_queued(self):
        record_observation(self.observation, self.research_db)
        with sqlite3.connect(str(self.research_db)) as connection:
            signal = connection.execute("SELECT signal_id FROM signals").fetchone()[0]
            outbox = connection.execute(
                "SELECT signal_id, status, attempts FROM relay_outbox"
            ).fetchone()
        self.assertEqual(signal, self.observation["signal_id"])
        self.assertEqual(outbox, (signal, "PENDING", 0))

    def test_failed_delivery_is_restart_safe_and_does_not_change_research(self):
        record_observation(self.observation, self.research_db)
        with sqlite3.connect(str(self.research_db)) as connection:
            connection.execute("UPDATE relay_outbox SET next_attempt_at=1000")
        def fail(*_args):
            raise RuntimeError("offline")
        self.assertEqual(
            deliver_pending_relays(self.research_db, now=1000, post=fail,
                                   relay_url="https://relay.invalid", secret="secret"),
            {"delivered": 0, "failed": 1, "disabled": 0},
        )
        with sqlite3.connect(str(self.research_db)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT status, attempts FROM relay_outbox").fetchone(), ("PENDING", 1))
        delivered = []
        def accept(_url, body, headers):
            delivered.append((body, headers))
            return 200
        result = deliver_pending_relays(
            self.research_db, now=1002, post=accept,
            relay_url="https://relay.invalid", secret="secret",
        )
        self.assertEqual(result["delivered"], 1)
        with sqlite3.connect(str(self.research_db)) as connection:
            self.assertEqual(connection.execute("SELECT status, attempts FROM relay_outbox").fetchone(), ("DELIVERED", 2))

    def test_receiver_authentication_and_idempotency(self):
        record_observation(self.observation, self.research_db)
        body = self._queued_body()
        timestamp = 5000
        signature = sign_relay_payload(body, timestamp, "secret")
        first = receive_signal(body, str(timestamp), signature, secret="secret", now=5000, engine=self.engine)
        second = receive_signal(body, str(timestamp), signature, secret="secret", now=5000, engine=self.engine)
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertFalse(first["broker_action"])
        with self.engine.begin() as connection:
            self.assertEqual(connection.exec_driver_sql("SELECT COUNT(*) FROM deriv_v5_relay_signals").scalar(), 1)
        with self.assertRaisesRegex(RuntimeError, "V5_RELAY_UNAUTHENTICATED"):
            receive_signal(body, str(timestamp), "bad", secret="secret", now=5000, engine=self.engine)
        with self.assertRaisesRegex(RuntimeError, "V5_RELAY_SIGNATURE_EXPIRED"):
            receive_signal(body, str(timestamp), signature, secret="secret", now=6000, engine=self.engine)

    def test_receiver_only_persists_inbox_and_never_execution_state(self):
        record_observation(self.observation, self.research_db)
        body = self._queued_body(); timestamp = 5000
        receive_signal(body, str(timestamp), sign_relay_payload(body, timestamp, "secret"),
                       secret="secret", now=timestamp, engine=self.engine)
        with self.engine.begin() as connection:
            tables = {row[0] for row in connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn("deriv_v5_relay_signals", tables)
            self.assertNotIn("deriv_binary_executions", tables)

    def test_authoritative_payload_preserves_v5_identity_and_direction(self):
        record_observation(self.observation, self.research_db)
        payload = json.loads(self._queued_body())
        self.assertEqual(payload["strategy_version"], STRATEGY_VERSION)
        self.assertEqual(payload["rule_hash"], EXPECTED_SPEC_SHA256)
        self.assertEqual(payload["direction"], self.observation["predicted_direction"])
        self.assertEqual(SPEC_SHA256, EXPECTED_SPEC_SHA256)

    def test_relay_has_no_broker_or_forex_execution_surface(self):
        paths = [
            BACKEND_DIR / "services" / "deriv_v5_signal_relay.py",
            BACKEND_DIR / "services" / "deriv_v5_demo_relay_service.py",
        ]
        forbidden = {"ctrader_connector", "place_market_order", "execute_demo_signal", "buy"}
        for path in paths:
            source = path.read_text()
            tree = ast.parse(source)
            names = {node.func.id for node in ast.walk(tree)
                     if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
            imports = {node.module for node in ast.walk(tree)
                       if isinstance(node, ast.ImportFrom) and node.module}
            self.assertTrue(names.isdisjoint(forbidden))
            self.assertTrue(imports.isdisjoint(forbidden))


if __name__ == "__main__":
    unittest.main()
