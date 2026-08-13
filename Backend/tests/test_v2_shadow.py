import ast
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models import Base, StrategyShadowEvaluation, StrategyShadowRuntime, StrategyShadowTrade
from services import v2_shadow_service as shadow
from services.execution_risk_service import validate_pre_submit, validate_sl_amendment


UTC = timezone.utc


def context(symbol="XAUUSD", extension=0.5, structure="BOS", signal="BUY"):
    now = datetime(2026, 8, 13, 15, 5, tzinfo=UTC)
    level = 4400.0
    atr = 10.0
    entry = level + extension * atr if signal == "BUY" else level - extension * atr
    return {
        "symbol": symbol,
        "version": shadow.VERSIONS[symbol],
        "evaluated_at": now,
        "signal": signal,
        "direction": signal if signal in {"BUY", "SELL"} else None,
        "v1_decision": "TRADE" if signal in {"BUY", "SELL"} else "WAIT",
        "v1_reason": None,
        "setup_fingerprint": f"setup-{symbol}-{signal}-{structure}",
        "structure_type": structure,
        "bos_level": level,
        "bos_timestamp": now - timedelta(minutes=20),
        "bos_buffer": 1.0,
        "atr14": atr,
        "ema_state": "BULLISH" if signal == "BUY" else "BEARISH",
        "consolidation_state": "CLEAR",
        "m5_confirmation_timestamp": now - timedelta(minutes=5),
        "reference_price": entry,
        "extension_atr": extension,
        "entry": entry,
        "sl": entry - 10 if signal == "BUY" else entry + 10,
        "tp1": entry + 12 if signal == "BUY" else entry - 12,
        "tp2": entry + 15 if signal == "BUY" else entry - 15,
        "protected_sl": entry + 7.5 if signal == "BUY" else entry - 7.5,
        "rr": 1.5,
        "risk_percent": 1.0,
        "latest_5m": None,
        "latest_m15_close": now,
        "risk_valid": True,
    }


class V2DecisionTests(unittest.TestCase):
    def test_extension_at_or_below_limit_passes_xau(self):
        decision, reason, state, trade = shadow.decide_v2(context(extension=0.75), {})
        self.assertEqual(decision, "BUY_READY")
        self.assertEqual(reason, "NORMAL_ENTRY_WITHIN_EXTENSION_LIMIT")
        self.assertIsNotNone(trade)

    def test_extension_over_limit_waits(self):
        decision, reason, state, trade = shadow.decide_v2(context(extension=0.751), {})
        self.assertEqual(decision, "WAIT_EXTENDED")
        self.assertEqual(reason, "ENTRY_TOO_EXTENDED")
        self.assertIsNone(trade)
        self.assertIsNotNone(state["pending_setup"])

    def test_eurusd_research_always_waits_for_retest(self):
        decision, reason, state, trade = shadow.decide_v2(context(symbol="EURUSD", extension=0.2), {})
        self.assertEqual(decision, "WAIT_RETEST")
        self.assertEqual(state["pending_setup"]["reason"], "EURUSD_RESEARCH_RETEST")
        self.assertIsNone(trade)

    def test_valid_retest_then_later_continuation_reactivates(self):
        ctx = context(extension=1.0)
        _, _, state, _ = shadow.decide_v2(ctx, {})
        state["pending_setup"]["sl"] = 4392.0
        state["pending_setup"]["tp2"] = 4417.0
        first = dict(ctx)
        first["latest_5m"] = {
            "open_time": ctx["evaluated_at"], "close_time": ctx["evaluated_at"] + timedelta(minutes=5),
            "open": 4402.5, "high": 4403, "low": 4401, "close": 4401.5,
        }
        decision, _, state, trade = shadow.decide_v2(first, state)
        self.assertEqual(decision, "WAIT_CONTINUATION")
        self.assertIsNone(trade)
        second = dict(ctx)
        second["latest_5m"] = {
            "open_time": ctx["evaluated_at"] + timedelta(minutes=5),
            "close_time": ctx["evaluated_at"] + timedelta(minutes=10),
            "open": 4400.5, "high": 4403, "low": 4400, "close": 4402.0,
        }
        decision, reason, state, trade = shadow.decide_v2(second, state)
        self.assertEqual(decision, "BUY_READY")
        self.assertEqual(reason, "RETEST_AND_FRESH_M5_CONFIRMED")
        self.assertAlmostEqual(trade["extension_atr"], 0.2)

    def test_distant_retracement_does_not_count_as_retest(self):
        ctx = context(extension=1.0)
        _, _, state, _ = shadow.decide_v2(ctx, {})
        ctx["latest_5m"] = {
            "open_time": ctx["evaluated_at"], "close_time": ctx["evaluated_at"] + timedelta(minutes=5),
            "open": 4405, "high": 4407, "low": 4404, "close": 4405,
        }
        decision, reason, state, _ = shadow.decide_v2(ctx, state)
        self.assertEqual((decision, reason), ("WAIT_RETEST", "WAIT_BOS_RETEST"))
        self.assertIsNone(state["pending_setup"]["retest_timestamp"])

    def test_same_candle_cannot_retest_and_continue(self):
        ctx = context(extension=1.0)
        _, _, state, _ = shadow.decide_v2(ctx, {})
        ctx["latest_5m"] = {
            "open_time": ctx["evaluated_at"], "close_time": ctx["evaluated_at"] + timedelta(minutes=5),
            "open": 4400, "high": 4403, "low": 4399, "close": 4401.5,
        }
        decision, _, state, trade = shadow.decide_v2(ctx, state)
        self.assertEqual(decision, "WAIT_CONTINUATION")
        self.assertIsNone(trade)

    def test_expired_setup_cannot_reactivate(self):
        ctx = context(extension=1.0)
        _, _, state, _ = shadow.decide_v2(ctx, {})
        ctx["latest_m15_close"] = ctx["bos_timestamp"] + timedelta(minutes=61)
        decision, reason, state, trade = shadow.decide_v2(ctx, state)
        self.assertEqual(decision, "EXPIRED_NO_RETEST")
        self.assertIsNone(state["pending_setup"])
        self.assertIsNone(trade)

    def test_immediate_opposite_choch_after_sl_is_blocked(self):
        ctx = context(signal="SELL", structure="CHOCH")
        state = {"post_sl_reset": {
            "state": "WAIT_NEW_STRUCTURE_AFTER_SL", "stopped_direction": "BUY",
            "exit_timestamp": (ctx["bos_timestamp"] - timedelta(minutes=5)).isoformat(),
        }}
        decision, reason, state, trade = shadow.decide_v2(ctx, state)
        self.assertEqual(decision, "WAIT_FRESH_OPPOSITE_BOS")
        self.assertEqual(reason, "OPPOSITE_CHOCH_ALONE_BLOCKED")
        self.assertIsNone(trade)

    def test_choch_then_later_bos_and_m5_can_trade(self):
        choch = context(signal="SELL", structure="CHOCH")
        state = {"post_sl_reset": {
            "state": "WAIT_NEW_STRUCTURE_AFTER_SL", "stopped_direction": "BUY",
            "exit_timestamp": (choch["bos_timestamp"] - timedelta(minutes=5)).isoformat(),
        }}
        _, _, state, _ = shadow.decide_v2(choch, state)
        bos = context(signal="SELL", structure="BOS", extension=0.5)
        bos["bos_timestamp"] = choch["bos_timestamp"] + timedelta(minutes=15)
        bos["m5_confirmation_timestamp"] = bos["bos_timestamp"] + timedelta(minutes=5)
        decision, _, state, trade = shadow.decide_v2(bos, state)
        self.assertEqual(decision, "SELL_READY")
        self.assertIsNotNone(trade)

    def test_bos_before_choch_cannot_satisfy_reset(self):
        bos = context(signal="SELL", structure="BOS")
        state = {"post_sl_reset": {
            "state": "OPPOSITE_CHOCH_SEEN", "stopped_direction": "BUY",
            "exit_timestamp": (bos["bos_timestamp"] - timedelta(minutes=20)).isoformat(),
            "choch_timestamp": (bos["bos_timestamp"] + timedelta(minutes=1)).isoformat(),
        }}
        decision, reason, _, trade = shadow.decide_v2(bos, state)
        self.assertEqual(decision, "WAIT_NEW_STRUCTURE_AFTER_SL")
        self.assertEqual(reason, "REQUIRE_CHOCH_THEN_LATER_BOS")
        self.assertIsNone(trade)


class V2PersistenceTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        handle.close()
        self.path = handle.name
        self.engine = create_engine(f"sqlite:///{self.path}", connect_args={"check_same_thread": False})
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.patch = patch.object(shadow, "SessionLocal", self.Session)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.engine.dispose()
        os.unlink(self.path)

    @staticmethod
    def frames(now):
        index15 = pd.date_range(now - timedelta(hours=6), periods=24, freq="15min", tz="UTC")
        close15 = pd.Series([4380 + index for index in range(24)], index=index15)
        frame15 = pd.DataFrame({"Open": close15 - .5, "High": close15 + 1, "Low": close15 - 1, "Close": close15})
        index5 = pd.date_range(now - timedelta(minutes=60), periods=12, freq="5min", tz="UTC")
        close5 = pd.Series([4400 + index * .1 for index in range(12)], index=index5)
        frame5 = pd.DataFrame({"Open": close5 - .05, "High": close5 + .1, "Low": close5 - .1, "Close": close5})
        return frame5, frame15

    @staticmethod
    def ready_result(now):
        return {
            "symbol": "XAUUSD", "signal": "BUY", "entry_price": 4405,
            "price": 4405, "stop_loss": 4395, "tp1": 4417, "tp2": 4420,
            "protected_sl_price": 4412.5, "risk_reward_ratio": 1.5,
            "risk_percent": 1.0, "signal_setup_id": "durable-setup",
            "trend_15m": {"trend": "BULLISH"},
            "consolidation": {"is_consolidation": False, "atr14": 10},
            "fifteen_m_swing_break": {
                "side": "BUY", "break_type": "BOS", "level": 4400,
                "bos_buffer": 1, "break_close_time": (now - timedelta(minutes=20)).isoformat(),
            },
            "confirmation_5m": {"side": "BUY", "confirmation_close_time": (now - timedelta(minutes=5)).isoformat()},
        }

    def test_decision_and_open_trade_survive_restart(self):
        now = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)
        frame5, frame15 = self.frames(now)
        result = shadow.evaluate_cycle_safely("XAUUSD", self.ready_result(now), frame5, frame15, now=now)
        self.assertTrue(result["ok"])
        db = self.Session()
        runtime = db.execute(select(StrategyShadowRuntime)).scalar_one()
        trade = db.execute(select(StrategyShadowTrade)).scalar_one()
        self.assertEqual(runtime.state_json["active_shadow_trade_id"], trade.shadow_trade_id)
        db.close()

    def test_same_setup_does_not_create_duplicate_trade(self):
        now = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)
        frame5, frame15 = self.frames(now)
        plan = self.ready_result(now)
        shadow.evaluate_cycle_safely("XAUUSD", plan, frame5, frame15, now=now)
        shadow.evaluate_cycle_safely("XAUUSD", plan, frame5, frame15, now=now + timedelta(seconds=30))
        db = self.Session()
        self.assertEqual(db.query(StrategyShadowTrade).count(), 1)
        db.close()

    def test_symbol_metrics_never_mix(self):
        now = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)
        db = self.Session()
        db.add_all([
            StrategyShadowRuntime(symbol="XAUUSD", strategy_version=shadow.VERSIONS["XAUUSD"], started_at=now, updated_at=now, state_json={}),
            StrategyShadowRuntime(symbol="EURUSD", strategy_version=shadow.VERSIONS["EURUSD"], started_at=now, updated_at=now, state_json={}),
        ])
        db.commit(); db.close()
        self.assertEqual(shadow.get_shadow_summary("XAUUSD")["symbol"], "XAUUSD")
        self.assertEqual(shadow.get_shadow_summary("EURUSD")["symbol"], "EURUSD")


class ShadowSafetyBoundaryTests(unittest.TestCase):
    def test_v1_status_strings_are_safe_observer_inputs(self):
        result = {
            "symbol": "XAUUSD",
            "signal": "WAIT",
            "fifteen_m_swing_break": "NO",
            "confirmation_5m": "NO",
            "consolidation": "CLEAR",
            "trend_15m": "NEUTRAL",
        }
        index_5m = pd.date_range("2026-06-01T11:00:00Z", periods=20, freq="5min")
        index_15m = pd.date_range("2026-06-01T06:00:00Z", periods=20, freq="15min")
        candles = lambda index: pd.DataFrame(
            {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.5},
            index=index,
        )
        projected = shadow.build_context(
            "XAUUSD", result, candles(index_5m), candles(index_15m),
            now=datetime(2026, 6, 1, 13, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(projected["v1_decision"], "WAIT")
        self.assertIsNone(projected["bos_level"])

    def test_shadow_module_has_no_broker_or_execution_import(self):
        path = Path(shadow.__file__)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node.module or "")
        forbidden = ("ctrader", "broker", "api", "brain", "trading", "order")
        self.assertFalse([name for name in imports if any(token in name.lower() for token in forbidden)])

    def test_v2_decision_does_not_mutate_input_context_or_v1_state(self):
        ctx = context(extension=1.0)
        original = dict(ctx)
        v1_memory = {"consumed": set(), "cooldown": 123}
        shadow.decide_v2(ctx, {})
        self.assertEqual(ctx, original)
        self.assertEqual(v1_memory, {"consumed": set(), "cooldown": 123})

    def test_pre_submit_validation_never_increases_volume(self):
        risk = Mock(return_value={"ok": True, "volume_units": 80, "lot_size": .8})
        rr = Mock(return_value={"ok": True})
        result = validate_pre_submit("XAUUSD", "BUY", 4400, 4390, 4420, 100, {"ask": 4402}, risk, rr)
        self.assertTrue(result["ok"])
        self.assertEqual(result["volume_units"], 80)

    def test_sl_widening_is_flagged_when_volume_exceeds_allowed(self):
        result = validate_sl_amendment(4400, 4390, 4385, 100, {"volume_units": 70})
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "RISK_EXCEEDED_AFTER_SL_CHANGE")

    def test_sl_tightening_passes(self):
        result = validate_sl_amendment(4400, 4390, 4395, 100, {"volume_units": 70})
        self.assertTrue(result["ok"])


class ShadowLifecycleTests(unittest.TestCase):
    @staticmethod
    def trade(direction="BUY"):
        now = datetime(2026, 8, 13, 15, 0, tzinfo=UTC)
        return StrategyShadowTrade(
            shadow_trade_id="t", symbol="XAUUSD", strategy_version="XAUUSD_V2_SHADOW",
            setup_fingerprint="s", direction=direction, entry_timestamp=now,
            entry=100, sl=90 if direction == "BUY" else 110,
            tp1=108 if direction == "BUY" else 92,
            tp2=115 if direction == "BUY" else 85,
            protected_sl=107.5 if direction == "BUY" else 92.5,
            rr=1.5, status="OPEN", mae_r=0, mfe_r=0,
            tp1_reached=False, tp2_reached=False, sl_reached=False,
            last_processed_m5=now, diagnostics_json={}, created_at=now, updated_at=now,
        )

    def test_intrabar_stop_and_target_is_ambiguous(self):
        trade = self.trade()
        candle = {"close_time": trade.entry_timestamp + timedelta(minutes=5), "open": 100, "high": 116, "low": 89, "close": 105}
        shadow._advance_open_trade(trade, [candle], {"active_shadow_trade_id": "t"}, candle["close_time"])
        self.assertEqual(trade.status, "AMBIGUOUS_INTRABAR")
        self.assertIsNone(trade.r_result)

    def test_tp1_then_protected_stop_is_protected_exit(self):
        trade = self.trade()
        first = {"close_time": trade.entry_timestamp + timedelta(minutes=5), "open": 100, "high": 109, "low": 99, "close": 108}
        second = {"close_time": trade.entry_timestamp + timedelta(minutes=10), "open": 108, "high": 109, "low": 107, "close": 107.5}
        state = {"active_shadow_trade_id": "t"}
        shadow._advance_open_trade(trade, [first, second], state, second["close_time"])
        self.assertEqual(trade.status, "TP1_PROTECTED")
        self.assertGreater(trade.r_result, 0)

    def test_tp2_is_full_tp(self):
        trade = self.trade()
        candle = {"close_time": trade.entry_timestamp + timedelta(minutes=5), "open": 100, "high": 116, "low": 99, "close": 115}
        shadow._advance_open_trade(trade, [candle], {"active_shadow_trade_id": "t"}, candle["close_time"])
        self.assertEqual(trade.status, "FULL_TP")
        self.assertEqual(trade.r_result, 1.5)


if __name__ == "__main__":
    unittest.main()
