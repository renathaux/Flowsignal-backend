import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import api
from models import Base, RuntimeSetting
from services import strategy_settings_service as settings_service
from strategies import shared, strict_trader


class StrategySettingsExecutionCacheTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        settings_service.invalidate_execution_settings_cache(self.sessions)

    def tearDown(self):
        settings_service.invalidate_execution_settings_cache(self.sessions)
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def test_missing_rows_use_exact_production_defaults(self):
        self.assertEqual(
            settings_service.get_cached_execution_settings(
                self.sessions, force_refresh=True
            ),
            {
                "minimum_rr": 1.2,
                "maximum_rr": 2.0,
                "post_trade_cooldown_minutes": 15,
            },
        )

    def test_malformed_or_unavailable_store_uses_complete_safe_defaults(self):
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        with self.sessions() as session:
            session.add(RuntimeSetting(
                setting_name="strategy.minimum_rr",
                setting_value="not-json",
                updated_at=now,
                updated_by="test",
            ))
            session.add(RuntimeSetting(
                setting_name="strategy.maximum_rr",
                setting_value="3.0",
                updated_at=now,
                updated_by="test",
            ))
            session.commit()
        self.assertEqual(
            settings_service.get_cached_execution_settings(
                self.sessions, force_refresh=True
            ),
            settings_service.execution_defaults(),
        )

        def unavailable_factory():
            raise RuntimeError("database unavailable")

        settings_service.invalidate_execution_settings_cache(unavailable_factory)
        self.assertEqual(
            settings_service.get_cached_execution_settings(
                unavailable_factory, force_refresh=True
            ),
            settings_service.execution_defaults(),
        )

    def test_cache_avoids_cycle_reads_and_save_reset_refresh_it(self):
        with patch.object(
            settings_service,
            "_read_execution_settings",
            wraps=settings_service._read_execution_settings,
        ) as reader:
            first = settings_service.get_cached_execution_settings(
                self.sessions, force_refresh=True
            )
            second = settings_service.get_cached_execution_settings(self.sessions)
            self.assertEqual(first, second)
            self.assertEqual(reader.call_count, 1)

            settings_service.save_strategy_settings(
                {"minimum_rr": 1.4},
                updated_by="owner",
                session_factory=self.sessions,
            )
            self.assertEqual(
                settings_service.get_cached_execution_settings(self.sessions)["minimum_rr"],
                1.4,
            )
            self.assertEqual(reader.call_count, 1)

            settings_service.reset_strategy_settings(
                confirmed=True,
                updated_by="owner",
                session_factory=self.sessions,
            )
            self.assertEqual(
                settings_service.get_cached_execution_settings(self.sessions),
                settings_service.execution_defaults(),
            )
            self.assertEqual(reader.call_count, 1)

    def test_cache_refreshes_after_thirty_second_ttl(self):
        settings_service.invalidate_execution_settings_cache(self.sessions)
        with patch.object(
            settings_service,
            "_read_execution_settings",
            wraps=settings_service._read_execution_settings,
        ) as reader:
            settings_service.get_cached_execution_settings(
                self.sessions, force_refresh=True, monotonic_now=0
            )
            settings_service.get_cached_execution_settings(
                self.sessions, monotonic_now=29
            )
            self.assertEqual(reader.call_count, 1)
            settings_service.get_cached_execution_settings(
                self.sessions, monotonic_now=31
            )
            self.assertEqual(reader.call_count, 2)

    def test_concurrent_cache_miss_cannot_overwrite_completed_save(self):
        started = threading.Event()
        release = threading.Event()

        def slow_read(_factory):
            started.set()
            self.assertTrue(release.wait(timeout=3))
            return settings_service.execution_defaults()

        settings_service.invalidate_execution_settings_cache(self.sessions)
        with patch.object(settings_service, "_read_execution_settings", side_effect=slow_read):
            with ThreadPoolExecutor(max_workers=2) as executor:
                reader = executor.submit(
                    settings_service.get_cached_execution_settings,
                    self.sessions,
                    force_refresh=True,
                )
                self.assertTrue(started.wait(timeout=3))
                writer = executor.submit(
                    settings_service.save_strategy_settings,
                    {"minimum_rr": 1.4},
                    "owner",
                    self.sessions,
                )
                release.set()
                self.assertEqual(reader.result(timeout=3)["minimum_rr"], 1.2)
                self.assertEqual(writer.result(timeout=3)["current"]["minimum_rr"], 1.4)

        self.assertEqual(
            settings_service.get_cached_execution_settings(self.sessions)["minimum_rr"],
            1.4,
        )


class StrategySettingsRRWiringTests(unittest.TestCase):
    @staticmethod
    def swing(swing_type, price):
        return {"type": swing_type, "price": price, "time": "2026-08-11T12:00:00Z"}

    def test_default_buy_sell_and_wait_parity(self):
        with patch.object(strict_trader, "get_configured_rr_window", return_value=(1.2, 2.0)):
            default_results = [
                strict_trader.select_tp2(
                    [self.swing(swing_type, price)], side, 100.0, 10.0, "XAUUSD"
                )
                for side, swing_type, price in (
                    ("BUY", "HIGH", 111.0),
                    ("BUY", "HIGH", 112.0),
                    ("BUY", "HIGH", 125.0),
                    ("SELL", "LOW", 89.0),
                    ("SELL", "LOW", 88.0),
                    ("SELL", "LOW", 75.0),
                )
            ]
            wait_default = strict_trader.get_mtf_signal(
                pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "XAUUSD"
            )
        explicit_results = [
            strict_trader.select_tp2(
                [self.swing(swing_type, price)], side, 100.0, 10.0,
                "XAUUSD", 1.2, 2.0,
            )
            for side, swing_type, price in (
                ("BUY", "HIGH", 111.0),
                ("BUY", "HIGH", 112.0),
                ("BUY", "HIGH", 125.0),
                ("SELL", "LOW", 89.0),
                ("SELL", "LOW", 88.0),
                ("SELL", "LOW", 75.0),
            )
        ]
        self.assertEqual(default_results, explicit_results)
        self.assertEqual(
            [result["rr"] for result in default_results],
            [2.0, 1.2, 2.0, 2.0, 1.2, 2.0],
        )
        self.assertEqual(wait_default["signal"], "WAIT")

    def test_minimum_rr_values_change_only_acceptance_threshold(self):
        candidate = [self.swing("HIGH", 113.0)]
        expected = {
            1.0: (1.3, "inverse_15m_swing"),
            1.4: (2.0, "fallback_2r"),
            2.0: (2.0, "fallback_2r"),
        }
        for minimum_rr, (expected_rr, source) in expected.items():
            with self.subTest(minimum_rr=minimum_rr):
                result = strict_trader.select_tp2(
                    candidate, "BUY", 100.0, 10.0, "XAUUSD",
                    minimum_rr, 2.0,
                )
                self.assertAlmostEqual(result["rr"], expected_rr)
                self.assertEqual(result["source"], source)

    def test_maximum_rr_values_change_only_existing_cap(self):
        candidate = [self.swing("HIGH", 125.0)]
        expected = {
            1.5: (1.5, "fallback_1.5r"),
            2.0: (2.0, "fallback_2r"),
            3.0: (2.5, "inverse_15m_swing"),
        }
        for maximum_rr, (expected_rr, source) in expected.items():
            with self.subTest(maximum_rr=maximum_rr):
                result = strict_trader.select_tp2(
                    candidate, "BUY", 100.0, 10.0, "XAUUSD",
                    1.0, maximum_rr,
                )
                self.assertAlmostEqual(result["rr"], expected_rr)
                self.assertEqual(result["source"], source)

    def test_final_live_rr_gate_uses_same_window(self):
        cases = [
            ((1.2, 2.0), 1.2, True),
            ((1.2, 2.0), 1.1, False),
            ((1.2, 2.0), 2.1, False),
            ((1.0, 2.0), 1.1, True),
            ((1.4, 2.0), 1.3, False),
            ((1.0, 1.5), 2.0, False),
            ((1.0, 3.0), 2.5, True),
        ]
        for rr_window, rr, expected_ok in cases:
            with self.subTest(rr_window=rr_window, rr=rr):
                with patch.object(api, "get_configured_rr_window", return_value=rr_window):
                    result = api.validate_live_trade_risk_reward(
                        "XAUUSD", "BUY", 100.0, 90.0, 100.0 + (10.0 * rr)
                    )
                self.assertEqual(result["ok"], expected_ok)


class StrategySettingsCooldownWiringTests(unittest.TestCase):
    def build_live_payload(self):
        identity = {
            "symbol": "XAUUSD",
            "direction": "BUY",
            "swing_type": "HIGH",
            "swing_timestamp": "1970-01-01T00:17:30Z",
            "swing_price": 101.0,
            "bos_candle_timestamp": "1970-01-01T00:18:20Z",
            "bos_level": 101.0,
            "confirmation_timestamp": "1970-01-01T00:19:10Z",
        }
        return {
            "signal_setup_id": "stable-id",
            "fifteen_m_break_close_time": "1970-01-01T00:18:20Z",
            "five_m_confirmation_close_time": "1970-01-01T00:19:10Z",
            "trend_15m": {"trend": "BULLISH", "buy_allowed": True},
            "setup_identity": identity,
        }

    def test_live_and_paper_use_same_zero_five_fifteen_thirty_minute_setting(self):
        age_seconds = 600.0
        expected_blocked = {0: False, 5: False, 15: True, 30: True}
        payload = self.build_live_payload()
        for minutes, blocked in expected_blocked.items():
            seconds = minutes * 60
            with self.subTest(minutes=minutes):
                with patch.object(shared, "PAPER_TRADE_HISTORY", [{"symbol": "XAUUSD", "side": "BUY"}]), patch.object(
                    shared, "paper_trade_age_seconds", return_value=age_seconds
                ), patch.object(
                    shared, "get_configured_cooldown_seconds", return_value=seconds
                ):
                    paper_blocked, paper_age = shared.is_paper_reentry_cooling_down(
                        "XAUUSD", "BUY"
                    )
                self.assertEqual(paper_age, age_seconds)
                self.assertEqual(paper_blocked, blocked)

                with patch.dict(api.LIVE_LAST_POSITION_CLOSED_AT, {"XAUUSD": 1000.0}), patch.dict(
                    api.LIVE_ACTIVE_ORDERS, {"XAUUSD": None}
                ), patch.object(
                    api, "get_signal_setup_id", return_value="stable-id"
                ), patch.object(
                    api, "get_live_post_close_cooldown_seconds", return_value=seconds
                ):
                    live = api.validate_auto_entry_state_locked(
                        "XAUUSD", "BUY", payload, broker_positions=[], now=1600.0
                    )
                self.assertEqual(live["reason"] == "post-close cooldown active", blocked)
                self.assertEqual(live["details"]["post_close_cooldown_seconds"], seconds)


if __name__ == "__main__":
    unittest.main()
