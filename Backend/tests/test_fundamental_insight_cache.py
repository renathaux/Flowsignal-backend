import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fundamentals.insight_cache import (
    _reset_for_tests,
    get_or_calculate,
)
from fundamentals.repositories.economic_events import persist_calendar_batch
from models import Base
from routes import fundamentals as fundamentals_route


NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


class FundamentalInsightCacheTests(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()

    def tearDown(self):
        _reset_for_tests()

    def test_eurusd_cache_hit(self):
        calls = []

        def calculate():
            calls.append("EURUSD")
            return {"symbol": "EURUSD", "value": len(calls)}

        first = get_or_calculate("EURUSD", calculate)
        second = get_or_calculate("EURUSD", calculate)
        self.assertEqual(first, second)
        self.assertEqual(calls, ["EURUSD"])

    def test_xauusd_cache_hit(self):
        calls = []

        def calculate():
            calls.append("XAUUSD")
            return {"symbol": "XAUUSD", "value": len(calls)}

        self.assertEqual(
            get_or_calculate("XAUUSD", calculate),
            get_or_calculate("XAUUSD", calculate),
        )
        self.assertEqual(calls, ["XAUUSD"])

    def test_symbol_caches_are_isolated_and_cannot_leak(self):
        eur = get_or_calculate("EURUSD", lambda: {"symbol": "EURUSD"})
        xau = get_or_calculate("XAUUSD", lambda: {"symbol": "XAUUSD"})
        self.assertEqual(eur["symbol"], "EURUSD")
        self.assertEqual(xau["symbol"], "XAUUSD")
        self.assertEqual(
            get_or_calculate("EURUSD", lambda: self.fail("EUR cache missed"))["symbol"],
            "EURUSD",
        )
        self.assertEqual(
            get_or_calculate("XAUUSD", lambda: self.fail("XAU cache missed"))["symbol"],
            "XAUUSD",
        )

    def test_ttl_expiry_recalculates(self):
        current = [100.0]
        calls = []

        def calculate():
            calls.append(len(calls) + 1)
            return {"value": calls[-1]}

        first = get_or_calculate(
            "EURUSD", calculate, ttl_seconds=180, monotonic=lambda: current[0]
        )
        current[0] = 279.9
        self.assertEqual(
            get_or_calculate(
                "EURUSD", calculate, ttl_seconds=180, monotonic=lambda: current[0]
            ),
            first,
        )
        current[0] = 280.0
        self.assertEqual(
            get_or_calculate(
                "EURUSD", calculate, ttl_seconds=180, monotonic=lambda: current[0]
            )["value"],
            2,
        )

    def test_manual_refresh_bypasses_and_revalidates_cache(self):
        responses = [
            {"symbol": "EURUSD", "sequence": 1},
            {"symbol": "EURUSD", "sequence": 2},
        ]
        with patch.object(
            fundamentals_route,
            "get_fundamental_insight",
            side_effect=responses,
        ) as insight:
            first = fundamentals_route.fundamental_insight("EURUSD", refresh=False)
            cached = fundamentals_route.fundamental_insight("EURUSD", refresh=False)
            refreshed = fundamentals_route.fundamental_insight("EURUSD", refresh=True)
            cached_after_refresh = fundamentals_route.fundamental_insight(
                "EURUSD", refresh=False
            )
        self.assertEqual(first, cached)
        self.assertEqual(refreshed, cached_after_refresh)
        self.assertNotEqual(first, refreshed)
        self.assertEqual(insight.call_count, 2)
        insight.assert_called_with("EURUSD", persist=False)

    def test_ingestion_change_invalidates_cached_insights(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        sessions = sessionmaker(bind=engine)
        try:
            calls = {"EURUSD": 0, "XAUUSD": 0}
            get_or_calculate(
                "EURUSD",
                lambda: calls.__setitem__("EURUSD", calls["EURUSD"] + 1)
                or {"symbol": "EURUSD", "sequence": calls["EURUSD"]},
            )
            get_or_calculate(
                "XAUUSD",
                lambda: calls.__setitem__("XAUUSD", calls["XAUUSD"] + 1)
                or {"symbol": "XAUUSD", "sequence": calls["XAUUSD"]},
            )
            raw = [{
                "event_name": "US CPI",
                "currency": "USD",
                "impact": "HIGH",
                "release_time": "2026-08-11T12:30:00Z",
                "actual": "3.0%",
                "forecast": "2.9%",
                "previous": "2.8%",
                "source": "bls",
            }]
            result = persist_calendar_batch(
                "bls", raw, raw, completed_at=NOW, session_factory=sessions
            )
            self.assertEqual(result["observations_added"], 1)
            refreshed = get_or_calculate(
                "EURUSD",
                lambda: calls.__setitem__("EURUSD", calls["EURUSD"] + 1)
                or {"symbol": "EURUSD", "sequence": calls["EURUSD"]},
            )
            refreshed_xau = get_or_calculate(
                "XAUUSD",
                lambda: calls.__setitem__("XAUUSD", calls["XAUUSD"] + 1)
                or {"symbol": "XAUUSD", "sequence": calls["XAUUSD"]},
            )
            self.assertEqual(refreshed["sequence"], 2)
            self.assertEqual(refreshed_xau["sequence"], 2)
            self.assertEqual(calls, {"EURUSD": 2, "XAUUSD": 2})
        finally:
            engine.dispose()

    def test_cache_invalidation_failure_does_not_fail_committed_ingestion(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        sessions = sessionmaker(bind=engine)
        try:
            raw = [{
                "event_name": "US CPI",
                "currency": "USD",
                "impact": "HIGH",
                "release_time": "2026-08-11T12:30:00Z",
                "actual": "3.0%",
                "forecast": "2.9%",
                "previous": "2.8%",
                "source": "bls",
            }]
            with patch(
                "fundamentals.insight_cache.invalidate",
                side_effect=RuntimeError("cache unavailable"),
            ):
                with self.assertLogs(
                    "fundamentals.repositories.economic_events", level="ERROR"
                ):
                    result = persist_calendar_batch(
                        "bls", raw, raw, completed_at=NOW,
                        session_factory=sessions,
                    )
            self.assertEqual(result["observations_added"], 1)
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
