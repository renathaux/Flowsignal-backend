import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import create_engine, event as sqlalchemy_event
from sqlalchemy.orm import sessionmaker

from fundamentals.insight_service import (
    _history_lookup_from_observations,
    get_fundamental_insight,
)
from fundamentals.gold_insight_service import get_xauusd_fundamental_insight
from fundamentals.repositories.observations import provider_health
from models import (
    Base,
    EconomicEvent,
    EconomicEventObservation,
    EconomicProviderFetch,
)
from routes import fundamentals as fundamentals_route


NOW = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)


class FundamentalInsightReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed_history(self, count=240):
        with self.sessions() as session:
            for index in range(count):
                currency = "USD" if index % 2 else "EUR"
                released = NOW - timedelta(days=index + 1)
                economic_event = EconomicEvent(
                    event_id=f"macro-{index}",
                    event_name=f"{currency} CPI",
                    indicator="cpi_y_y",
                    country="United States" if currency == "USD" else "Euro Area",
                    currency=currency,
                    impact="HIGH",
                    release_time=released,
                    provider="bls" if currency == "USD" else "eurostat",
                    provider_event_id=f"provider-{index}",
                    data_status="RELEASED",
                    first_seen_at=released,
                    last_seen_at=released,
                )
                session.add(economic_event)
                session.flush()
                session.add(EconomicEventObservation(
                    observation_hash=f"{index:064x}",
                    economic_event_id=economic_event.id,
                    actual=str(2.0 + index / 1000),
                    forecast="2.0",
                    previous="1.9",
                    provider=economic_event.provider,
                    provider_timestamp=released,
                    fetched_at=released,
                    data_status="RELEASED",
                    raw_payload={},
                ))
            session.add(EconomicProviderFetch(
                provider="bls",
                started_at=NOW - timedelta(minutes=2),
                completed_at=NOW - timedelta(minutes=1),
                status="SUCCESS",
                raw_event_count=count,
                normalized_event_count=count,
                error=None,
            ))
            session.commit()

    def test_request_path_has_bounded_select_count_and_no_provider_call(self):
        self._seed_history()
        statements = []

        def capture(_connection, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        sqlalchemy_event.listen(self.engine, "before_cursor_execute", capture)
        started = time.perf_counter()
        try:
            with patch("services.news_service.fetch_calendar_events") as provider_fetch:
                response = get_fundamental_insight(
                    "EURUSD",
                    now=NOW,
                    session_factory=self.sessions,
                    persist=False,
                    ingest=False,
                )
        finally:
            sqlalchemy_event.remove(self.engine, "before_cursor_execute", capture)

        self.assertEqual(response["symbol"], "EURUSD")
        self.assertLessEqual(len(statements), 9)
        self.assertLess(time.perf_counter() - started, 2.0)
        provider_fetch.assert_not_called()

    def test_in_memory_history_is_series_scoped_and_strictly_earlier(self):
        observations = [
            {
                "event_id": f"usd-{index}",
                "currency": "USD",
                "indicator": "cpi_y_y",
                "actual": str(2 + index),
                "forecast": "2",
                "release_time": NOW - timedelta(days=3 - index),
            }
            for index in range(3)
        ] + [{
            "event_id": "eur-1",
            "currency": "EUR",
            "indicator": "cpi_y_y",
            "actual": "2.1",
            "forecast": "2",
            "release_time": NOW - timedelta(days=4),
        }]
        lookup = _history_lookup_from_observations(observations)
        history = lookup(observations[2])
        self.assertEqual([item["event_id"] for item in history], ["usd-0", "usd-1"])

    def test_health_keeps_official_provider_visible_beyond_latest_100_rows(self):
        with self.sessions() as session:
            session.add(EconomicProviderFetch(
                provider="bls", started_at=NOW - timedelta(days=1),
                completed_at=NOW - timedelta(days=1), status="SUCCESS",
                raw_event_count=6, normalized_event_count=6,
            ))
            for index in range(101):
                completed = NOW - timedelta(minutes=index)
                session.add(EconomicProviderFetch(
                    provider="fmp", started_at=completed - timedelta(seconds=1),
                    completed_at=completed, status="FAILED",
                    raw_event_count=0, normalized_event_count=0, error="HTTP 402",
                ))
            session.commit()
        result = provider_health(now=NOW, session_factory=self.sessions)
        self.assertIn("bls", result["providers"])
        self.assertEqual(result["providers"]["bls"]["status"], "SUCCESS")

    def test_route_disables_snapshot_writes(self):
        expected = {"symbol": "EURUSD", "read_only": True}
        with patch.object(
            fundamentals_route, "get_fundamental_insight", return_value=expected
        ) as insight:
            self.assertEqual(fundamentals_route.fundamental_insight("EURUSD"), expected)
        insight.assert_called_once_with("EURUSD", persist=False)

    def test_xauusd_does_not_promote_stale_inflation_or_employment(self):
        stale_time = NOW - timedelta(days=70)
        observations = [
            {
                "event_id": "old-cpi", "event_name": "US CPI y/y",
                "indicator": "cpi_y_y", "currency": "USD",
                "actual": "3.5", "forecast": "3.4", "previous": "3.3",
                "revised_previous": None, "release_time": stale_time,
                "impact": "HIGH", "provider": "bls", "data_status": "RELEASED",
            },
            {
                "event_id": "old-nfp", "event_name": "US Non-Farm Payrolls",
                "indicator": "nonfarm_payrolls", "currency": "USD",
                "actual": "150", "forecast": "140", "previous": "130",
                "revised_previous": None, "release_time": stale_time,
                "impact": "HIGH", "provider": "bls", "data_status": "RELEASED",
            },
        ]
        with patch(
            "fundamentals.gold_insight_service.provider_health",
            return_value={"observation_count": len(observations), "providers": {}},
        ):
            response = get_xauusd_fundamental_insight(
                now=NOW, observations=observations, next_event={}
            )
        self.assertEqual(response["drivers"]["inflation"]["status"], "STALE")
        self.assertEqual(response["drivers"]["employment"]["status"], "STALE")
        self.assertEqual(response["overall_bias"]["status"], "INSUFFICIENT_DATA")

    def test_eurusd_xauusd_route_isolation(self):
        eur = {"symbol": "EURUSD"}
        xau = {"symbol": "XAUUSD"}
        with patch.object(
            fundamentals_route, "get_fundamental_insight", return_value=eur
        ), patch.object(
            fundamentals_route, "get_xauusd_fundamental_insight", return_value=xau
        ):
            self.assertEqual(fundamentals_route.fundamental_insight("EURUSD"), eur)
            self.assertEqual(fundamentals_route.fundamental_insight("XAUUSD"), xau)


if __name__ == "__main__":
    unittest.main()
