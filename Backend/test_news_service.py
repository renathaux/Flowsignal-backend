import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import news_service


class NewsServiceTests(unittest.TestCase):
    def test_before_release_waits_for_actual_data(self):
        now = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        event = {
            "event": "USD Nonfarm Payrolls",
            "currency": "USD",
            "impact": "HIGH",
            "forecast": "180K",
            "actual": None,
            "previous": "170K",
            "time": now + timedelta(hours=1),
        }

        with (
            patch.object(news_service, "utc_now", return_value=now),
            patch.object(
                news_service,
                "fetch_jblanked_calendar_events",
                return_value=[event],
            ),
        ):
            result = news_service.get_news_impact("EURUSD")

        self.assertEqual(result["decision"], "WAITING_FOR_ACTUAL_DATA")
        self.assertEqual(result["news_bias"], "Waiting for release")
        self.assertEqual(result["effect"], "EURUSD Neutral")
        self.assertEqual(result["score"], 0)

    def test_released_usd_bullish_event_pressures_eurusd_sell(self):
        now = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        event = {
            "event": "USD Nonfarm Payrolls",
            "currency": "USD",
            "impact": "HIGH",
            "forecast": "180K",
            "actual": "240K",
            "previous": "170K",
            "time": now - timedelta(minutes=10),
        }

        with (
            patch.object(news_service, "utc_now", return_value=now),
            patch.object(
                news_service,
                "fetch_jblanked_calendar_events",
                return_value=[event],
            ),
        ):
            result = news_service.get_news_impact("EURUSD")

        self.assertEqual(result["news_bias"], "Bullish USD")
        self.assertEqual(result["effect"], "EURUSD SELL PRESSURE")
        self.assertEqual(result["score"], -15)

    def test_released_usd_bullish_event_pressures_xauusd_sell(self):
        now = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        event = {
            "event": "USD CPI",
            "currency": "USD",
            "impact": "HIGH",
            "forecast": "3.0%",
            "actual": "3.4%",
            "previous": "2.9%",
            "time": now - timedelta(minutes=5),
        }

        with (
            patch.object(news_service, "utc_now", return_value=now),
            patch.object(
                news_service,
                "fetch_jblanked_calendar_events",
                return_value=[event],
            ),
        ):
            result = news_service.get_news_impact("XAUUSD")

        self.assertEqual(result["news_bias"], "Bullish USD")
        self.assertEqual(result["effect"], "XAUUSD SELL PRESSURE")
        self.assertEqual(result["score"], -15)

    def test_lower_is_better_event_inverts_currency_bias(self):
        now = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        event = {
            "event": "USD Unemployment Rate",
            "currency": "USD",
            "impact": "HIGH",
            "forecast": "4.2%",
            "actual": "4.0%",
            "previous": "4.1%",
            "time": now - timedelta(minutes=5),
        }

        with (
            patch.object(news_service, "utc_now", return_value=now),
            patch.object(
                news_service,
                "fetch_jblanked_calendar_events",
                return_value=[event],
            ),
        ):
            result = news_service.get_news_impact("EURUSD")

        self.assertEqual(result["news_bias"], "Bullish USD")
        self.assertEqual(result["effect"], "EURUSD SELL PRESSURE")
        self.assertEqual(result["score"], -15)

    def test_api_failure_returns_safe_fallback(self):
        def failing_fetcher():
            raise RuntimeError("calendar offline")

        with patch.object(
            news_service,
            "fetch_jblanked_calendar_events",
            side_effect=failing_fetcher,
        ):
            result = news_service.get_news_impact("EURUSD")

        self.assertEqual(result["decision"], "NEWS_UNAVAILABLE")
        self.assertEqual(result["score"], 0)
        self.assertEqual(result["effect"], "EURUSD Neutral")

    def test_missing_api_key_returns_safe_fallback(self):
        with patch.dict("os.environ", {"JBLANKED_API_KEY": ""}, clear=False):
            result = news_service.get_news_impact("EURUSD")

        self.assertEqual(result["decision"], "NEWS_UNAVAILABLE")
        self.assertEqual(result["score"], 0)

    def test_no_relevant_event_returns_no_major_news(self):
        with patch.object(
            news_service,
            "fetch_jblanked_calendar_events",
            return_value=[
                {
                    "event": "GBP CPI",
                    "currency": "GBP",
                    "impact": "HIGH",
                    "forecast": None,
                    "actual": None,
                    "previous": None,
                    "release_time": None,
                    "time": None,
                }
            ],
        ):
            result = news_service.get_news_impact("XAUUSD")

        self.assertEqual(result["event"], "No major news now")
        self.assertEqual(result["decision"], "NO_MAJOR_NEWS")
        self.assertEqual(result["effect"], "XAUUSD Neutral")

    def test_symbol_filtering_prefers_eur_or_usd_for_eurusd(self):
        now = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        events = [
            {
                "event": "GBP CPI",
                "currency": "GBP",
                "impact": "HIGH",
                "forecast": None,
                "actual": None,
                "previous": None,
                "release_time": now + timedelta(minutes=5),
                "time": now + timedelta(minutes=5),
            },
            {
                "event": "EUR CPI",
                "currency": "EUR",
                "impact": "HIGH",
                "forecast": None,
                "actual": None,
                "previous": None,
                "release_time": now + timedelta(minutes=30),
                "time": now + timedelta(minutes=30),
            },
        ]

        with (
            patch.object(news_service, "utc_now", return_value=now),
            patch.object(
                news_service,
                "fetch_jblanked_calendar_events",
                return_value=events,
            ),
        ):
            result = news_service.get_news_impact("EURUSD")

        self.assertEqual(result["currency"], "EUR")

    def test_symbol_filtering_uses_usd_for_xauusd(self):
        now = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
        events = [
            {
                "event": "EUR CPI",
                "currency": "EUR",
                "impact": "HIGH",
                "forecast": None,
                "actual": None,
                "previous": None,
                "release_time": now + timedelta(minutes=5),
                "time": now + timedelta(minutes=5),
            },
            {
                "event": "USD GDP",
                "currency": "USD",
                "impact": "MEDIUM",
                "forecast": None,
                "actual": None,
                "previous": None,
                "release_time": now + timedelta(minutes=30),
                "time": now + timedelta(minutes=30),
            },
        ]

        with (
            patch.object(news_service, "utc_now", return_value=now),
            patch.object(
                news_service,
                "fetch_jblanked_calendar_events",
                return_value=events,
            ),
        ):
            result = news_service.get_news_impact("GOLD")

        self.assertEqual(result["symbol"], "XAUUSD")
        self.assertEqual(result["currency"], "USD")

    def test_jblanked_fetch_uses_api_key_header_and_cache(self):
        news_service._CALENDAR_CACHE["events"] = None
        news_service._CALENDAR_CACHE["fetched_at"] = 0

        class FakeResponse:
            status_code = 200
            text = '{"events":[]}'

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "events": [
                        {
                            "event": "USD GDP",
                            "currency": "USD",
                            "impact": "High",
                            "forecast": "2.0",
                            "previous": "1.8",
                            "release_time": "2026-06-26T12:30:00Z",
                        }
                    ]
                }

        with (
            patch.dict(
                "os.environ",
                {
                    "JBLANKED_API_KEY": "test-key",
                    "JBLANKED_NEWS_BASE_URL": "https://example.test/news/api",
                },
                clear=False,
            ),
            patch.object(
                news_service.requests,
                "get",
                return_value=FakeResponse(),
            ) as get_request,
        ):
            first = news_service.fetch_jblanked_calendar_events(force=True)
            second = news_service.fetch_jblanked_calendar_events()

        self.assertEqual(len(first), 1)
        self.assertEqual(second, first)
        get_request.assert_called_once()
        _, kwargs = get_request.call_args
        self.assertEqual(
            kwargs["headers"]["Authorization"],
            "Api-Key test-key",
        )

    def test_parser_accepts_capitalized_jblanked_fields(self):
        raw = {
            "Event": "USD Retail Sales",
            "Currency": "USD",
            "Impact": "High",
            "Forecast": "0.2%",
            "Actual": "0.4%",
            "Previous": "0.1%",
            "ReleaseTime": "2026-06-26T12:30:00Z",
        }

        event = news_service.normalize_event(raw)

        self.assertEqual(event["event"], "USD Retail Sales")
        self.assertEqual(event["currency"], "USD")
        self.assertEqual(event["impact"], "HIGH")
        self.assertEqual(event["actual"], "0.4%")
        self.assertIsNotNone(event["release_time"])

    def test_non_200_status_reports_auth_issue(self):
        news_service._CALENDAR_CACHE["events"] = None
        news_service._CALENDAR_CACHE["fetched_at"] = 0

        class FakeResponse:
            status_code = 403
            text = '{"detail":"forbidden"}'

            def raise_for_status(self):
                raise RuntimeError("HTTP 403")

            def json(self):
                return {}

        with (
            patch.dict("os.environ", {"JBLANKED_API_KEY": "test-key"}, clear=False),
            patch.object(
                news_service.requests,
                "get",
                return_value=FakeResponse(),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "auth issue"):
                news_service.fetch_jblanked_calendar_events(force=True)

    def test_manual_env_loader_reads_jblanked_values(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as env_file:
            env_file.write("JBLANKED_API_KEY=temp-key-1234\n")
            env_file.write("JBLANKED_NEWS_BASE_URL=https://example.test/news/api\n")
            env_path = env_file.name

        try:
            with (
                patch.object(news_service, "ENV_PATH", env_path),
                patch.dict(
                    "os.environ",
                    {
                        "JBLANKED_API_KEY": "",
                        "JBLANKED_NEWS_BASE_URL": "",
                    },
                    clear=False,
                ),
            ):
                news_service.load_news_env_file()
                self.assertEqual(
                    os.getenv("JBLANKED_API_KEY"),
                    "temp-key-1234",
                )
                self.assertEqual(
                    os.getenv("JBLANKED_NEWS_BASE_URL"),
                    "https://example.test/news/api",
                )
        finally:
            os.unlink(env_path)


if __name__ == "__main__":
    unittest.main()
