import unittest
from unittest.mock import patch

import pandas as pd

from strategies.xauusd_risk_guard import _protected_15m_stop


class XauusdRiskGuardTests(unittest.TestCase):
    def frame(self, count=12):
        index = pd.date_range("2026-08-17T00:00:00Z", periods=count, freq="15min")
        rows = [(4650.0, 4660.0, 4640.0, 4655.0) for _ in range(count)]
        return pd.DataFrame(rows, index=index, columns=["Open", "High", "Low", "Close"])

    @patch("strategies.xauusd_risk_guard.analyze_structure")
    def test_buy_stop_is_five_pips_below_protected_15m_swing(self, analyze):
        analyze.return_value = {
            "bias": "BULLISH",
            "current_structure": {
                "low": 4648.0,
                "low_start_timestamp": "2026-08-17T01:30:00+00:00",
                "high": 4665.0,
                "high_start_timestamp": "2026-08-17T02:00:00+00:00",
            },
        }
        result = _protected_15m_stop(self.frame(), "BUY", 4658.0, 100)
        self.assertTrue(result["ok"])
        self.assertEqual(result["sl_structure_source"], "protected_15m_structure")
        self.assertAlmostEqual(result["swing"]["price"], 4648.0)
        self.assertAlmostEqual(result["stop_loss"], 4647.5)
        self.assertAlmostEqual(result["buffer_pips"], 5.0)
        # 10.0 from entry to swing + 0.5 (5 Gold pips) beyond = 10.5 total.
        self.assertAlmostEqual(result["distance"], 10.5)

    @patch("strategies.xauusd_risk_guard.analyze_structure")
    def test_sell_stop_is_five_pips_above_protected_15m_swing(self, analyze):
        analyze.return_value = {
            "bias": "BEARISH",
            "current_structure": {
                "low": 4635.0,
                "low_start_timestamp": "2026-08-17T01:30:00+00:00",
                "high": 4650.0,
                "high_start_timestamp": "2026-08-17T02:00:00+00:00",
            },
        }
        result = _protected_15m_stop(self.frame(), "SELL", 4640.0, 100)
        self.assertTrue(result["ok"])
        self.assertAlmostEqual(result["swing"]["price"], 4650.0)
        self.assertAlmostEqual(result["stop_loss"], 4650.5)
        self.assertAlmostEqual(result["buffer_pips"], 5.0)

    @patch("strategies.xauusd_risk_guard.analyze_structure")
    def test_minimum_distance_does_not_manufacture_a_stop(self, analyze):
        analyze.return_value = {
            "bias": "BULLISH",
            "current_structure": {
                "low": 4657.8,
                "low_start_timestamp": "2026-08-17T01:30:00+00:00",
                "high": 4660.0,
                "high_start_timestamp": "2026-08-17T02:00:00+00:00",
            },
        }
        result = _protected_15m_stop(self.frame(), "BUY", 4658.0, 100)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "WAIT_SL_TOO_SMALL")
        self.assertNotIn("stop_loss", result)


if __name__ == "__main__":
    unittest.main()
