import unittest
from unittest.mock import Mock, patch

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
                "protected_low": {
                    "type": "LOW",
                    "price": 4648.0,
                    "timestamp": "2026-08-17T01:30:00+00:00",
                },
                "high": 4665.0,
                "high_start_timestamp": "2026-08-17T02:00:00+00:00",
            },
        }
        result = _protected_15m_stop(self.frame(), "BUY", 4649.0, 100)
        self.assertTrue(result["ok"])
        self.assertEqual(result["sl_structure_source"], "protected_15m_structure")
        self.assertAlmostEqual(result["swing"]["price"], 4648.0)
        self.assertAlmostEqual(result["stop_loss"], 4647.5)
        self.assertAlmostEqual(result["buffer_pips"], 5.0)
        # 10 Gold pips (1.00) from entry to swing + 5 pips (0.50) = 1.50.
        self.assertAlmostEqual(result["distance"], 1.5)
        self.assertAlmostEqual(result["distance_points"], 150.0)

    @patch("strategies.xauusd_risk_guard.analyze_structure")
    def test_sell_stop_is_five_pips_above_protected_15m_swing(self, analyze):
        analyze.return_value = {
            "bias": "BEARISH",
            "current_structure": {
                "low": 4635.0,
                "low_start_timestamp": "2026-08-17T01:30:00+00:00",
                "high": 4650.0,
                "high_start_timestamp": "2026-08-17T02:00:00+00:00",
                "protected_high": {
                    "type": "HIGH",
                    "price": 4650.0,
                    "timestamp": "2026-08-17T02:00:00+00:00",
                },
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
                "protected_low": {
                    "type": "LOW",
                    "price": 4657.8,
                    "timestamp": "2026-08-17T01:30:00+00:00",
                },
                "high": 4660.0,
                "high_start_timestamp": "2026-08-17T02:00:00+00:00",
            },
        }
        result = _protected_15m_stop(self.frame(), "BUY", 4658.0, 100)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "WAIT_SL_TOO_SMALL")
        self.assertNotIn("stop_loss", result)

    @patch("strategies.xauusd_risk_guard.analyze_structure")
    def test_generic_frame_extreme_is_not_accepted_without_protected_swing(self, analyze):
        analyze.return_value = {
            "bias": "BULLISH",
            "current_structure": {
                "low": 4640.0,
                "low_start_timestamp": "2026-08-17T00:00:00+00:00",
                "protected_low": None,
            },
        }
        result = _protected_15m_stop(self.frame(), "BUY", 4658.0, 100)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "WAIT_NO_PROTECTED_15M_SWING_SL")
        self.assertNotIn("stop_loss", result)

    @patch("strategies.xauusd_risk_guard.analyze_structure")
    def test_stop_analysis_receives_only_the_supplied_15m_frame(self, analyze):
        frame_15m = self.frame()
        analyze.return_value = {
            "bias": "BULLISH",
            "current_structure": {
                "protected_low": {
                    "type": "LOW",
                    "price": 4648.0,
                    "timestamp": "2026-08-17T01:30:00+00:00",
                },
            },
        }
        result = _protected_15m_stop(frame_15m, "BUY", 4649.0, 100)
        self.assertTrue(result["ok"])
        analyze.assert_called_once_with(frame_15m)
        self.assertEqual(result["sl_structure_source"], "protected_15m_structure")

    def test_eurusd_risk_builder_remains_the_original_generic_path(self):
        import brain

        wrapper = brain.build_risk_levels_with_xauusd_15m
        original = Mock(return_value={"ok": True, "source": "generic_eurusd"})
        globals_dict = wrapper.__globals__
        with patch.dict(globals_dict, {"_original_build_risk_levels": original}):
            result = wrapper(
                self.frame(),
                "BUY",
                1.1000,
                "EURUSD",
                setup_break_time="2026-08-17T01:00:00Z",
                execution_settings={"minimum_sl_distance_points": 100},
            )
        self.assertEqual(result, {"ok": True, "source": "generic_eurusd"})
        original.assert_called_once()


if __name__ == "__main__":
    unittest.main()
