import unittest

import pandas as pd

from strategies.xauusd_risk_guard import _choose_5m_stop


class XauusdRiskGuardTests(unittest.TestCase):
    def frame(self, rows):
        index = pd.date_range("2026-08-17T00:00:00Z", periods=len(rows), freq="5min")
        return pd.DataFrame(rows, index=index, columns=["Open", "High", "Low", "Close"])

    def test_buy_stop_is_below_confirmed_5m_swing_not_fixed_100_points(self):
        data = self.frame([
            (4650, 4652, 4648, 4651),
            (4651, 4653, 4646, 4652),
            (4652, 4654, 4640, 4645),
            (4645, 4650, 4643, 4648),
            (4648, 4655, 4644, 4654),
            (4654, 4658, 4651, 4657),
            (4657, 4660, 4653, 4659),
        ])
        result = _choose_5m_stop(data, "BUY", 4659.0, 100)
        self.assertTrue(result["ok"])
        self.assertEqual(result["sl_structure_source"], "confirmed_5m_swing")
        self.assertAlmostEqual(result["swing"]["price"], 4640.0)
        self.assertAlmostEqual(result["stop_loss"], 4639.5)
        self.assertGreater(result["distance_points"], 100)

    def test_too_close_swing_is_rejected_instead_of_manufacturing_minimum_stop(self):
        data = self.frame([
            (4650.0, 4651.0, 4648.0, 4650.5),
            (4650.5, 4652.0, 4649.0, 4651.0),
            (4651.0, 4652.0, 4650.2, 4651.5),
            (4651.5, 4652.5, 4651.0, 4652.0),
            (4652.0, 4653.0, 4651.2, 4652.5),
        ])
        result = _choose_5m_stop(data, "BUY", 4652.0, 100)
        self.assertFalse(result["ok"])
        self.assertIn(result["reason"], {
            "WAIT_NO_CONFIRMED_5M_SWING_SL",
            "WAIT_NO_SAFE_5M_SWING_SL",
        })
        self.assertNotIn("stop_loss", result)


if __name__ == "__main__":
    unittest.main()
