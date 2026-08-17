import unittest

from services.v2_risk_alignment import build_v2_risk_plan


class V2RiskAlignmentTests(unittest.TestCase):
    def test_xauusd_100_points_and_1_5r(self):
        plan = build_v2_risk_plan(
            "XAUUSD",
            "BUY",
            2400.00,
            2399.00,
            1.50,
            1.50,
            source_rr=2.00,
            minimum_sl_points=100,
        )
        self.assertTrue(plan["ok"])
        self.assertAlmostEqual(plan["sl_distance_points"], 100.0)
        self.assertAlmostEqual(plan["rr"], 1.50)
        self.assertAlmostEqual(plan["tp2"], 2401.50)

    def test_xauusd_99_points_is_blocked(self):
        plan = build_v2_risk_plan(
            "XAUUSD",
            "BUY",
            2400.00,
            2399.01,
            1.50,
            1.50,
            minimum_sl_points=100,
        )
        self.assertFalse(plan["ok"])
        self.assertEqual(plan["reason"], "V2_SL_UNDER_MINIMUM_POINTS")

    def test_eurusd_sell_uses_own_entry_for_1_5r(self):
        # 0.00100 / 0.00001 = 100 points. Reward must be 0.00150.
        plan = build_v2_risk_plan(
            "EURUSD",
            "SELL",
            1.17000,
            1.17100,
            1.50,
            1.50,
            source_rr=1.20,
            minimum_sl_points=100,
        )
        self.assertTrue(plan["ok"])
        self.assertAlmostEqual(plan["sl_distance_points"], 100.0)
        self.assertAlmostEqual(plan["rr"], 1.50)
        self.assertAlmostEqual(plan["tp2"], 1.16850)

    def test_wrong_side_sl_is_blocked(self):
        plan = build_v2_risk_plan(
            "EURUSD",
            "BUY",
            1.17000,
            1.17100,
            1.50,
            1.50,
            minimum_sl_points=100,
        )
        self.assertFalse(plan["ok"])
        self.assertEqual(plan["reason"], "V2_SL_WRONG_SIDE")

    def test_alignment_module_has_no_execution_imports(self):
        import services.v2_risk_alignment as module

        text = open(module.__file__, "r", encoding="utf-8").read().lower()
        forbidden = [
            "place_market_order",
            "close_position",
            "ctrader_connector",
            "execute_live_order",
            "live_auto",
            "paper_auto",
        ]
        for token in forbidden:
            self.assertNotIn(token, text)


if __name__ == "__main__":
    unittest.main()
