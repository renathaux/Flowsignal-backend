import unittest

import pandas as pd

from indicators.smc.engine import analyze_structure, detect_confirmed_swings


class SmcIndicatorEngineTests(unittest.TestCase):
    def frame(self, rows):
        index = pd.date_range("2026-08-17T00:00:00Z", periods=len(rows), freq="5min")
        return pd.DataFrame(rows, index=index, columns=["Open", "High", "Low", "Close"])

    def test_swing_confirmation_does_not_look_ahead(self):
        data = self.frame([
            (10, 11, 9, 10),
            (10, 12, 9.5, 11),
            (11, 15, 10, 14),
            (14, 13, 10.5, 12),
            (12, 12.5, 10.8, 11.5),
        ])
        swings = detect_confirmed_swings(data, left_bars=2, right_bars=2)
        highs = [item for item in swings if item.swing_type == "HIGH"]
        self.assertEqual(len(highs), 1)
        self.assertEqual(highs[0].index, 2)
        self.assertEqual(highs[0].confirmed_index, 4)

    def test_bullish_break_is_bos_from_neutral(self):
        data = self.frame([
            (10, 11, 9.0, 10.0),
            (10, 12, 9.5, 11.0),
            (11, 15, 10.0, 14.0),
            (14, 13, 10.5, 12.0),
            (12, 12.5, 10.8, 11.5),
            (11.5, 14.0, 11.0, 13.5),
            (13.5, 16.0, 13.0, 15.5),
        ])
        result = analyze_structure(data, left_bars=2, right_bars=2)
        bullish = [event for event in result["events"] if event["direction"] == "BULLISH"]
        self.assertTrue(bullish)
        self.assertEqual(bullish[-1]["event_type"], "BOS")
        self.assertEqual(result["bias"], "BULLISH")

    def test_opposite_break_becomes_choch(self):
        data = self.frame([
            (10, 11, 9.0, 10.0),
            (10, 12, 9.5, 11.0),
            (11, 15, 10.0, 14.0),
            (14, 13, 10.5, 12.0),
            (12, 12.5, 10.8, 11.5),
            (11.5, 14.0, 11.0, 13.5),
            (13.5, 16.0, 13.0, 15.5),
            (15.5, 15.8, 12.0, 13.0),
            (13.0, 13.5, 10.0, 10.5),
            (10.5, 11.0, 8.5, 9.0),
        ])
        result = analyze_structure(data, left_bars=2, right_bars=2)
        bearish = [event for event in result["events"] if event["direction"] == "BEARISH"]
        if bearish:
            self.assertIn(bearish[-1]["event_type"], {"BOS", "CHOCH"})

    def test_module_is_analysis_only(self):
        import indicators.smc.engine as engine

        module_text = open(engine.__file__, "r", encoding="utf-8").read().lower()
        forbidden = ["place_market_order", "close_position", "live_auto", "paper_auto"]
        for token in forbidden:
            self.assertNotIn(token, module_text)


if __name__ == "__main__":
    unittest.main()
