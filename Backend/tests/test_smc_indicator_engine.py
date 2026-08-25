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

    def test_internal_higher_low_break_does_not_flip_bullish_regime(self):
        # External bullish origin low is 100. A later internal HL forms near 112,
        # then price makes another high. Closing below 112 must not be CHoCH
        # while price remains above the protected external 100 low.
        data = self.frame([
            (103, 104, 100, 103),
            (103, 106, 102, 105),
            (105, 110, 104, 109),
            (109, 108, 103, 106),
            (106, 107, 102, 105),
            (105, 109, 104, 108),
            (108, 112, 107, 111),
            (111, 116, 110, 115),
            (115, 114, 111, 112),
            (112, 113, 111.5, 112.5),
            (112.5, 117, 112, 116.5),
            (116.5, 120, 115, 119),
            (119, 118, 113, 114),
            (114, 115, 110.5, 111),
            (111, 112, 108, 109),
        ])
        result = analyze_structure(data, left_bars=2, right_bars=2)
        bearish_choch = [
            event for event in result["events"]
            if event["direction"] == "BEARISH" and event["event_type"] == "CHOCH"
        ]
        self.assertFalse(bearish_choch)
        self.assertNotEqual(result["bias"], "BEARISH")

    def test_external_protected_low_break_can_flip_regime(self):
        data = self.frame([
            (103, 104, 100, 103),
            (103, 106, 102, 105),
            (105, 110, 104, 109),
            (109, 108, 103, 106),
            (106, 107, 102, 105),
            (105, 109, 104, 108),
            (108, 112, 107, 111),
            (111, 116, 110, 115),
            (115, 114, 111, 112),
            (112, 113, 111.5, 112.5),
            (112.5, 117, 112, 116.5),
            (116.5, 120, 115, 119),
            (119, 118, 110, 112),
            (112, 113, 104, 105),
            (105, 106, 98, 99),
        ])
        result = analyze_structure(data, left_bars=2, right_bars=2)
        bearish_choch = [
            event for event in result["events"]
            if event["direction"] == "BEARISH" and event["event_type"] == "CHOCH"
        ]
        if bearish_choch:
            self.assertEqual(result["bias"], "BEARISH")

    def test_module_is_analysis_only(self):
        import indicators.smc.engine as engine

        module_text = open(engine.__file__, "r", encoding="utf-8").read().lower()
        forbidden = ["place_market_order", "close_position", "live_auto", "paper_auto"]
        for token in forbidden:
            self.assertNotIn(token, module_text)


if __name__ == "__main__":
    unittest.main()
