import unittest
from unittest.mock import patch

import pandas as pd

from strategies.smc_breakout_adapter import evaluate_15m_breakout


class SmcBreakoutAdapterTests(unittest.TestCase):
    def frame(self, count=20):
        index = pd.date_range("2026-08-17T00:00:00Z", periods=count, freq="15min")
        rows = [(1.10, 1.11, 1.09, 1.10) for _ in range(count)]
        return pd.DataFrame(rows, index=index, columns=["Open", "High", "Low", "Close"])

    def structure(self, *, direction="BULLISH", event_type="BOS", break_index=19):
        return {
            "bias": direction,
            "current_structure": {
                "bias": direction,
                "high": 1.1200,
                "low": 1.1000,
                "high_start_index": 12,
                "low_start_index": 14,
                "high_start_timestamp": "2026-08-17T03:00:00+00:00",
                "low_start_timestamp": "2026-08-17T03:30:00+00:00",
                "end_timestamp": "2026-08-17T04:45:00+00:00",
                "range": 0.0200,
            },
            "events": [{
                "event_type": event_type,
                "direction": direction,
                "timestamp": "2026-08-17T04:45:00+00:00",
                "close": 1.1210 if direction == "BULLISH" else 1.0990,
                "broken_swing_timestamp": "2026-08-17T03:00:00+00:00",
                "broken_level": 1.1200 if direction == "BULLISH" else 1.1000,
                "structure_start_index": 12,
                "break_index": break_index,
                "previous_direction": 1 if direction == "BULLISH" else 2,
                "new_direction": 2 if direction == "BULLISH" else 1,
            }],
            "fib_levels": [],
            "swings": [],
            "config": {"source_algorithm": "LudoGH68_SMC_Structures"},
        }

    @patch("strategies.smc_breakout_adapter.analyze_structure")
    def test_fresh_bullish_bos_becomes_v1_buy_break(self, analyze):
        analyze.return_value = self.structure(direction="BULLISH", event_type="BOS", break_index=19)
        result = evaluate_15m_breakout(self.frame(), "EURUSD")
        self.assertEqual(result["side"], "BUY")
        self.assertEqual(result["break_type"], "BOS")
        self.assertEqual(result["source"], "smc_structure_engine")
        self.assertEqual(result["bos_buffer"], 0.0)

    @patch("strategies.smc_breakout_adapter.analyze_structure")
    def test_fresh_bearish_choch_becomes_v1_sell_break(self, analyze):
        analyze.return_value = self.structure(direction="BEARISH", event_type="CHOCH", break_index=19)
        result = evaluate_15m_breakout(self.frame(), "EURUSD")
        self.assertEqual(result["side"], "SELL")
        self.assertEqual(result["break_type"], "CHOCH")

    @patch("strategies.smc_breakout_adapter.analyze_structure")
    def test_historical_chart_event_is_not_fresh_entry(self, analyze):
        analyze.return_value = self.structure(direction="BULLISH", event_type="BOS", break_index=10)
        result = evaluate_15m_breakout(self.frame(), "EURUSD")
        self.assertEqual(result["side"], "WAIT")
        self.assertEqual(result["reason"], "WAIT_NO_FRESH_15M_SMC_BREAK")


if __name__ == "__main__":
    unittest.main()
