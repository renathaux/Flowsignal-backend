# FlowSignal SMC Indicator Engine

This folder contains Smart Money Concepts analysis only.

Current scope:
- confirmed swing highs/lows;
- closed-candle BOS;
- closed-candle CHoCH;
- non-repainting confirmation timing;
- deterministic output for simulator/chart rendering.

Hard boundary:
- no cTrader imports;
- no order placement or modification;
- no LIVE/PAPER state;
- no risk sizing;
- no cooldown or execution gates.

The strategy may consume this engine later only after shadow/simulator validation.
