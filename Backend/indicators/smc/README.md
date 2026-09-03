# FlowSignal SMC Indicator Engine

This folder contains the deterministic Smart Money Concepts structure engine.

Current scope:
- confirmed swing highs/lows;
- closed-candle BOS;
- closed-candle CHoCH;
- protected external structure;
- non-repainting confirmation timing;
- deterministic output for strategy and chart rendering.

The engine is now the single backend authority for whether a 15-minute structure event exists and whether that event is BOS or CHoCH. The strict trader still owns the remaining entry and safety gates such as minimum structural size, BOS buffer, EMA permission, 5-minute confirmation, consolidation, SL/TP, risk, duplicate prevention and broker execution.

Hard boundary inside this package:
- no cTrader imports;
- no order placement or modification;
- no LIVE/PAPER state;
- no risk sizing;
- no cooldown or execution imports.

The browser indicator visibility switch is presentation only. Hiding the chart overlay does not disable this backend engine or the strategy's use of it.
