import sys

from strategies import shared as _shared
from strategies import strict_trader as _strict_trader
from strategies.eurusd_strategy import analyze_eurusd
from strategies.smc_breakout_adapter import evaluate_15m_breakout as evaluate_smc_15m_breakout
from strategies.xauusd_strategy import analyze_xauusd


# Make the TradingView-style SMC structure engine authoritative for V1's
# closed-15m BOS/CHoCH gate. Everything after that gate remains strict_trader:
# EMA, consolidation, later 5m confirmation, SL/TP/RR, risk and execution.
_strict_trader.evaluate_15m_breakout = evaluate_smc_15m_breakout
get_strict_mtf_signal = _strict_trader.get_mtf_signal

_original_get_mtf_signal = _shared.get_mtf_signal


def get_mtf_signal(data_5m, data_15m, data_1h, symbol):
    return get_strict_mtf_signal(data_5m, data_15m, data_1h, symbol)


_shared.get_mtf_signal = get_mtf_signal
_shared._original_get_mtf_signal = _original_get_mtf_signal
_shared.analyze_eurusd = analyze_eurusd
_shared.analyze_xauusd = analyze_xauusd
_shared.get_strict_mtf_signal = get_strict_mtf_signal
_shared.evaluate_smc_15m_breakout = evaluate_smc_15m_breakout

sys.modules[__name__] = _shared
