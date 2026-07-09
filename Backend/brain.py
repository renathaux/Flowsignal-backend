import sys

from strategies import shared as _shared
from strategies.eurusd_strategy import analyze_eurusd
from strategies.strict_trader import get_mtf_signal as get_strict_mtf_signal
from strategies.xauusd_strategy import analyze_xauusd


_original_get_mtf_signal = _shared.get_mtf_signal


def get_mtf_signal(data_5m, data_15m, data_1h, symbol):
    return get_strict_mtf_signal(data_5m, data_15m, data_1h, symbol)


_shared.get_mtf_signal = get_mtf_signal
_shared._original_get_mtf_signal = _original_get_mtf_signal
_shared.analyze_eurusd = analyze_eurusd
_shared.analyze_xauusd = analyze_xauusd
_shared.get_strict_mtf_signal = get_strict_mtf_signal

sys.modules[__name__] = _shared
