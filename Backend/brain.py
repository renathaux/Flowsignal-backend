import sys

from strategies import shared as _shared
from strategies.eurusd_strategy import analyze_eurusd
from strategies.xauusd_strategy import analyze_xauusd


_original_get_mtf_signal = _shared.get_mtf_signal


def get_mtf_signal(data_5m, data_15m, data_1h, symbol):
    normalized_symbol = _shared.normalize_symbol(symbol)

    if normalized_symbol == "EURUSD":
        return analyze_eurusd(data_5m, data_15m, data_1h)

    if normalized_symbol == "XAUUSD":
        return analyze_xauusd(data_5m, data_15m, data_1h)

    return _original_get_mtf_signal(data_5m, data_15m, data_1h, symbol)


_shared.get_mtf_signal = get_mtf_signal
_shared._original_get_mtf_signal = _original_get_mtf_signal
_shared.analyze_eurusd = analyze_eurusd
_shared.analyze_xauusd = analyze_xauusd

sys.modules[__name__] = _shared
