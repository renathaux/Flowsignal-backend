def live_prices_match(symbol, left, right, *, normalize_symbol):
    try:
        tolerance = 0.005 if normalize_symbol(symbol) == "XAUUSD" else 0.000005
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return False


def build_live_protection_audit(
    symbol,
    side,
    *,
    normalize_symbol,
    entry=None,
    saved_sl=None,
    broker_sl=None,
    displayed_sl=None,
    tp2=None,
    broker_tp=None,
    bid=None,
    ask=None,
    lot_size=None,
    expected_loss_usd=None,
    max_risk_usd=None,
    repair_result=None,
    stage=None,
):
    return {
        "stage": stage,
        "symbol": normalize_symbol(symbol),
        "side": str(side or "").upper(),
        "entry": entry,
        "saved_sl": saved_sl,
        "broker_sl": broker_sl,
        "displayed_sl": displayed_sl,
        "tp2": tp2,
        "broker_tp": broker_tp,
        "bid": bid,
        "ask": ask,
        "lot_size": lot_size,
        "expected_loss_usd": expected_loss_usd,
        "max_risk_usd": max_risk_usd,
        "repair_result": repair_result,
    }

