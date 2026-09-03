from datetime import datetime, timezone


SWING_CHANGED_REASON = "WAIT_SETUP_SWING_CHANGED_BEFORE_EXECUTION"


def _parse_timestamp(value):
    if value in [None, "", "--"]:
        return None
    try:
        if isinstance(value, (int, float)):
            numeric = float(value)
            if numeric > 1e12:
                numeric /= 1000
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def validate_fresh_setup_swing_identity(
    closed_15m,
    symbol,
    setup_identity,
    strict_trader_module,
):
    """Revalidate the immutable setup pivot without re-qualifying its old leg.

    The strict strategy already qualified the swing when it created the signal.
    Re-running ``detect_valid_swings`` on a shorter, freshly fetched window can
    change the qualification of an older pivot when the opposite anchor that
    originally qualified it has fallen outside that window.  For the final
    execution gate we only need to prove that the exact pivot identity still
    exists in the latest closed broker candles; EMA/consolidation and the setup
    fingerprint are validated separately by the existing execution gates.
    """
    identity = setup_identity if isinstance(setup_identity, dict) else {}
    normalized_symbol = strict_trader_module.shared.normalize_symbol(symbol)
    expected_type = str(identity.get("swing_type") or "").upper()
    expected_time = _parse_timestamp(identity.get("swing_timestamp"))
    try:
        expected_price = float(identity.get("swing_price"))
    except (TypeError, ValueError):
        expected_price = None

    details = {
        "fresh_setup_swing_match_method": "raw_pivot_identity",
        "fresh_setup_swing_matched": False,
        "fresh_setup_expected_swing": {
            "type": expected_type or None,
            "time": expected_time.isoformat() if expected_time else None,
            "price": expected_price,
        },
    }

    if (
        closed_15m is None
        or len(closed_15m) < 5
        or expected_type not in {"HIGH", "LOW"}
        or expected_time is None
        or expected_price is None
    ):
        details["fresh_setup_swing_validation_error"] = (
            "fresh closed candles or setup identity unavailable"
        )
        return {
            "ok": False,
            "reason": SWING_CHANGED_REASON,
            "details": details,
        }

    raw_swings = strict_trader_module.detect_raw_swings(
        closed_15m.copy(),
        normalized_symbol,
    )
    tolerance = strict_trader_module.point_size(normalized_symbol) + 1e-12
    matching_swing = None

    for swing in raw_swings:
        if str(swing.get("type") or "").upper() != expected_type:
            continue
        swing_time = _parse_timestamp(swing.get("time"))
        try:
            swing_price = float(swing.get("price"))
        except (TypeError, ValueError):
            continue
        if (
            swing_time == expected_time
            and abs(swing_price - expected_price) <= tolerance
        ):
            matching_swing = swing
            break

    details["fresh_raw_swing_count"] = len(raw_swings)
    details["fresh_setup_swing_matched"] = matching_swing is not None
    if matching_swing is not None:
        details["fresh_setup_matched_swing"] = {
            "type": matching_swing.get("type"),
            "time": matching_swing.get("time"),
            "price": matching_swing.get("price"),
            "fresh_window_valid_flag": matching_swing.get("valid"),
            "fresh_window_valid_reason": matching_swing.get("valid_reason"),
        }

    return {
        "ok": matching_swing is not None,
        "reason": None if matching_swing is not None else SWING_CHANGED_REASON,
        "details": details,
    }
