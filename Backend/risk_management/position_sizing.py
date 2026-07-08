import math


def round_volume_down_to_step(volume_units, step_units):
    try:
        volume = int(float(volume_units))
        step = int(float(step_units))
    except (TypeError, ValueError):
        return 0

    if volume <= 0 or step <= 0:
        return 0

    return (volume // step) * step


def calculate_max_risk_usd(account_equity, risk_percent):
    try:
        equity = float(account_equity)
        risk = float(risk_percent)
    except (TypeError, ValueError):
        return None

    if equity <= 0 or risk <= 0:
        return None

    return equity * (risk / 100)


def calculate_expected_loss_usd(sl_pips, pip_value_per_lot, lot_size):
    try:
        value = float(sl_pips) * float(pip_value_per_lot) * float(lot_size)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(value) or value < 0:
        return None

    return value


def calculate_position_size(
    symbol,
    account_equity,
    risk_percent,
    stop_loss_pips,
    metadata,
    *,
    convert_lots_to_volume,
    convert_volume_to_lots,
    payload_volume_scale=None,
    default_lot_size=100000,
    risk_tolerance_percent=0.10,
    maximum_allowed_risk_percent=None,
):
    execution_symbol = str(symbol or "").upper()
    payload_volume_scale = payload_volume_scale or {}

    try:
        equity_value = float(account_equity)
        risk_percent_value = float(risk_percent)
        sl_pips = float(stop_loss_pips)
    except (TypeError, ValueError):
        return {"ok": False, "reason": "Invalid risk sizing inputs"}

    if equity_value <= 0:
        return {"ok": False, "reason": "Cannot calculate risk without account balance"}

    if risk_percent_value <= 0:
        return {"ok": False, "reason": "Invalid risk percent"}

    if sl_pips <= 0:
        return {"ok": False, "reason": "Cannot calculate risk without valid SL"}

    metadata = metadata or {}
    if not metadata.get("ok"):
        return {
            "ok": False,
            "reason": metadata.get("reason") or "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    pip_value_per_lot = metadata.get("pip_value_per_lot")
    lot_contract_size = metadata.get("lot_size", default_lot_size)
    min_volume_units = metadata.get("min_volume_units")
    max_volume_units = metadata.get("max_volume_units")
    volume_step_units = metadata.get("volume_step_units")

    try:
        pip_value_per_lot = float(pip_value_per_lot)
        lot_contract_size = float(lot_contract_size)
        min_volume_units = int(float(min_volume_units))
        max_volume_units = int(float(max_volume_units))
        volume_step_units = int(float(volume_step_units))
    except (TypeError, ValueError):
        return {
            "ok": False,
            "reason": "Cannot calculate risk without broker symbol metadata",
            "symbol_metadata": metadata,
        }

    if (
        pip_value_per_lot <= 0
        or lot_contract_size <= 0
        or min_volume_units <= 0
        or max_volume_units <= 0
        or volume_step_units <= 0
        or min_volume_units > max_volume_units
    ):
        return {
            "ok": False,
            "reason": "Invalid broker symbol volume metadata",
            "symbol_metadata": metadata,
        }

    risk_money = calculate_max_risk_usd(equity_value, risk_percent_value)
    calculated_lots = risk_money / (sl_pips * pip_value_per_lot)

    if calculated_lots <= 0 or not math.isfinite(calculated_lots):
        return {
            "ok": False,
            "reason": "Calculated position size is invalid",
            "symbol_metadata": metadata,
        }

    raw_volume_units = convert_lots_to_volume(calculated_lots, lot_contract_size)
    rounded_volume_units = round_volume_down_to_step(raw_volume_units, volume_step_units)
    calculated_volume_units = rounded_volume_units
    minimum_volume_rounded_up = rounded_volume_units < min_volume_units

    if minimum_volume_rounded_up:
        rounded_volume_units = min_volume_units

    rounded_lots = convert_volume_to_lots(rounded_volume_units, lot_contract_size)
    broker_min_lots = convert_volume_to_lots(min_volume_units, lot_contract_size)
    payload_scale = payload_volume_scale.get(execution_symbol, 1)
    payload_volume = int(rounded_volume_units * payload_scale)
    pip_size = metadata.get("pip_size")

    try:
        stop_loss_price_distance = round(sl_pips * float(pip_size), 8)
    except (TypeError, ValueError):
        stop_loss_price_distance = None

    base_check = {
        "symbol": execution_symbol,
        "account_balance": round(equity_value, 2),
        "risk_percent": risk_percent_value,
        "risk_amount": round(risk_money, 2),
        "sl_pips": round(sl_pips, 2),
        "pip_size": pip_size,
        "stop_loss_price_distance": stop_loss_price_distance,
        "pip_value_per_lot": pip_value_per_lot,
        "calculated_lots": round(calculated_lots, 4),
        "rounded_lots": round(rounded_lots, 4) if rounded_lots else 0,
        "broker_min_lots": round(broker_min_lots, 4) if broker_min_lots else None,
        "broker_min_volume": min_volume_units,
        "volume_step": volume_step_units,
        "payload_volume": payload_volume,
        "requested_units": int(rounded_volume_units),
        "max_lot_cap_used": False,
        "lot_contract_size": lot_contract_size,
        "volume_units": int(rounded_volume_units),
        "raw_volume_units": int(raw_volume_units),
        "calculated_volume_units": int(calculated_volume_units),
        "min_volume_units": min_volume_units,
        "max_volume_units": max_volume_units,
        "volume_step_units": volume_step_units,
        "minimum_volume_rounded_up": minimum_volume_rounded_up,
    }

    if rounded_volume_units <= 0:
        return {
            **base_check,
            "ok": False,
            "reason": "Calculated volume is invalid",
            "symbol_metadata": metadata,
        }

    if rounded_volume_units % volume_step_units != 0:
        return {
            **base_check,
            "ok": False,
            "reason": "Calculated volume is not aligned with broker step",
            "symbol_metadata": metadata,
        }

    if rounded_volume_units > max_volume_units:
        return {
            **base_check,
            "ok": False,
            "reason": "Calculated volume is above broker/safety maximum",
            "symbol_metadata": metadata,
            "pip_value_per_lot": pip_value_per_lot,
            "lot_size": round(calculated_lots, 4),
            "raw_lots": calculated_lots,
            "broker_max_volume": max_volume_units,
            "safety_max_volume_units": max_volume_units,
        }

    lot_size = convert_volume_to_lots(rounded_volume_units, lot_contract_size)

    if not lot_size or lot_size <= 0:
        return {
            **base_check,
            "ok": False,
            "reason": "Calculated lot size is invalid",
            "symbol_metadata": metadata,
        }

    final_risk_amount = calculate_expected_loss_usd(sl_pips, pip_value_per_lot, lot_size)
    final_risk_percent = (final_risk_amount / equity_value) * 100
    volume_step_lots = convert_volume_to_lots(volume_step_units, lot_contract_size) or 0
    allowed_risk_difference = (volume_step_lots * sl_pips * pip_value_per_lot) + 0.01
    risk_difference = abs(risk_money - final_risk_amount)
    max_allowed_percent = maximum_allowed_risk_percent
    if max_allowed_percent is None:
        max_allowed_percent = risk_percent_value + 0.01
    minimum_volume_risk_limit = max_allowed_percent + risk_tolerance_percent

    if minimum_volume_rounded_up and final_risk_percent > minimum_volume_risk_limit:
        return {
            **base_check,
            "ok": False,
            "reason": (
                "LIVE BLOCKED: calculated volume below broker minimum and "
                "min volume exceeds allowed risk."
            ),
            "symbol_metadata": metadata,
            "lot_size": round(lot_size, 4),
            "final_risk_amount": round(final_risk_amount, 2),
            "final_risk_percent": round(final_risk_percent, 4),
            "maximum_allowed_risk_percent": max_allowed_percent,
            "allowed_risk_percent": max_allowed_percent,
            "risk_tolerance_percent": risk_tolerance_percent,
        }

    if (
        final_risk_amount is None
        or final_risk_amount <= 0
        or not math.isfinite(final_risk_amount)
        or final_risk_amount > risk_money + 0.01
        or (
            not minimum_volume_rounded_up
            and (
                risk_difference > allowed_risk_difference
                or final_risk_amount > risk_money + allowed_risk_difference
            )
        )
    ):
        return {
            **base_check,
            "ok": False,
            "reason": (
                f"Calculated risk is not close to {risk_percent_value:.2f}% "
                "after broker rounding"
            ),
            "symbol_metadata": metadata,
            "lot_size": round(lot_size, 4),
            "final_risk_amount": round(final_risk_amount, 2),
            "final_risk_percent": round(final_risk_percent, 4),
            "risk_difference": round(risk_difference, 2),
            "allowed_risk_difference": round(allowed_risk_difference, 2),
        }

    return {
        "ok": True,
        "symbol": execution_symbol,
        "risk_percent": risk_percent_value,
        "risk_amount": round(risk_money, 2),
        "account_balance": round(equity_value, 2),
        "lot_size": round(lot_size, 4),
        "calculated_lots": round(calculated_lots, 4),
        "rounded_lots": round(lot_size, 4),
        "broker_min_lots": round(broker_min_lots, 4) if broker_min_lots else None,
        "volume_units": int(rounded_volume_units),
        "raw_lots": calculated_lots,
        "raw_volume_units": int(raw_volume_units),
        "calculated_volume_units": int(calculated_volume_units),
        "sl_pips": round(sl_pips, 2),
        "pip_size": metadata.get("pip_size"),
        "stop_loss_price_distance": base_check.get("stop_loss_price_distance"),
        "pip_value_per_lot": pip_value_per_lot,
        "tick_size": metadata.get("tick_size"),
        "tick_value": metadata.get("tick_value"),
        "pip_position": metadata.get("pip_position"),
        "lot_contract_size": lot_contract_size,
        "volume_step_units": volume_step_units,
        "broker_min_volume": min_volume_units,
        "volume_step": volume_step_units,
        "payload_volume": payload_volume,
        "requested_units": int(rounded_volume_units),
        "max_lot_cap_used": False,
        "min_volume_units": min_volume_units,
        "max_volume_units": max_volume_units,
        "broker_max_volume": max_volume_units,
        "volume_step_lots": metadata.get("volume_step_lots"),
        "min_lot": metadata.get("min_lot"),
        "max_lot": metadata.get("max_lot"),
        "metadata_source": metadata.get("metadata_source"),
        "final_risk_amount": round(final_risk_amount, 2),
        "final_risk_percent": round(final_risk_percent, 4),
        "risk_difference": round(risk_difference, 2),
        "allowed_risk_difference": round(allowed_risk_difference, 2),
        "maximum_allowed_risk_percent": max_allowed_percent,
        "allowed_risk_percent": max_allowed_percent,
        "risk_tolerance_percent": risk_tolerance_percent,
        "minimum_volume_rounded_up": minimum_volume_rounded_up,
    }


def calculate_expected_loss_usd_from_risk_size(risk_size):
    if not isinstance(risk_size, dict):
        return None

    value = calculate_expected_loss_usd(
        risk_size.get("sl_pips"),
        risk_size.get("pip_value_per_lot"),
        risk_size.get("lot_size"),
    )
    if value is not None:
        return value

    for key in ["final_risk_amount", "risk_amount_actual", "expected_loss_usd"]:
        try:
            fallback = float(risk_size.get(key))
        except (TypeError, ValueError):
            continue
        if math.isfinite(fallback) and fallback >= 0:
            return fallback

    return None
