from datetime import datetime, timezone
from email.mime.text import MIMEText
import os
import re
import smtplib
import threading

import api
from services.setup_swing_execution_guard import validate_fresh_setup_swing_identity


_ORIGINAL_GET_SIGNAL_ALERT_EMAIL_TO = api.get_signal_alert_email_to
_ORIGINAL_PROTECT_LIVE_TRADE_AFTER_TP1 = api.protect_live_trade_after_tp1
_ORIGINAL_VALIDATE_FRESH_EMA_PERMISSION_LOCKED = (
    api.validate_fresh_ema_permission_locked
)


def _split_recipients(value):
    recipients = []
    seen = set()
    for item in re.split(r"[,;]", str(value or "")):
        address = item.strip()
        if not address:
            continue
        key = address.casefold()
        if key in seen:
            continue
        seen.add(key)
        recipients.append(address)
    return recipients


def _signal_alert_recipients():
    recipients = []
    seen = set()
    for source in (
        _ORIGINAL_GET_SIGNAL_ALERT_EMAIL_TO(),
        os.getenv("SIGNAL_ALERT_EMAIL_CC", ""),
    ):
        for address in _split_recipients(source):
            key = address.casefold()
            if key in seen:
                continue
            seen.add(key)
            recipients.append(address)
    return recipients


def get_signal_alert_email_to_multi():
    return ", ".join(_signal_alert_recipients())


def _send_tp1_protection_email(trade):
    recipients = _signal_alert_recipients()
    if not recipients:
        return False

    symbol = api.normalize_symbol(trade.get("symbol"))
    side = str(trade.get("side") or trade.get("action") or "").upper()
    protected_sl = trade.get("protected_sl_price") or trade.get("sl")
    generated_at = datetime.now(timezone.utc).isoformat()

    subject = (
        f"FlowSignal TP1 Hit: {symbol} {side} - "
        f"Secure SL {protected_sl}"
    )
    body = f"""
FlowSignal TP1 Protection Alert

Symbol: {symbol}
Direction: {side}

TP1 has been hit.
Move your stop loss now to: {protected_sl}

Entry: {trade.get("entry")}
TP1: {trade.get("tp1")}
TP2: {trade.get("tp2")}
Original SL: {trade.get("original_sl")}
Secured SL: {protected_sl}
Broker protection: CONFIRMED
Time generated: {generated_at}
""".strip()

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = api.FEEDBACK_EMAIL
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(api.FEEDBACK_EMAIL, api.FEEDBACK_APP_PASSWORD)
        server.send_message(msg, to_addrs=recipients)

    print("TP1_PROTECTION_EMAIL_SENT =", {
        "symbol": symbol,
        "side": side,
        "protected_sl": protected_sl,
        "to": recipients,
        "subject": subject,
    })
    return True


def protect_live_trade_after_tp1_with_email(trade):
    was_confirmed = (
        api.live_sl_protection_confirmed(trade)
        if isinstance(trade, dict)
        else False
    )

    result = _ORIGINAL_PROTECT_LIVE_TRADE_AFTER_TP1(trade)
    if not isinstance(result, dict):
        return result

    now_confirmed = api.live_sl_protection_confirmed(result)

    # Mark a notification as pending only on the transition from unprotected to
    # broker-verified protection. This avoids stale TP1 emails after a deploy.
    if now_confirmed and not was_confirmed:
        result["tp1_protection_email_pending"] = True
        result["tp1_protection_confirmed_at"] = (
            datetime.now(timezone.utc).isoformat()
        )
        api.persist_live_trade_state(result)

    # Persist the pending flag before SMTP. If delivery fails, the next broker
    # sync retries the email without moving the stop loss again.
    if (
        now_confirmed
        and result.get("tp1_protection_email_pending")
        and not result.get("tp1_protection_email_sent")
    ):
        try:
            if _send_tp1_protection_email(result):
                result["tp1_protection_email_sent"] = True
                result["tp1_protection_email_sent_at"] = (
                    datetime.now(timezone.utc).isoformat()
                )
                result["tp1_protection_email_pending"] = False
                api.persist_live_trade_state(result)
        except Exception as exc:
            print("TP1_PROTECTION_EMAIL_FAILED =", {
                "symbol": api.normalize_symbol(result.get("symbol")),
                "position_id": (
                    result.get("position_id")
                    or result.get("broker_position_id")
                ),
                "error": str(exc),
            })

    return result


def validate_fresh_ema_permission_locked_with_stable_swing_identity(
    symbol,
    side,
    setup_identity=None,
):
    """Recover only the known short-window swing requalification false negative.

    The original final gate still owns EMA, consolidation, and all of its normal
    failure modes.  If and only if it reaches the historical swing mismatch,
    verify that the exact strategy-approved pivot still exists in the latest
    closed broker candles without re-running its path-dependent 100-point leg
    qualification on the shortened fresh window.
    """
    result = _ORIGINAL_VALIDATE_FRESH_EMA_PERMISSION_LOCKED(
        symbol,
        side,
        setup_identity=setup_identity,
    )
    if not isinstance(result, dict):
        return result
    if result.get("ok"):
        return result
    if result.get("reason") != "WAIT_SETUP_SWING_CHANGED_BEFORE_EXECUTION":
        return result

    details = dict(result.get("details") or {})
    try:
        from strategies import strict_trader

        latest_15m = api.get_ctrader_market_data(
            api.normalize_symbol(symbol),
            "15m",
            limit=100,
            force_refresh=False,
        )
        closed_15m = strict_trader.closed_frame(latest_15m, 15)
        swing_check = validate_fresh_setup_swing_identity(
            closed_15m,
            symbol,
            setup_identity,
            strict_trader,
        )
        details.update(swing_check.get("details") or {})

        if swing_check.get("ok"):
            details["legacy_short_window_valid_swing_requalification"] = (
                "false_negative_recovered"
            )
            print("LIVE_SETUP_SWING_IDENTITY_RECOVERED =", {
                "symbol": api.normalize_symbol(symbol),
                "side": str(side or "").upper(),
                "setup_identity": setup_identity,
                "details": swing_check.get("details"),
            })
            return {
                "ok": True,
                "reason": None,
                "details": details,
            }
    except Exception as exc:
        details["stable_setup_swing_recheck_error"] = str(exc)

    return {
        "ok": False,
        "reason": "WAIT_SETUP_SWING_CHANGED_BEFORE_EXECUTION",
        "details": details,
    }


def _start_forex_background_task():
    print("Startup OK - warming panel cache")
    api.warm_panel_cache_from_persisted_candles()
    try:
        api.start_ctrader_live_price_stream()
    except Exception as exc:
        print("CTRADER_LIVE_STREAM_START_ERROR =", str(exc))
    with api.BACKGROUND_THREAD_LOCK:
        if api.BACKGROUND_THREAD is not None and api.BACKGROUND_THREAD.is_alive():
            print("BACKGROUND_FETCH_ALREADY_RUNNING =", {
                "thread_id": api.BACKGROUND_THREAD.ident,
            })
            return
        api.BACKGROUND_THREAD = threading.Thread(
            target=api.background_fetch,
            name="flowsignal-trading-engine",
            daemon=True,
        )
        api.BACKGROUND_THREAD.start()
        api.ENGINE_RUNTIME_STATE["loop_thread_id"] = api.BACKGROUND_THREAD.ident


# Replace the legacy startup hook with the Forex-only runtime while preserving
# the same panel warmup, cTrader stream, and 24/7 strategy background thread.
api.app.router.on_startup = [
    handler for handler in api.app.router.on_startup
    if handler is not api.start_background_task
]
api.app.router.on_startup.append(_start_forex_background_task)

# Keep existing alert behavior and install the narrow final-entry swing guard
# correction. The correction cannot bypass EMA, consolidation, risk, duplicate,
# broker-position, setup-fingerprint, or market-data gates.
api.get_signal_alert_email_to = get_signal_alert_email_to_multi
api.protect_live_trade_after_tp1 = protect_live_trade_after_tp1_with_email
api.validate_fresh_ema_permission_locked = (
    validate_fresh_ema_permission_locked_with_stable_swing_identity
)

app = api.app
