from datetime import datetime, timezone
from email.mime.text import MIMEText
import os
import re
import smtplib

from fastapi.responses import JSONResponse

import api


_ORIGINAL_GET_SIGNAL_ALERT_EMAIL_TO = api.get_signal_alert_email_to
_ORIGINAL_PROTECT_LIVE_TRADE_AFTER_TP1 = api.protect_live_trade_after_tp1


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


# Keep all existing signal-email behavior while allowing extra recipients from
# SIGNAL_ALERT_EMAIL_CC. No strategy, risk, or execution logic is changed.
api.get_signal_alert_email_to = get_signal_alert_email_to_multi
api.protect_live_trade_after_tp1 = protect_live_trade_after_tp1_with_email


# Binary/Deriv is intentionally retired. Keep the database tables and code in
# place for reversibility, but prevent any Binary runtime polling, execution,
# settlement recovery, OAuth/status traffic, or relay ingestion from touching
# Neon. Forex/cTrader behavior is unaffected.
try:
    from services import deriv_binary_settlement_recovery as _binary_recovery

    def _binary_recovery_disabled():
        print("BINARY_RUNTIME_DISABLED = True")

    _binary_recovery.start_settlement_recovery_worker = _binary_recovery_disabled
except Exception as exc:
    print("BINARY_RUNTIME_DISABLE_WARNING =", type(exc).__name__)


app = api.app


@app.middleware("http")
async def block_retired_binary_routes(request, call_next):
    path = request.url.path
    if path == "/deriv" or path.startswith("/deriv/") or path == "/user/deriv" or path.startswith("/user/deriv/"):
        return JSONResponse(
            status_code=410,
            content={
                "detail": "BINARY_FEATURE_DISABLED",
                "feature": "binary",
                "database_access": False,
            },
        )
    return await call_next(request)
