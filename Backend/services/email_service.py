from __future__ import annotations

import json
import os
import smtplib
import urllib.error
import urllib.request
from email.mime.text import MIMEText

RESEND_API_URL = "https://api.resend.com/emails"
DEFAULT_GMAIL_FROM = "flowsignal.contact@gmail.com"


def _verification_html(code: str) -> str:
    return (
        "<div style='font-family:Arial,sans-serif;background:#07111e;color:#eaf3ff;"
        "padding:28px;border-radius:16px'>"
        "<h2 style='margin:0 0 12px'>Verify your FlowSignal account</h2>"
        "<p style='color:#a9bbce'>Enter this 6-digit code to finish creating your account.</p>"
        f"<div style='font-size:34px;font-weight:800;letter-spacing:8px;margin:24px 0'>{code}</div>"
        "<p style='color:#a9bbce'>This code expires in 10 minutes. If you did not request it, you can ignore this email.</p>"
        "</div>"
    )


def _send_with_resend(email: str, code: str, api_key: str) -> None:
    sender = str(
        os.getenv("FLOWSIGNAL_EMAIL_FROM", "FlowSignal <onboarding@resend.dev>") or ""
    ).strip()
    if not sender:
        raise RuntimeError("EMAIL_FROM_NOT_CONFIGURED")

    payload = {
        "from": sender,
        "to": [str(email).strip()],
        "subject": "Your FlowSignal verification code",
        "html": _verification_html(code),
    }
    request = urllib.request.Request(
        RESEND_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "FlowSignal/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            if int(getattr(response, "status", 200)) >= 300:
                raise RuntimeError("EMAIL_DELIVERY_FAILED")
    except urllib.error.HTTPError as exc:
        raise RuntimeError("EMAIL_DELIVERY_FAILED") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("EMAIL_DELIVERY_FAILED") from exc


def _send_with_gmail(email: str, code: str, app_password: str) -> None:
    sender = str(os.getenv("FEEDBACK_EMAIL", DEFAULT_GMAIL_FROM) or DEFAULT_GMAIL_FROM).strip()
    if not sender:
        raise RuntimeError("EMAIL_FROM_NOT_CONFIGURED")

    message = MIMEText(_verification_html(code), "html")
    message["Subject"] = "Your FlowSignal verification code"
    message["From"] = f"FlowSignal <{sender}>"
    message["To"] = str(email).strip()
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=12) as server:
            server.starttls()
            server.login(sender, app_password)
            server.send_message(message)
    except Exception as exc:
        raise RuntimeError("EMAIL_DELIVERY_FAILED") from exc


def send_verification_email(email: str, code: str) -> None:
    resend_key = str(os.getenv("RESEND_API_KEY", "") or "").strip()
    if resend_key:
        _send_with_resend(email, code, resend_key)
        return

    gmail_password = str(os.getenv("FEEDBACK_APP_PASSWORD", "") or "").strip()
    if gmail_password:
        _send_with_gmail(email, code, gmail_password)
        return

    raise RuntimeError("EMAIL_PROVIDER_NOT_CONFIGURED")
