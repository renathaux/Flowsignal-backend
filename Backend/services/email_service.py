from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

RESEND_API_URL = "https://api.resend.com/emails"


def send_verification_email(email: str, code: str) -> None:
    api_key = str(os.getenv("RESEND_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("EMAIL_PROVIDER_NOT_CONFIGURED")

    sender = str(
        os.getenv("FLOWSIGNAL_EMAIL_FROM", "FlowSignal <onboarding@resend.dev>") or ""
    ).strip()
    if not sender:
        raise RuntimeError("EMAIL_FROM_NOT_CONFIGURED")

    payload = {
        "from": sender,
        "to": [str(email).strip()],
        "subject": "Your FlowSignal verification code",
        "html": (
            "<div style='font-family:Arial,sans-serif;background:#07111e;color:#eaf3ff;"
            "padding:28px;border-radius:16px'>"
            "<h2 style='margin:0 0 12px'>Verify your FlowSignal account</h2>"
            "<p style='color:#a9bbce'>Enter this 6-digit code to finish creating your account.</p>"
            f"<div style='font-size:34px;font-weight:800;letter-spacing:8px;margin:24px 0'>{code}</div>"
            "<p style='color:#a9bbce'>This code expires in 10 minutes. If you did not request it, you can ignore this email.</p>"
            "</div>"
        ),
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
