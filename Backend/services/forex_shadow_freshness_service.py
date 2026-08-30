"""Pure, read-only freshness policy comparisons.

This module intentionally has no broker, strategy-state, or order-placement
imports.  It evaluates a deep copy of finalized production diagnostics only.
"""
from __future__ import annotations

import copy


POLICIES = (
    "CURRENT",
    "CONFIRMATION_EXPIRES_15M",
    "CONFIRMATION_EXPIRES_30M",
    "CONFIRMATION_EXPIRES_45M",
    "RECONFIRM_AFTER_REVIVAL",
    "CONTINUOUS_VALIDITY",
)


def evaluate_shadow_policies(production_context):
    context = copy.deepcopy(production_context or {})
    production_eligible = bool(context.get("signal_ready"))
    confirmation_age = context.get("confirmation_age_seconds")
    revival_count = int(context.get("revival_count") or 0)
    last_reappeared = context.get("setup_last_reappeared_at")
    confirmation_time = context.get("confirmation_time")
    setup_was_absent = bool(context.get("setup_was_absent_after_confirmation"))

    def base(reason="MATCHES_PRODUCTION"):
        return {
            "eligible": production_eligible,
            "reason": reason,
            "shadow_only": True,
            "production_executed": False,
        }

    results = {"CURRENT": base()}
    for minutes in (15, 30, 45):
        policy = f"CONFIRMATION_EXPIRES_{minutes}M"
        expired = confirmation_age is not None and float(confirmation_age) > minutes * 60
        results[policy] = {
            "eligible": bool(production_eligible and not expired),
            "reason": "CONFIRMATION_EXPIRED" if expired else "WITHIN_SHADOW_AGE_LIMIT",
            "shadow_only": True,
            "production_executed": False,
        }

    requires_reconfirm = bool(revival_count and last_reappeared)
    confirmed_after_revival = bool(
        context.get("new_confirmation_after_revival")
        or (confirmation_time and last_reappeared and confirmation_time > last_reappeared)
    )
    results["RECONFIRM_AFTER_REVIVAL"] = {
        "eligible": bool(
            production_eligible
            and (not requires_reconfirm or confirmed_after_revival)
        ),
        "reason": (
            "RECONFIRMATION_REQUIRED_AFTER_REVIVAL"
            if requires_reconfirm and not confirmed_after_revival
            else "RECONFIRMATION_NOT_REQUIRED_OR_PRESENT"
        ),
        "shadow_only": True,
        "production_executed": False,
    }
    results["CONTINUOUS_VALIDITY"] = {
        "eligible": bool(production_eligible and not setup_was_absent),
        "reason": (
            "SETUP_BECAME_ABSENT_AFTER_CONFIRMATION"
            if setup_was_absent else "SETUP_REMAINED_CONTINUOUS"
        ),
        "shadow_only": True,
        "production_executed": False,
    }
    return copy.deepcopy(results)
