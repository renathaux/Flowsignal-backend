from __future__ import annotations

from functools import wraps

from fastapi.responses import JSONResponse

# These are owner/global trading-state endpoints. Customer Forex remains
# signal/read-only. The existing owner login token is preserved as authority.
EXACT_PATHS = {
    "/connect-ctrader",
    "/refresh-ctrader-accounts",
    "/set-active-ctrader-account",
    "/forget-ctrader-account",
    "/disconnect-ctrader",
    "/close-live-trade",
    "/modify-live-position-levels",
    "/live-auto-toggle",
    "/execute-trade",
    "/ctrader/disconnect",
    "/ctrader/accounts/refresh",
    "/ctrader/accounts/active",
    "/ctrader/accounts/forget",
    "/ctrader/accounts/clear",
}
PREFIX_PATHS = (
    "/settings/",
)


def _sensitive(path: str, method: str) -> bool:
    if path in EXACT_PATHS:
        return True
    if method.upper() in {"POST", "PUT", "PATCH", "DELETE"} and any(path.startswith(prefix) for prefix in PREFIX_PATHS):
        return True
    return False


def _bearer(headers) -> str:
    raw = str(headers.get("authorization") or "")
    if not raw.lower().startswith("bearer "):
        return ""
    return raw.split(" ", 1)[1].strip()


def install_owner_forex_mutation_guard(app, legacy_sessions: dict) -> dict:
    """Wrap sensitive legacy routes after route registration.

    This intentionally leaves read-only signal/panel routes open to normal
    FlowSignal users while requiring the existing backend legacy admin session
    for any owner/global Forex mutation.
    """
    installed = 0
    for route in list(getattr(app, "routes", [])):
        path = str(getattr(route, "path", "") or "")
        methods = set(getattr(route, "methods", set()) or set())
        if not path or not any(_sensitive(path, method) for method in methods):
            continue
        if getattr(route, "_flowsignal_owner_guarded", False):
            continue
        original = route.app

        @wraps(original)
        async def guarded(scope, receive, send, _original=original):
            method = str(scope.get("method") or "GET").upper()
            request_path = str(scope.get("path") or "")
            if _sensitive(request_path, method):
                headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
                token = _bearer(headers)
                session = legacy_sessions.get(token) if token else None
                if not isinstance(session, dict) or str(session.get("role") or "").lower() != "admin":
                    response = JSONResponse({"ok": False, "detail": "ADMIN_FOREX_MUTATION_REQUIRED"}, status_code=403)
                    await response(scope, receive, send)
                    return
            await _original(scope, receive, send)

        route.app = guarded
        route._flowsignal_owner_guarded = True
        installed += 1
    return {"ok": True, "guarded_routes": installed}
