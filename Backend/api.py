from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from email.mime.text import MIMEText
import smtplib
import time
import threading
import json
import os
import hashlib
import uuid
from ctrader_connector import (
    get_open_positions,
    place_market_order,
    set_debug_open_positions
)

app = FastAPI()
@app.on_event("startup")
def start_background_task():
    print("Startup OK - starting background fetch")
    thread = threading.Thread(target=background_fetch)
    thread.daemon = True
    thread.start()
    
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TradeRequest(BaseModel):
    symbol: str
    action: str
    token: str

class FeedbackRequest(BaseModel):
    message: str
    user: str | None = None
    time: str | None = None

class SignupRequest(BaseModel):
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

class DebugBrokerPositionsRequest(BaseModel):
    positions: list

# =========================
# DEFAULT PANEL DATA
# =========================
def default_panel():
    return {
        "EURUSD": {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (startup)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "UNKNOWN",
            "entry_quality": "WEAK"
        },
        "GOLD": {
            "signal": "WAIT",
            "signal_text": "WAIT ⚪ (startup)",
            "buy_pct": 0,
            "sell_pct": 0,
            "confidence": 0,
            "market_condition": "UNKNOWN",
            "entry_quality": "WEAK"
        }
    }


# =========================
# CACHE
# =========================
PANEL_CACHE = {
    "data": default_panel(),
    "last_update": 0
}

CACHE_SECONDS = 300
ADMIN_TOKEN = "N2415"
FEEDBACK_EMAIL = "flowsignal.contact@gmail.com"
FEEDBACK_APP_PASSWORD = "wwro vjjg grzt vpcp"
USERS_FILE = "users.json"
VISITS_FILE = "visits.json"
SESSIONS = {}

def load_users():
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_users(users):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2)
def load_visits():
    if not os.path.exists(VISITS_FILE):
        return []

    try:
        with open(VISITS_FILE, "r") as f:
            return json.load(f)
    except:
        return []


def save_visits(visits):
    with open(VISITS_FILE, "w") as f:
        json.dump(visits, f, indent=2)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()
import threading

def background_fetch():
    from brain import get_panel_data

    while True:
        try:
            print("🔄 BACKGROUND FETCH (once for all users)")

            data = get_panel_data()

            if isinstance(data, dict) and "EURUSD" in data and "GOLD" in data:
                PANEL_CACHE["data"] = data
                PANEL_CACHE["last_update"] = time.time()
                print("✅ Cache updated globally")

            else:
                print("⚠️ Invalid data format from brain")

        except Exception as e:
            print("❌ Background fetch error:", e)

        time.sleep(CACHE_SECONDS)

@app.get("/")
def root():
    return {"message": "FlowSignal backend is running"}


@app.get("/panel-data")
def panel_data():
    age = time.time() - PANEL_CACHE["last_update"]

    cached_data = PANEL_CACHE.get("data")

    if not isinstance(cached_data, dict):
        cached_data = default_panel()

    data = cached_data.copy()

    sync_live_positions()

    data["_meta"] = {
        "source": "shared_cache",
        "cache_age_seconds": round(age, 1),
        "refresh_seconds": CACHE_SECONDS,
        "error": None,

        "paper_auto_enabled":
            AUTO_TRADE_ENABLED["enabled"],

        "live_auto_enabled":
            LIVE_AUTO_TRADE_ENABLED["enabled"],

        "live_account":
            LIVE_ACCOUNT_STATE,

        "live_active_orders":
            LIVE_ACTIVE_ORDERS,

        "live_trade_history":
            LIVE_TRADE_HISTORY,


        "execution_mode":
            EXECUTION_MODE["mode"]
    }

    return data
@app.post("/feedback")
def send_feedback(request: FeedbackRequest):
    try:
        msg_body = f"""
FlowSignal Feedback

User: {request.user or "anonymous"}
Time: {request.time or "unknown"}

Message:
{request.message}
""".strip()

        msg = MIMEText(msg_body)
        msg["Subject"] = "FlowSignal Feedback"
        msg["From"] = FEEDBACK_EMAIL
        msg["To"] = FEEDBACK_EMAIL

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(FEEDBACK_EMAIL, FEEDBACK_APP_PASSWORD)
            server.send_message(msg)

        print("✅ Feedback email sent")
        return {"status": "sent"}

    except Exception as e:
        print("❌ Feedback error:", e)
        return {"status": "error", "message": str(e)}
    
@app.post("/signup")
def signup(request: SignupRequest):
    users = load_users()
    email = request.email.strip().lower()

    if not email or not request.password.strip():
        return {"ok": False, "message": "Email and password required"}

    if email in users:
        return {"ok": False, "message": "Account already exists"}

    role = "user"

    if email == "flowsignal.contact@gmail.com":
        role = "admin"

    users[email] = {
        "password": hash_password(request.password),
        "role": role
    }
    save_users(users)

    return {"ok": True, "message": "Account created"}

@app.post("/login")
def login(request: LoginRequest):
    users = load_users()
    email = request.email.strip().lower()
    hashed = hash_password(request.password)
    
    if email == "flowsignal.contact@gmail.com" and request.password == "@Renathaux509.":
        token = str(uuid.uuid4())
        SESSIONS[token] = {
            "email": email,
            "role": "admin"
        }

        return {
            "ok": True,
            "message": "Admin login success",
            "token": token,
            "email": email,
            "role": "admin"
        }

    if email not in users:
        return {"ok": False, "message": "Account not found"}

    if users[email]["password"] != hashed:
        return {"ok": False, "message": "Wrong password"}

    role = "admin" if email == "flowsignal.contact@gmail.com" else users[email].get("role", "user")

    token = str(uuid.uuid4())
    SESSIONS[token] = {
        "email": email,
        "role": role
    }

    return {
        "ok": True,
        "message": "Login success",
        "token": token,
        "email": email,
        "role": role
    }

@app.post("/track-visit")
def track_visit(data: dict, request: Request):
    try:
        visits = load_visits()

        visitor_id = data.get("visitor_id")
        
        ip = request.headers.get("x-forwarded-for", request.client.host)
        if ip and "," in ip:
            ip = ip.split(",")[0].strip()

        country = "Unknown"

        try:
            if ip not in ["127.0.0.1", "localhost"]:
                geo = requests.get(f"http://ip-api.com/json/{ip}", timeout=2).json()
                country = geo.get("country", "Unknown")
            else:
                country = "Local"
        except Exception:
            country = "Unknown"

        if not visitor_id:
            visitor_id = str(uuid.uuid4())

        visit_data = {
            "time": time.time(),
            "visitor_id": visitor_id,
            "country": country
        }

        visits.append(visit_data)

        thirty_days_ago = time.time() - (30 * 86400)

        visits = [
            v for v in visits
            if v.get("time", 0) >= thirty_days_ago
        ]

        save_visits(visits)

        unique_visitors = len(
            set(v.get("visitor_id") for v in visits if v.get("visitor_id"))
        )

        return {
            "ok": True,
            "visitor_id": visitor_id,
            "total_visits": len(visits),
            "unique_visitors": unique_visitors
        }

    except Exception as e:
        print("TRACK VISIT ERROR:", e)

        return {
            "ok": False,
            "message": str(e)
        }
@app.get("/admin-stats")
def admin_stats():
    visits = load_visits()

    total_visits = len(visits)

    unique_visitors = len(
        set(v.get("visitor_id") for v in visits if v.get("visitor_id"))
    )

    today_start = time.time() - 86400

    today_visits = len([
        v for v in visits
        if v.get("time", 0) >= today_start
    ])

    last_visit = None
    if visits:
        last_visit = max(v["time"] for v in visits)

    countries = list(
    set(
        v.get("country")
        for v in visits
        if v.get("country")
    )
    )

    return {
        "total_visits": total_visits,
        "unique_visitors": unique_visitors,
        "today_visits": today_visits,
        "last_visit": last_visit,
        "countries": countries
    }

@app.post("/execute-trade")
def execute_trade(request: TradeRequest):
    try:
        if request.token != ADMIN_TOKEN:
            print("❌ UNAUTHORIZED TRADE ATTEMPT")
            return {
                "ok": False,
                "message": "Unauthorized"
            }

        print(f"TRADE REQUEST RECEIVED -> {request.symbol} {request.action}")
        return {
            "ok": True,
            "message": f"Trade request received for {request.symbol} {request.action}",
            "symbol": request.symbol,
            "action": request.action
        }
    except Exception as e:
        print("TRADE ERROR:", e)
        return {
            "ok": False,
            "message": str(e)
        }


AUTO_TRADE_ENABLED = {
    "enabled": False
}

EXECUTION_MODE = {
    "mode": "paper"
}

@app.post("/execution-mode")
def set_execution_mode(payload: dict):

    mode = str(
        payload.get("mode", "paper")
    ).lower()

    allowed_modes = [
        "paper",
        "live"
    ]

    if mode not in allowed_modes:
        return {
            "ok": False,
            "message": "Invalid execution mode",
            "mode": EXECUTION_MODE["mode"]
        }

    EXECUTION_MODE["mode"] = mode

    print("EXECUTION MODE:", EXECUTION_MODE["mode"])

    return {
        "ok": True,
        "mode": EXECUTION_MODE["mode"]
    }

@app.post("/paper-auto-toggle")
def paper_auto_toggle(payload: dict):

    enabled = bool(
        payload.get("enabled", False)
    )

    AUTO_TRADE_ENABLED["enabled"] = enabled

    print(
        "AUTO TRADE STATE:",
        AUTO_TRADE_ENABLED["enabled"]
    )

    return {
        "status": "ok",
        "enabled": AUTO_TRADE_ENABLED["enabled"]
    }

LIVE_AUTO_TRADE_ENABLED = {
    "enabled": False
}

LIVE_ACCOUNT_STATE = {
    "connected": False,
    "mode": "demo",   # demo/live
    "broker": "ctrader"
}

LIVE_ACTIVE_ORDERS = {
    "EURUSD": None,
    "GOLD": None
}

LIVE_TRADE_HISTORY = []
MAX_LIVE_TRADE_HISTORY = 50
LIVE_BACKUP_FILE = os.path.join(
    os.path.dirname(__file__),
    "live_backup.json"
)

def get_persistable_live_active_orders():
    return {
        symbol: trade
        for symbol, trade in LIVE_ACTIVE_ORDERS.items()
        if trade and trade.get("source") == "broker"
    }

def save_live_backup():
    try:
        with open(LIVE_BACKUP_FILE, "w") as f:
            json.dump({
                "live_active_orders":
                    get_persistable_live_active_orders()
            }, f, indent=2)
    except Exception as e:
        print("LIVE BACKUP SAVE ERROR:", e)

def load_live_backup():
    if not os.path.exists(LIVE_BACKUP_FILE):
        return

    try:
        with open(LIVE_BACKUP_FILE, "r") as f:
            backup = json.load(f)

        active_orders = backup.get("live_active_orders", {})

        if not isinstance(active_orders, dict):
            return

        for symbol, trade in active_orders.items():
            if symbol in LIVE_ACTIVE_ORDERS and trade:
                LIVE_ACTIVE_ORDERS[symbol] = trade

        print("LIVE BACKUP LOADED:", get_persistable_live_active_orders())

    except Exception as e:
        print("LIVE BACKUP LOAD ERROR:", e)

load_live_backup()

def is_dev_request(request: Request):
    host = request.client.host if request.client else ""
    forwarded = request.headers.get("x-forwarded-for", "")
    origin = request.headers.get("origin", "")
    referer = request.headers.get("referer", "")
    local_hosts = ["127.0.0.1", "localhost", "::1"]
    local_origins = [
        "http://127.0.0.1:5501",
        "http://localhost:5501",
    ]

    return (
        host in local_hosts
        or forwarded.split(",")[0].strip() in local_hosts
        or origin in local_origins
        or any(origin.startswith(f"http://{local}") for local in local_hosts)
        or any(referer.startswith(f"http://{local}") for local in local_hosts)
    )

def sync_live_positions():
    if (
        not LIVE_ACCOUNT_STATE.get("connected")
        or LIVE_ACCOUNT_STATE.get("mode") != "live"
    ):
        return []

    try:
        positions = get_open_positions()
        print("LIVE POSITION SYNC:", positions)

        synced_symbols = set()

        for position in positions:
            symbol = str(position.get("symbol", "")).upper()

            if symbol not in LIVE_ACTIVE_ORDERS:
                continue

            synced_symbols.add(symbol)

            position_id = (
                position.get("position_id")
                or f"broker-{symbol}"
            )

            mirrored_order = {
                "order_id": f"broker-{position_id}",
                "position_id": position_id,
                "symbol": symbol,
                "side": str(position.get("side", "")).upper(),
                "mode": LIVE_ACCOUNT_STATE["mode"],
                "broker": LIVE_ACCOUNT_STATE["broker"],
                "volume": position.get("volume"),
                "entry": position.get("entry"),
                "opened_at": position.get("opened_at") or time.time(),
                "source": "broker",
                "result": "RUNNING",
                "raw": position.get("raw", position),
            }

            current_order = LIVE_ACTIVE_ORDERS.get(symbol)

            if current_order and current_order.get("position_id") == position_id:
                LIVE_ACTIVE_ORDERS[symbol] = {
                    **current_order,
                    **mirrored_order,
                    "opened_at": current_order.get("opened_at") or mirrored_order["opened_at"],
                }
                save_live_backup()
                continue

            if not current_order:
                LIVE_ACTIVE_ORDERS[symbol] = mirrored_order
                save_live_backup()
                print("BROKER POSITION MIRRORED:", mirrored_order)

        closed_at = time.time()

        for symbol, trade in list(LIVE_ACTIVE_ORDERS.items()):
            if not trade or trade.get("source") != "broker":
                continue

            if symbol in synced_symbols:
                continue

            closed_trade = {
                **trade,
                "result": "BROKER_CLOSED",
                "closed_at": closed_at,
                "note": "Broker position disappeared during read-only sync."
            }

            LIVE_TRADE_HISTORY.insert(0, closed_trade)
            LIVE_ACTIVE_ORDERS[symbol] = None
            save_live_backup()

            print("BROKER POSITION CLOSED:", closed_trade)

        del LIVE_TRADE_HISTORY[MAX_LIVE_TRADE_HISTORY:]

        return positions
    except Exception as e:
        print("LIVE POSITION SYNC ERROR:", e)
        return []

@app.post("/debug/set-broker-positions")
async def debug_set_broker_positions(
    payload: DebugBrokerPositionsRequest,
    request: Request
):
    if not is_dev_request(request):
        print(
            "DEBUG BROKER POSITIONS BLOCKED:",
            {
                "client_host": request.client.host if request.client else "",
                "origin": request.headers.get("origin", ""),
            }
        )

        return {
            "ok": False,
            "message": "Debug endpoint is only available locally"
        }

    received_payload = payload.model_dump()
    requested_positions = received_payload.get("positions")

    print("DEBUG BROKER POSITIONS PAYLOAD:", received_payload)

    if not isinstance(requested_positions, list):
        print("DEBUG BROKER POSITIONS VALIDATION ERROR:", received_payload)

        return {
            "ok": False,
            "message": "Expected payload shape: { positions: [] }"
        }

    positions = set_debug_open_positions(requested_positions)

    print("DEBUG BROKER POSITIONS:", positions)

    return {
        "ok": True,
        "positions": positions
    }

@app.post("/connect-ctrader")
def connect_ctrader(payload: dict):
    mode = str(payload.get("mode", "demo")).lower()

    if mode not in ["demo", "live"]:
        return {
            "ok": False,
            "message": "Invalid mode"
        }

    LIVE_ACCOUNT_STATE["connected"] = True
    LIVE_ACCOUNT_STATE["mode"] = mode
    LIVE_ACCOUNT_STATE["broker"] = "ctrader"

    print("CTRADER CONNECTED:", LIVE_ACCOUNT_STATE)

    return {
        "ok": True,
        "connected": LIVE_ACCOUNT_STATE["connected"],
        "mode": LIVE_ACCOUNT_STATE["mode"],
        "broker": LIVE_ACCOUNT_STATE["broker"]
    }


@app.post("/disconnect-ctrader")
def disconnect_ctrader():
    LIVE_AUTO_TRADE_ENABLED["enabled"] = False

    disconnected_at = time.time()

    for symbol, trade in list(LIVE_ACTIVE_ORDERS.items()):
        if not trade:
            continue

        order_id = trade.get("order_id")
        history_trade = None

        for item in LIVE_TRADE_HISTORY:
            if order_id and item.get("order_id") == order_id:
                history_trade = item
                break

        disconnected_trade = {
            **trade,
            "symbol": symbol,
            "result": "DISCONNECTED",
            "closed_at": disconnected_at,
            "note": "FlowSignal tracking stopped; broker positions were not auto-closed."
        }

        if history_trade:
            LIVE_TRADE_HISTORY.remove(history_trade)

        LIVE_TRADE_HISTORY.insert(0, disconnected_trade)

    del LIVE_TRADE_HISTORY[MAX_LIVE_TRADE_HISTORY:]

    LIVE_ACCOUNT_STATE["connected"] = False
    LIVE_ACCOUNT_STATE["mode"] = "demo"
    LIVE_ACCOUNT_STATE["broker"] = "ctrader"
    LIVE_ACTIVE_ORDERS["EURUSD"] = None
    LIVE_ACTIVE_ORDERS["GOLD"] = None
    save_live_backup()

    print("CTRADER DISCONNECTED")

    return {
        "ok": True,
        "connected": False,
        "mode": "demo",
        "broker": "ctrader",
        "live_auto_enabled": False
    }

@app.post("/live-auto-toggle")
def live_auto_toggle(payload: dict):
    enabled = bool(
        payload.get("enabled", False)
    )

    if enabled and not LIVE_ACCOUNT_STATE["connected"]:
        LIVE_AUTO_TRADE_ENABLED["enabled"] = False

        return {
            "status": "error",
            "enabled": False,
            "message": "Connect broker mode before enabling LIVE auto"
        }

    LIVE_AUTO_TRADE_ENABLED["enabled"] = enabled

    print(
        "LIVE AUTO TRADE STATE:",
        LIVE_AUTO_TRADE_ENABLED["enabled"]
    )

    return {
        "status": "ok",
        "enabled": LIVE_AUTO_TRADE_ENABLED["enabled"]
    }

@app.post("/execute-live-order")
def execute_live_order(payload: dict):

    if not LIVE_ACCOUNT_STATE["connected"]:

        return {
            "ok": False,
            "message": "No LIVE account connected"
        }

    if not LIVE_AUTO_TRADE_ENABLED["enabled"]:

        return {
            "ok": False,
            "message": "LIVE auto disabled"
        }

    symbol = payload.get("symbol")
    side = str(payload.get("side", "")).upper()

    if symbol not in LIVE_ACTIVE_ORDERS:
        return {
            "ok": False,
            "message": "Invalid symbol"
        }

    if side not in ["BUY", "SELL"]:
        return {
            "ok": False,
            "message": "Invalid side"
        }

    if LIVE_ACTIVE_ORDERS[symbol] is not None:
        return {
            "ok": False,
            "message": f"Active LIVE order already exists for {symbol}",
            "active_order": LIVE_ACTIVE_ORDERS[symbol]
        }

    result = place_market_order(
        symbol=symbol,
        side=side,
        volume=0.01
    )

    if not result.get("ok", False):
        return {
            "ok": False,
            "message": "LIVE order rejected",
            "result": result
        }

    order_id = str(uuid.uuid4())

    LIVE_ACTIVE_ORDERS[symbol] = {
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "mode": LIVE_ACCOUNT_STATE["mode"],
        "broker": LIVE_ACCOUNT_STATE["broker"],
        "volume": 0.01,
        "opened_at": time.time(),
        "result": result
    }

    LIVE_TRADE_HISTORY.insert(0, {
        **LIVE_ACTIVE_ORDERS[symbol],
        "result": "RUNNING"
    })

    del LIVE_TRADE_HISTORY[MAX_LIVE_TRADE_HISTORY:]

    return {
        "ok": True,
        "result": result,
        "active_order": LIVE_ACTIVE_ORDERS[symbol]
    }
