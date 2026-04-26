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

CACHE_SECONDS = 60
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

    data = PANEL_CACHE["data"].copy()

    data["_meta"] = {
        "source": "shared_cache",
        "cache_age_seconds": round(age, 1),
        "refresh_seconds": CACHE_SECONDS,
        "error": None
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