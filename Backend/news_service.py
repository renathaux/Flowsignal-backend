import os
import re
import time
from datetime import datetime, timezone

import requests

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False


ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=ENV_PATH)

JBLANKED_DEFAULT_BASE_URL = "https://www.jblanked.com/news/api"
JBLANKED_CALENDAR_PATH = "/mql5/calendar/today/"
CALENDAR_CACHE_SECONDS = 60

SUPPORTED_SYMBOLS = {"EURUSD", "XAUUSD"}
SYMBOL_CURRENCIES = {
    "EURUSD": ["EUR", "USD"],
    "XAUUSD": ["USD"],
}
IMPACT_WEIGHT = {
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
}

LOWER_IS_BETTER_KEYWORDS = [
    "unemployment",
    "jobless",
    "claims",
    "claimant",
    "layoff",
]
BETTER_IS_BULLISH_KEYWORDS = [
    "cpi",
    "inflation",
    "pce",
    "payroll",
    "employment",
    "jobs",
    "gdp",
    "retail",
    "sales",
    "wage",
    "earnings",
    "pmi",
    "ism",
    "production",
]

_CALENDAR_CACHE = {
    "events": None,
    "fetched_at": 0,
}


def load_news_env_file():
    if not os.path.exists(ENV_PATH):
        return False

    loaded = False
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if key not in ["JBLANKED_API_KEY", "JBLANKED_NEWS_BASE_URL"]:
                    continue
                if os.getenv(key):
                    continue
                os.environ[key] = value.strip().strip('"').strip("'")
                loaded = True
    except OSError as exc:
        print("NEWS_ENV_LOAD_ERROR =", {
            "env_path": ENV_PATH,
            "error": str(exc),
        })
    return loaded


load_news_env_file()


def mask_secret(value):
    text = str(value or "")
    if len(text) <= 8:
        return "****" if text else ""
    return f"{text[:4]}...{text[-4:]}"


def preview_text(value, limit=1200):
    text = str(value or "")
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...<truncated>"


def normalize_symbol(symbol):
    normalized = str(symbol or "EURUSD").upper().replace("GOLD", "XAUUSD")
    return normalized if normalized in SUPPORTED_SYMBOLS else "EURUSD"


def utc_now():
    return datetime.now(timezone.utc)


def iso_timestamp(dt=None):
    return (dt or utc_now()).astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def get_jblanked_api_key():
    key = os.getenv("JBLANKED_API_KEY", "").strip()
    if not key or key == "PUT_YOUR_KEY_HERE":
        return None
    return key


def get_jblanked_calendar_url():
    base_url = (
        os.getenv("JBLANKED_NEWS_BASE_URL")
        or JBLANKED_DEFAULT_BASE_URL
    ).rstrip("/")
    return f"{base_url}{JBLANKED_CALENDAR_PATH}"


def safe_news_fallback(symbol, decision="NEWS_UNAVAILABLE"):
    normalized_symbol = normalize_symbol(symbol)
    event = (
        "No major news now"
        if decision == "NO_MAJOR_NEWS"
        else "News unavailable"
    )
    return {
        "symbol": normalized_symbol,
        "event": event,
        "currency": " / ".join(SYMBOL_CURRENCIES[normalized_symbol]),
        "impact": "LOW",
        "forecast": None,
        "actual": None,
        "previous": None,
        "release_time": None,
        "time": None,
        "time_until": "--",
        "news_bias": "Neutral",
        "effect": f"{normalized_symbol} Neutral",
        "score": 0,
        "decision": decision,
        "last_update": iso_timestamp(),
    }


def fetch_jblanked_calendar_events(force=False, timeout=8):
    api_key = get_jblanked_api_key()
    if not api_key:
        print("NEWS_JBLANKED_ENV_DEBUG =", {
            "env_path": ENV_PATH,
            "api_key_loaded": False,
            "api_key_masked": "",
            "base_url": os.getenv("JBLANKED_NEWS_BASE_URL"),
            "calendar_url": get_jblanked_calendar_url(),
        })
        raise RuntimeError("JBlanked API key missing")

    now_ts = time.time()
    cached_events = _CALENDAR_CACHE.get("events")
    cache_age = now_ts - float(_CALENDAR_CACHE.get("fetched_at") or 0)

    if (
        not force
        and cached_events is not None
        and cache_age < CALENDAR_CACHE_SECONDS
    ):
        return cached_events

    url = get_jblanked_calendar_url()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Api-Key {api_key}",
    }
    print("NEWS_JBLANKED_REQUEST_DEBUG =", {
        "url": url,
        "api_key_loaded": True,
        "api_key_masked": mask_secret(api_key),
        "headers": {
            "Content-Type": headers["Content-Type"],
            "Authorization": f"Api-Key {mask_secret(api_key)}",
        },
    })

    response = requests.get(
        url,
        headers=headers,
        timeout=timeout,
    )
    status_code = getattr(response, "status_code", None)
    print("NEWS_JBLANKED_RESPONSE_DEBUG =", {
        "url": url,
        "status_code": status_code,
    })

    if status_code != 200:
        body_preview = preview_text(getattr(response, "text", ""))
        print("NEWS_JBLANKED_RESPONSE_BODY_DEBUG =", {
            "status_code": status_code,
            "body_preview": body_preview,
        })
        if status_code in [401, 403]:
            raise RuntimeError(
                f"JBlanked auth issue: HTTP {status_code}"
            )
        if status_code == 404:
            raise RuntimeError(
                "JBlanked endpoint path is wrong: HTTP 404"
            )
        if status_code == 429:
            raise RuntimeError(
                "JBlanked rate limit/free usage issue: HTTP 429"
            )
        response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        print("NEWS_JBLANKED_JSON_DEBUG =", {
            "status_code": status_code,
            "body_preview": preview_text(getattr(response, "text", "")),
        })
        raise RuntimeError("JBlanked response was not valid JSON") from exc

    raw_events = extract_raw_events(payload)
    events = [event for event in map(normalize_event, raw_events) if event]
    print("NEWS_JBLANKED_PARSE_DEBUG =", {
        "payload_type": type(payload).__name__,
        "raw_events": len(raw_events),
        "normalized_events": len(events),
        "sample_raw_keys": (
            list(raw_events[0].keys())
            if raw_events and isinstance(raw_events[0], dict)
            else []
        ),
    })
    if not raw_events:
        print("NEWS_JBLANKED_EMPTY_RESPONSE_DEBUG =", {
            "status_code": status_code,
            "body_preview": preview_text(getattr(response, "text", "")),
        })

    _CALENDAR_CACHE["events"] = events
    _CALENDAR_CACHE["fetched_at"] = now_ts
    return events


def extract_raw_events(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    if any(
        key.lower() in ["event", "title", "name", "indicator"]
        for key in payload.keys()
    ):
        return [payload]

    for key in [
        "events",
        "data",
        "calendar",
        "results",
        "response",
        "today",
        "items",
    ]:
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = extract_raw_events(value)
            if nested:
                return nested

    dict_values = [value for value in payload.values() if isinstance(value, dict)]
    if dict_values and all(
        any(
            str(key).lower() in ["event", "title", "name", "indicator"]
            for key in value.keys()
        )
        for value in dict_values
    ):
        return dict_values

    for value in payload.values():
        nested = extract_raw_events(value)
        if nested:
            return nested

    return []


def normalize_event(raw):
    if not isinstance(raw, dict):
        return None

    event = first_present(
        raw,
        "event",
        "title",
        "name",
        "indicator",
        "description",
    )
    currency = first_present(
        raw,
        "currency",
        "country_currency",
        "ccy",
        "symbol",
        "country",
    )
    impact = first_present(
        raw,
        "impact",
        "importance",
        "priority",
        "strength",
    )
    forecast = first_present(raw, "forecast", "consensus", "expected")
    actual = first_present(raw, "actual", "actual_value", "value")
    previous = first_present(raw, "previous", "prior", "revised")
    release_time = first_present(
        raw,
        "release_time",
        "time",
        "date",
        "datetime",
        "timestamp",
        "calendar_time",
    )

    normalized_currency = normalize_currency(currency, raw)
    normalized_event = str(event or "").strip()

    if not normalized_event or not normalized_currency:
        return None

    parsed_time = normalize_event_time(release_time)
    return {
        "event": normalized_event,
        "currency": normalized_currency,
        "impact": normalize_impact(impact),
        "forecast": empty_to_none(forecast),
        "actual": empty_to_none(actual),
        "previous": empty_to_none(previous),
        "release_time": parsed_time,
        "time": parsed_time,
        "raw": raw,
    }


def first_present(source, *keys):
    for key in keys:
        value = source.get(key)
        if value in [None, "", "--", "N/A"]:
            value = source.get(key.upper())
        if value in [None, "", "--", "N/A"]:
            value = source.get(key.title())
        if value in [None, "", "--", "N/A"]:
            normalized_key = key.replace("_", "").lower()
            for source_key, source_value in source.items():
                source_normalized = str(source_key).replace("_", "").lower()
                if source_normalized == normalized_key:
                    value = source_value
                    break
        if value not in [None, "", "--", "N/A"]:
            return value
    return None


def normalize_currency(currency, raw=None):
    text = str(currency or "").upper().strip()
    if text in ["US", "USA", "UNITED STATES"]:
        return "USD"
    if text in ["EU", "EMU", "EUROZONE", "EURO AREA"]:
        return "EUR"
    if text in ["XAU", "GOLD", "XAUUSD"]:
        return "XAUUSD"

    match = re.search(r"\b[A-Z]{3}\b", text)
    if match:
        code = match.group(0)
        if code in ["USD", "EUR", "XAU"]:
            return "XAUUSD" if code == "XAU" else code

    country = str((raw or {}).get("country") or "").upper()
    if "UNITED STATES" in country:
        return "USD"
    if "EURO" in country or "GERMANY" in country or "FRANCE" in country:
        return "EUR"

    return text[:3] if len(text) >= 3 else ""


def normalize_impact(impact):
    text = str(impact or "").upper().strip()
    if "HIGH" in text or text in ["3", "RED"]:
        return "HIGH"
    if "MED" in text or text in ["2", "ORANGE"]:
        return "MEDIUM"
    if "LOW" in text or text in ["1", "YELLOW"]:
        return "LOW"
    return "LOW"


def normalize_event_time(value):
    if value in [None, "", "--", "N/A"]:
        return None

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
        for fmt in [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%m-%d-%Y %H:%M",
            "%m/%d/%Y %H:%M",
            "%b %d, %Y %H:%M",
            "%Y.%m.%d %H:%M",
        ]:
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def empty_to_none(value):
    if value in [None, "", "--", "N/A"]:
        return None
    return value


def parse_numeric(value):
    if value in [None, "", "--", "N/A"]:
        return None

    text = str(value).strip().replace(",", "")
    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    elif text.upper().endswith("K"):
        multiplier = 1_000.0
        text = text[:-1]
    elif text.upper().endswith("M"):
        multiplier = 1_000_000.0
        text = text[:-1]
    elif text.upper().endswith("B"):
        multiplier = 1_000_000_000.0
        text = text[:-1]

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None

    try:
        return float(match.group(0)) * multiplier
    except ValueError:
        return None


def event_is_relevant(event, symbol):
    normalized_symbol = normalize_symbol(symbol)
    return event.get("currency") in SYMBOL_CURRENCIES[normalized_symbol]


def get_event_time(event):
    return event.get("release_time") or event.get("time")


def choose_relevant_event(events, symbol, now=None):
    now = now or utc_now()
    relevant = [event for event in events if event_is_relevant(event, symbol)]

    if not relevant:
        return None

    upcoming = [
        event for event in relevant
        if not get_event_time(event) or get_event_time(event) >= now
    ]
    if upcoming:
        return min(
            upcoming,
            key=lambda event: (
                -IMPACT_WEIGHT.get(event.get("impact"), 1),
                get_event_time(event)
                or datetime.max.replace(tzinfo=timezone.utc),
            ),
        )

    released_with_actual = [
        event for event in relevant
        if event.get("actual") is not None
        and event.get("forecast") is not None
    ]
    if released_with_actual:
        return max(
            released_with_actual,
            key=lambda event: (
                IMPACT_WEIGHT.get(event.get("impact"), 1),
                get_event_time(event)
                or datetime.min.replace(tzinfo=timezone.utc),
            ),
        )

    return max(
        relevant,
        key=lambda event: (
            IMPACT_WEIGHT.get(event.get("impact"), 1),
            get_event_time(event)
            or datetime.min.replace(tzinfo=timezone.utc),
        ),
    )


def format_time_until(event_time, now=None):
    if not event_time:
        return "--"

    now = now or utc_now()
    seconds = int((event_time - now).total_seconds())
    released = seconds < 0
    seconds = abs(seconds)
    minutes = seconds // 60
    hours = minutes // 60
    days = hours // 24

    if days:
        value = f"{days}d {hours % 24}h"
    elif hours:
        value = f"{hours}h {minutes % 60}m"
    else:
        value = f"{minutes}m"

    return f"Released {value} ago" if released else value


def classify_currency_bias(event):
    actual = parse_numeric(event.get("actual"))
    forecast = parse_numeric(event.get("forecast"))

    if actual is None or forecast is None:
        return "Neutral", 0

    title = str(event.get("event") or "").lower()
    if any(keyword in title for keyword in LOWER_IS_BETTER_KEYWORDS):
        if actual > forecast:
            direction = -1
        elif actual < forecast:
            direction = 1
        else:
            direction = 0
    elif any(keyword in title for keyword in BETTER_IS_BULLISH_KEYWORDS):
        if actual > forecast:
            direction = 1
        elif actual < forecast:
            direction = -1
        else:
            direction = 0
    else:
        direction = 0

    currency = event.get("currency")
    if direction > 0:
        return f"Bullish {currency}", direction
    if direction < 0:
        return f"Bearish {currency}", direction
    return "Neutral", 0


def direction_for_symbol(symbol, currency, currency_direction):
    if not currency_direction:
        return 0

    if symbol == "EURUSD":
        if currency == "EUR":
            return currency_direction
        if currency == "USD":
            return -currency_direction

    if symbol == "XAUUSD" and currency == "USD":
        return -currency_direction

    return 0


def score_news_direction(event, symbol, now=None):
    now = now or utc_now()
    normalized_symbol = normalize_symbol(symbol)
    release_time = get_event_time(event)

    if (
        not release_time
        or release_time > now
        or event.get("actual") is None
        or event.get("forecast") is None
    ):
        return {
            "news_bias": "Waiting for release",
            "effect": f"{normalized_symbol} Neutral",
            "score": 0,
            "decision": "WAITING_FOR_ACTUAL_DATA",
        }

    news_bias, currency_direction = classify_currency_bias(event)
    symbol_direction = direction_for_symbol(
        normalized_symbol,
        event.get("currency"),
        currency_direction,
    )

    if symbol_direction > 0:
        effect = f"{normalized_symbol} BUY PRESSURE"
        score = 15
    elif symbol_direction < 0:
        effect = f"{normalized_symbol} SELL PRESSURE"
        score = -15
    else:
        effect = f"{normalized_symbol} Neutral"
        score = 0

    return {
        "news_bias": news_bias,
        "effect": effect,
        "score": max(-25, min(25, score)),
        "decision": "WAITING_FOR_ACTUAL_DATA" if score == 0 else "SUPPORTS_SETUP",
    }


def get_news_impact(symbol):
    normalized_symbol = normalize_symbol(symbol)
    now = utc_now()

    try:
        events = fetch_jblanked_calendar_events()
    except Exception as exc:
        print("NEWS_IMPACT_ERROR =", {
            "symbol": normalized_symbol,
            "error": str(exc),
        })
        return safe_news_fallback(
            normalized_symbol,
            decision="NEWS_UNAVAILABLE",
        )

    event = choose_relevant_event(events, normalized_symbol, now=now)
    if not event:
        return safe_news_fallback(
            normalized_symbol,
            decision="NO_MAJOR_NEWS",
        )

    score = score_news_direction(event, normalized_symbol, now=now)
    release_time = get_event_time(event)

    return {
        "symbol": normalized_symbol,
        "event": event.get("event"),
        "currency": event.get("currency"),
        "impact": event.get("impact"),
        "forecast": event.get("forecast"),
        "actual": event.get("actual"),
        "previous": event.get("previous"),
        "release_time": release_time.isoformat() if release_time else None,
        "time": release_time.isoformat() if release_time else None,
        "time_until": format_time_until(release_time, now=now),
        "news_bias": score["news_bias"],
        "effect": score["effect"],
        "score": score["score"],
        "decision": score["decision"],
        "last_update": iso_timestamp(now),
    }
