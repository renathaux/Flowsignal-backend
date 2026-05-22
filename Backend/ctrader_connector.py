# =========================
# 📡 CTRADER CONNECTOR
# =========================
import json
import os
import base64
import socket
import ssl
import struct
import uuid

from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

print("CTRADER ENV LOADED")

CONNECTED = {
    "status": False,
    "mode": "demo",   # demo or live
    "account_id": None
}

DEBUG_OPEN_POSITIONS = None

CTRADER_JSON_ENDPOINTS = {
    "demo": ("demo.ctraderapi.com", 5036),
    "live": ("live.ctraderapi.com", 5036),
}

PAYLOAD_APPLICATION_AUTH_REQ = 2100
PAYLOAD_APPLICATION_AUTH_RES = 2101
PAYLOAD_ACCOUNT_AUTH_REQ = 2102
PAYLOAD_ACCOUNT_AUTH_RES = 2103
PAYLOAD_SYMBOLS_LIST_REQ = 2114
PAYLOAD_SYMBOLS_LIST_RES = 2115
PAYLOAD_RECONCILE_REQ = 2124
PAYLOAD_RECONCILE_RES = 2125
PAYLOAD_ERROR_RES = 2142


def connect_account(account_id, mode="demo"):
    """
    Connect cTrader account
    """

    CONNECTED["status"] = True
    CONNECTED["mode"] = mode
    CONNECTED["account_id"] = account_id

    print(f"cTrader connected -> {mode}")

    return {
        "ok": True,
        "mode": mode,
        "account_id": account_id
    }


def disconnect_account():

    CONNECTED["status"] = False
    CONNECTED["account_id"] = None

    print("cTrader disconnected")

    return {
        "ok": True
    }


def place_market_order(
    symbol,
    side,
    volume,
    sl=None,
    tp=None
):
    """
    Future real cTrader execution
    """

    print(
        f"PLACE ORDER -> {symbol} {side}"
    )

    return {
        "ok": True,
        "symbol": symbol,
        "side": side,
        "volume": volume,
        "status": "SIMULATED"
    }


def get_open_positions():
    """
    Read-only cTrader DEMO position sync hook.
    Local debug mock positions are used first. Real broker sync only runs
    when CTRADER_* environment credentials are present and CTRADER_ENV=demo.
    This function never places or closes broker orders.
    """
    try:
        if DEBUG_OPEN_POSITIONS is not None:
            return normalize_positions(DEBUG_OPEN_POSITIONS)

        config = get_ctrader_config()

        if not config:
            print("CTRADER CONFIG MISSING")
            return []

        raw_positions = fetch_ctrader_open_positions(config)

        return normalize_positions(raw_positions)

    except Exception as e:
        print("CTRADER POSITIONS FETCH ERROR:", e)
        return []

def get_ctrader_config():
    config = {
        "client_id": os.getenv("CTRADER_CLIENT_ID"),
        "client_secret": os.getenv("CTRADER_CLIENT_SECRET"),
        "access_token": os.getenv("CTRADER_ACCESS_TOKEN"),
        "account_id": os.getenv("CTRADER_ACCOUNT_ID"),
        "env": os.getenv("CTRADER_ENV", "demo").lower(),
    }

    missing = [
        key for key in [
            "client_id",
            "client_secret",
            "access_token",
            "account_id",
        ]
        if not config.get(key)
    ]

    if missing:
        return None

    if config["env"] != "demo":
        print("CTRADER CONFIG MISSING: CTRADER_ENV must be demo for read-only demo sync")
        return None

    return config

def fetch_ctrader_open_positions(config):
    host, port = CTRADER_JSON_ENDPOINTS["demo"]
    account_id = int(config["account_id"])

    sock = open_ctrader_json_socket(host, port)

    try:
        send_ctrader_request(
            sock,
            PAYLOAD_APPLICATION_AUTH_REQ,
            {
                "clientId": config["client_id"],
                "clientSecret": config["client_secret"],
            },
            PAYLOAD_APPLICATION_AUTH_RES,
        )

        send_ctrader_request(
            sock,
            PAYLOAD_ACCOUNT_AUTH_REQ,
            {
                "ctidTraderAccountId": account_id,
                "accessToken": config["access_token"],
            },
            PAYLOAD_ACCOUNT_AUTH_RES,
        )

        symbol_map = fetch_ctrader_symbol_map(sock, account_id)

        reconcile = send_ctrader_request(
            sock,
            PAYLOAD_RECONCILE_REQ,
            {
                "ctidTraderAccountId": account_id,
                "returnProtectionOrders": False,
            },
            PAYLOAD_RECONCILE_RES,
        )

        positions = reconcile.get("payload", {}).get("position", [])
        normalized = [
            normalize_ctrader_position(position, symbol_map)
            for position in positions
        ]

        print("CTRADER POSITIONS FETCH OK:", normalized)

        return normalized

    finally:
        try:
            sock.close()
        except Exception:
            pass

def fetch_ctrader_symbol_map(sock, account_id):
    try:
        response = send_ctrader_request(
            sock,
            PAYLOAD_SYMBOLS_LIST_REQ,
            {
                "ctidTraderAccountId": account_id,
                "includeArchivedSymbols": False,
            },
            PAYLOAD_SYMBOLS_LIST_RES,
        )

        symbols = response.get("payload", {}).get("symbol", [])

        return {
            str(symbol.get("symbolId")): (
                symbol.get("symbolName")
                or symbol.get("name")
                or symbol.get("displayName")
            )
            for symbol in symbols
            if symbol.get("symbolId") is not None
        }

    except Exception as e:
        print("CTRADER SYMBOL MAP FETCH ERROR:", e)
        return {}

def normalize_ctrader_position(position, symbol_map):
    trade_data = position.get("tradeData", {})
    symbol_id = trade_data.get("symbolId")
    side = trade_data.get("tradeSide")

    return {
        "position_id": (
            position.get("positionId")
            or position.get("position_id")
            or position.get("id")
        ),
        "symbol": (
            symbol_map.get(str(symbol_id))
            or position.get("symbol")
            or position.get("symbolName")
            or str(symbol_id or "")
        ),
        "side": normalize_trade_side(side),
        "volume": trade_data.get("volume") or position.get("volume"),
        "entry": position.get("price") or position.get("entry"),
        "opened_at": (
            trade_data.get("openTimestamp")
            or position.get("opened_at")
        ),
        "raw": position,
    }

def normalize_trade_side(side):
    if side == 1 or str(side).upper() == "BUY":
        return "BUY"
    if side == 2 or str(side).upper() == "SELL":
        return "SELL"
    return str(side or "").upper()

def open_ctrader_json_socket(host, port):
    import certifi

    raw = socket.create_connection((host, port), timeout=8)

    context = ssl.create_default_context(
        cafile=certifi.where()
    )

    sock = context.wrap_socket(
        raw,
        server_hostname=host
    )
    sock.settimeout(8)

    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        "GET / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )

    sock.sendall(request.encode("ascii"))
    response = b""

    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk

    if b" 101 " not in response.split(b"\r\n", 1)[0]:
        raise RuntimeError(
            f"cTrader WebSocket handshake failed: {response[:120]!r}"
        )

    return sock

def send_ctrader_request(sock, payload_type, payload, expected_payload_type):
    client_msg_id = str(uuid.uuid4())
    message = {
        "clientMsgId": client_msg_id,
        "payloadType": payload_type,
        "payload": payload,
    }

    websocket_send_text(sock, json.dumps(message))

    while True:
        incoming = websocket_recv_text(sock)

        if not incoming:
            continue

        data = json.loads(incoming)
        incoming_type = data.get("payloadType")

        if incoming_type == PAYLOAD_ERROR_RES:
            raise RuntimeError(f"cTrader error response: {data}")

        if incoming_type == expected_payload_type:
            return data

def websocket_send_text(sock, text):
    payload = text.encode("utf-8")
    header = bytearray([0x81])
    length = len(payload)

    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))

    mask = os.urandom(4)
    masked_payload = bytes(
        byte ^ mask[index % 4]
        for index, byte in enumerate(payload)
    )

    sock.sendall(bytes(header) + mask + masked_payload)

def websocket_recv_text(sock):
    first = recv_exact(sock, 2)
    opcode = first[0] & 0x0F
    masked = bool(first[1] & 0x80)
    length = first[1] & 0x7F

    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]

    mask = recv_exact(sock, 4) if masked else None
    payload = recv_exact(sock, length) if length else b""

    if mask:
        payload = bytes(
            byte ^ mask[index % 4]
            for index, byte in enumerate(payload)
        )

    if opcode == 0x8:
        raise RuntimeError("cTrader WebSocket closed")
    if opcode != 0x1:
        return ""

    return payload.decode("utf-8")

def recv_exact(sock, length):
    data = b""

    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise RuntimeError("cTrader WebSocket connection closed")
        data += chunk

    return data

def normalize_positions(raw_positions):
    if not isinstance(raw_positions, list):
        return []

    positions = []

    for position in raw_positions:
        if not isinstance(position, dict):
            continue

        symbol = (
            position.get("symbol")
            or position.get("symbolName")
            or position.get("instrument")
        )
        side = (
            position.get("side")
            or position.get("tradeSide")
            or position.get("direction")
        )

        if not symbol or not side:
            continue

        positions.append({
            "position_id": (
                position.get("position_id")
                or position.get("positionId")
                or position.get("id")
            ),
            "symbol": str(symbol).upper(),
            "side": str(side).upper(),
            "volume": (
                position.get("volume")
                or position.get("quantity")
                or position.get("lotSize")
            ),
            "entry": (
                position.get("entry")
                or position.get("entry_price")
                or position.get("entryPrice")
            ),
            "opened_at": (
                position.get("opened_at")
                or position.get("openTime")
                or position.get("createdAt")
            ),
            "raw": position,
        })

    return positions

def set_debug_open_positions(positions):
    global DEBUG_OPEN_POSITIONS

    DEBUG_OPEN_POSITIONS = positions
    print("CTRADER DEBUG POSITIONS SET:", DEBUG_OPEN_POSITIONS)

    return normalize_positions(DEBUG_OPEN_POSITIONS)


def close_position(position_id):

    print(
        f"CLOSE POSITION -> {position_id}"
    )

    return {
        "ok": True
    }
