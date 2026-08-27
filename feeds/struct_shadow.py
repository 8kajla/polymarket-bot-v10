from __future__ import annotations

"""Optional Struct shadow feed.

Struct is never allowed to trigger a copy in this module.  When
STRUCT_API_KEY is configured it subscribes to the target wallet and records
confirmed trade arrival times for comparison with Polymarket RTDS.
"""

import json
import random
import threading
import time

import websocket

from config import STRUCT_API_KEY, STRUCT_SHADOW_ENABLED, WALLET, WS_RECONNECT_MAX
from feeds.race import record

STOP = threading.Event()
THREAD = None
STATUS = {
    "enabled": STRUCT_SHADOW_ENABLED,
    "connected": False,
    "received": 0,
    "confirmed": 0,
    "reconnects": 0,
    "last_message": 0.0,
    "last_error": "",
}

URL = "wss://api.struct.to/ws"


def _normalize(data):
    if not isinstance(data, dict):
        return None
    # Room messages are typically {type, room_id, data}; SDK examples also
    # refer to the trade payload as message. Accept both current wrappers.
    payload = data.get("data") or data.get("message") or data
    if not isinstance(payload, dict):
        return None
    trader = payload.get("trader")
    if isinstance(trader, dict):
        trader = trader.get("address")
    if trader and str(trader).lower() != WALLET:
        return None
    block = payload.get("block")
    if block in (None, "", 0, "0"):
        return None  # shadow confirmed path only
    side = str(payload.get("side") or "").upper()
    if side == "BUY":
        side = "BUY"
    elif side == "SELL":
        side = "SELL"
    else:
        return None
    ts = payload.get("confirmed_at") or payload.get("timestamp") or payload.get("received_at") or 0
    # Struct trade rows use shares_amount / amount_usd.
    shares = payload.get("shares_amount", payload.get("size", 0))
    price = payload.get("price", 0)
    return {
        "id": payload.get("trade_id") or payload.get("id") or payload.get("hash"),
        "hash": payload.get("hash"),
        "transactionHash": payload.get("hash"),
        "timestamp": ts,
        "side": side,
        "size": shares,
        "price": price,
        "asset": payload.get("position_id") or payload.get("asset_id") or payload.get("token_id", ""),
        "conditionId": payload.get("condition_id", ""),
        "outcome": payload.get("outcome", ""),
        "title": payload.get("question") or payload.get("market_slug") or "Struct trade",
        "slug": payload.get("market_slug", ""),
        "eventSlug": payload.get("event_slug", ""),
        "_feed_source": "struct",
    }


def _worker():
    attempt = 0
    while not STOP.is_set():
        ws = None
        try:
            ws = websocket.create_connection(
                URL + "?api-key=" + STRUCT_API_KEY,
                timeout=8,
                enable_multithread=True,
                origin="https://struct.to",
            )
            ws.settimeout(1.0)
            ws.send(json.dumps({
                "type": "join_room",
                "payload": {"room_id": "polymarket_trades"},
            }))
            ws.send(json.dumps({
                "type": "room_message",
                "payload": {
                    "room_id": "polymarket_trades",
                    "message": {
                        "action": "subscribe",
                        "traders": [WALLET],
                        "status": "all",
                    },
                },
            }))
            STATUS["connected"] = True
            STATUS["last_error"] = ""
            attempt = 0
            last_ping = time.time()
            while not STOP.is_set():
                if time.time() - last_ping >= 30:
                    ws.send(json.dumps({"type": "ping"}))
                    last_ping = time.time()
                try:
                    raw = ws.recv()
                except Exception as exc:
                    if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
                        continue
                    raise
                if raw is None:
                    raise RuntimeError("Struct websocket returned EOF")
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")
                if not str(raw).strip():
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                STATUS["received"] += 1
                STATUS["last_message"] = time.time()
                row = _normalize(msg)
                if row:
                    STATUS["confirmed"] += 1
                    record("struct", row, STATUS["last_message"])
        except Exception as exc:
            STATUS["connected"] = False
            STATUS["last_error"] = f"{type(exc).__name__}: {exc}"
            STATUS["reconnects"] += 1
            attempt += 1
        finally:
            STATUS["connected"] = False
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
        if not STOP.is_set():
            delay = min(WS_RECONNECT_MAX, max(1, 2 ** min(attempt, 5)))
            STOP.wait(delay + random.uniform(0, 0.5))


def start():
    global THREAD
    if not STRUCT_SHADOW_ENABLED:
        return
    if THREAD and THREAD.is_alive():
        return
    STOP.clear()
    THREAD = threading.Thread(target=_worker, name="struct-shadow", daemon=True)
    THREAD.start()


def stop():
    STOP.set()
    try:
        if THREAD and THREAD.is_alive():
            THREAD.join(timeout=2)
    except Exception:
        pass


def status():
    return dict(STATUS)
