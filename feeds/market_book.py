from __future__ import annotations

"""Low-latency local CLOB order-book cache.

The normal copy path reads a locally maintained L2 book.  REST /book remains
an explicit fallback for a cold/stale/missing book so a WebSocket hiccup does
not break paper execution.
"""

import json
import os
import queue
import random
import threading
import time

import websocket

from config import *
from utils import num, parse_timestamp
from api import fetch_book as fetch_book_rest


BOOK_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BOOK_SUBSCRIBE_QUEUE: queue.Queue[str] = queue.Queue(maxsize=5000)
BOOK_STOP = threading.Event()
BOOK_THREAD = None
BOOK_LOCK = threading.RLock()

BOOKS: dict[str, dict] = {}
SUBSCRIBED: set[str] = set()
PENDING_SUBSCRIPTIONS: set[str] = set()

BOOK_STATUS = {
    "enabled": USE_WEBSOCKET,
    "available": False,
    "connected": False,
    "last_message": 0.0,
    "last_book_update": 0.0,
    "reconnects": 0,
    "stale_reconnects": 0,
    "snapshots": 0,
    "deltas": 0,
    "fallbacks": 0,
    "local_hits": 0,
    "missing": 0,
    "invalidated": 0,
    "subscribed_assets": 0,
    "last_error": "",
    "last_lookup_source": "NONE",
    "last_lookup_age_ms": None,
}

BOOK_MAX_AGE_SECONDS = max(
    0.25,
    float(os.getenv("BOOK_MAX_AGE_SECONDS", "5")),
)
BOOK_STALE_AFTER_SECONDS = max(
    15.0,
    float(os.getenv("BOOK_STALE_AFTER_SECONDS", "30")),
)
BOOK_PING_SECONDS = max(
    2.0,
    float(os.getenv("BOOK_PING_SECONDS", "10")),
)
BOOK_MAX_ASSETS = max(
    10,
    int(os.getenv("BOOK_MAX_ASSETS", "1000")),
)


def _clean_level(level):
    if isinstance(level, dict):
        price = num(level.get("price"), -1)
        size = num(level.get("size"), -1)
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        price = num(level[0], -1)
        size = num(level[1], -1)
    else:
        return None
    if price < 0 or size < 0:
        return None
    return price, size


def _normalize_snapshot(payload):
    bids = {}
    asks = {}
    for level in payload.get("bids") or []:
        clean = _clean_level(level)
        if clean and clean[1] > 0:
            bids[clean[0]] = clean[1]
    for level in payload.get("asks") or []:
        clean = _clean_level(level)
        if clean and clean[1] > 0:
            asks[clean[0]] = clean[1]
    return bids, asks


def _snapshot_public(book):
    return {
        "bids": [
            {"price": f"{p:.10f}".rstrip("0").rstrip("."), "size": f"{s:.10f}".rstrip("0").rstrip(".")}
            for p, s in sorted(book["bids"].items(), reverse=True)
            if s > 0
        ],
        "asks": [
            {"price": f"{p:.10f}".rstrip("0").rstrip("."), "size": f"{s:.10f}".rstrip("0").rstrip(".")}
            for p, s in sorted(book["asks"].items())
            if s > 0
        ],
    }


def _apply_book(payload):
    asset = str(payload.get("asset_id") or payload.get("tokenId") or "")
    if not asset:
        return False
    bids, asks = _normalize_snapshot(payload)
    received = time.time()
    event_ts = parse_timestamp(payload.get("timestamp"))
    with BOOK_LOCK:
        BOOKS[asset] = {
            "bids": bids,
            "asks": asks,
            "received_at": received,
            "event_ts": event_ts,
            "hash": payload.get("hash"),
            "source": "WS",
            "valid": True,
        }
        BOOK_STATUS["snapshots"] += 1
        BOOK_STATUS["last_book_update"] = received
    return True


def _apply_price_change(payload):
    changes = payload.get("price_changes")
    if changes is None:
        changes = payload.get("priceChanges")
    if not isinstance(changes, list):
        return 0
    received = time.time()
    event_ts = parse_timestamp(payload.get("timestamp"))
    applied = 0
    with BOOK_LOCK:
        for change in changes:
            if not isinstance(change, dict):
                continue
            asset = str(change.get("asset_id") or change.get("assetId") or "")
            if not asset:
                continue
            price = num(change.get("price"), -1)
            size = num(change.get("size"), -1)
            side = str(change.get("side") or "").upper()
            if price < 0 or size < 0 or side not in ("BUY", "SELL"):
                continue
            book = BOOKS.get(asset)
            if not book or book.get("source") != "WS" or not book.get("valid"):
                # A delta without a snapshot is unsafe to apply.
                continue
            levels = book["bids"] if side == "BUY" else book["asks"]
            if size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            book["received_at"] = received
            book["event_ts"] = event_ts
            book["hash"] = change.get("hash") or book.get("hash")
            applied += 1
        if applied:
            BOOK_STATUS["deltas"] += applied
            BOOK_STATUS["last_book_update"] = received
    return applied


def _invalidate_all():
    with BOOK_LOCK:
        for book in BOOKS.values():
            book["valid"] = False
        BOOK_STATUS["invalidated"] += len(BOOKS)


def subscribe_assets(assets):
    """Request market-book subscriptions without blocking the copy path."""
    if not assets:
        return
    with BOOK_LOCK:
        for raw in assets:
            asset = str(raw or "").strip()
            if not asset or asset in SUBSCRIBED or asset in PENDING_SUBSCRIPTIONS:
                continue
            if len(SUBSCRIBED) + len(PENDING_SUBSCRIPTIONS) >= BOOK_MAX_ASSETS:
                break
            PENDING_SUBSCRIPTIONS.add(asset)
            try:
                BOOK_SUBSCRIBE_QUEUE.put_nowait(asset)
            except queue.Full:
                PENDING_SUBSCRIPTIONS.discard(asset)
                break


def _drain_pending():
    assets = []
    while True:
        try:
            assets.append(BOOK_SUBSCRIBE_QUEUE.get_nowait())
        except queue.Empty:
            break
    return assets


def _send_subscription(ws, assets, initial=False):
    if not assets:
        return
    unique = []
    with BOOK_LOCK:
        for asset in assets:
            if asset in SUBSCRIBED:
                PENDING_SUBSCRIPTIONS.discard(asset)
                continue
            SUBSCRIBED.add(asset)
            PENDING_SUBSCRIPTIONS.discard(asset)
            unique.append(asset)
        BOOK_STATUS["subscribed_assets"] = len(SUBSCRIBED)
    if unique:
        payload = {
            "assets_ids": unique,
            "type": "market",
            "custom_feature_enabled": True,
        }
        if not initial:
            payload["operation"] = "subscribe"
        ws.send(json.dumps(payload))


def _handle_message(msg):
    if not isinstance(msg, dict):
        return
    event_type = str(msg.get("event_type") or msg.get("type") or "").lower()
    if event_type == "book":
        _apply_book(msg)
    elif event_type == "price_change":
        _apply_price_change(msg)
    elif event_type == "best_bid_ask":
        # BBO is diagnostic only; the full local L2 book remains authoritative.
        BOOK_STATUS["last_message"] = time.time()


def _worker():
    global BOOK_THREAD
    attempt = 0
    while not BOOK_STOP.is_set():
        ws = None
        try:
            ws = websocket.create_connection(
                BOOK_WS_URL,
                timeout=max(5, FAST_TIMEOUT),
                enable_multithread=True,
                origin="https://polymarket.com",
            )
            ws.settimeout(1.0)
            BOOK_STATUS["available"] = True
            BOOK_STATUS["connected"] = True
            BOOK_STATUS["last_error"] = ""
            connection_last_data = time.time()
            last_ping = connection_last_data
            _invalidate_all()
            with BOOK_LOCK:
                assets = list(SUBSCRIBED)
                # Existing subscriptions must be replayed after reconnect.
                SUBSCRIBED.clear()
            _send_subscription(ws, assets, initial=True)
            attempt = 0

            while not BOOK_STOP.is_set():
                pending = _drain_pending()
                if pending:
                    _send_subscription(ws, pending)

                current = time.time()
                if current - last_ping >= BOOK_PING_SECONDS:
                    ws.send("PING")
                    last_ping = current

                with BOOK_LOCK:
                    have_assets = bool(SUBSCRIBED)
                if have_assets and current - connection_last_data >= BOOK_STALE_AFTER_SECONDS:
                    BOOK_STATUS["stale_reconnects"] += 1
                    raise RuntimeError(
                        f"CLOB book data stale for {current - connection_last_data:.1f}s"
                    )

                try:
                    raw = ws.recv()
                except Exception as exc:
                    text = str(exc).lower()
                    if "timeout" in text or "timed out" in text:
                        continue
                    raise

                if raw is None:
                    raise RuntimeError("CLOB market websocket returned EOF")
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")
                if raw in ("", "PONG", "pong"):
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                connection_last_data = time.time()
                BOOK_STATUS["last_message"] = connection_last_data
                _handle_message(msg)

        except Exception as exc:
            BOOK_STATUS["connected"] = False
            BOOK_STATUS["available"] = True
            BOOK_STATUS["last_error"] = f"{type(exc).__name__}: {exc}"
            BOOK_STATUS["reconnects"] += 1
            attempt += 1
        finally:
            BOOK_STATUS["connected"] = False
            try:
                if ws:
                    ws.close()
            except Exception:
                pass

        if not BOOK_STOP.is_set():
            delay = min(WS_RECONNECT_MAX, max(1, 2 ** min(attempt, 5)))
            BOOK_STOP.wait(delay + random.uniform(0, 0.5))


def start_market_book():
    global BOOK_THREAD
    if not USE_WEBSOCKET:
        return
    if BOOK_THREAD and BOOK_THREAD.is_alive():
        return
    BOOK_STOP.clear()
    BOOK_THREAD = threading.Thread(target=_worker, name="polymarket-clob-book", daemon=True)
    BOOK_THREAD.start()


def stop_market_book():
    BOOK_STOP.set()
    try:
        if BOOK_THREAD and BOOK_THREAD.is_alive():
            BOOK_THREAD.join(timeout=2)
    except Exception:
        pass


def get_local_book(asset):
    asset = str(asset or "")
    if not asset:
        return None, "MISSING", None
    with BOOK_LOCK:
        book = BOOKS.get(asset)
        if not book or book.get("source") != "WS" or not book.get("valid"):
            return None, "MISSING", None
        age = time.time() - num(book.get("received_at"), 0)
        if age > BOOK_MAX_AGE_SECONDS:
            return None, "STALE", age
        BOOK_STATUS["local_hits"] += 1
        return _snapshot_public(book), "WS", age


def ensure_asset(asset):
    """Ensure an asset is subscribed to the live CLOB book stream.

    This compatibility helper is used by the execution engine so the hot path
    can request a subscription without touching REST.
    """
    asset = str(asset or "").strip()
    if asset:
        subscribe_assets([asset])
    return asset


def get_book(asset):
    """Return a fresh local WS book, or None when unavailable/stale."""
    book, source, _age = get_local_book(asset)
    return book if source == "WS" else None


def note_fallback():
    """Record that an execution lookup had to fall back to REST."""
    with BOOK_LOCK:
        BOOK_STATUS["fallbacks"] += 1


def fetch_book(asset):
    """WS-first local-book lookup; briefly wait for a fresh WS snapshot before REST."""
    subscribe_assets([asset])
    deadline = time.time() + (BOOK_LOOKUP_GRACE_MS / 1000.0)
    while True:
        book, source, age = get_local_book(asset)
        if book is not None:
            BOOK_STATUS["last_lookup_source"] = "WS"
            BOOK_STATUS["last_lookup_age_ms"] = None if age is None else age * 1000.0
            return book
        if time.time() >= deadline or BOOK_LOOKUP_GRACE_MS <= 0:
            break
        time.sleep(min(0.005, max(0.0, deadline - time.time())))

    BOOK_STATUS["fallbacks"] += 1
    if source == "MISSING":
        BOOK_STATUS["missing"] += 1
    rest = fetch_book_rest(asset)
    if rest:
        BOOK_STATUS["last_lookup_source"] = "REST_FALLBACK"
        BOOK_STATUS["last_lookup_age_ms"] = None if age is None else age * 1000.0
        return rest
    BOOK_STATUS["last_lookup_source"] = "FAILED"
    BOOK_STATUS["last_lookup_age_ms"] = None if age is None else age * 1000.0
    return None


def prime_books(rows):
    subscribe_assets([r.get("asset") or r.get("assetId") or r.get("tokenId") for r in rows if isinstance(r, dict)])


def status():
    with BOOK_LOCK:
        subscribed = len(SUBSCRIBED)
    return {
        **BOOK_STATUS,
        "subscribed_assets": subscribed,
        "max_age": BOOK_MAX_AGE_SECONDS,
    }
