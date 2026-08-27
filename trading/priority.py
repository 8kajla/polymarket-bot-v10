from __future__ import annotations

"""Immediate execution path for live trader WebSocket events.

The polling/reporting loop is intentionally kept out of this path.  A live
RTDS event is consumed as soon as it arrives, deduplicated under one lock, and
sent to the existing ledger/copy engine.  REST polling remains a recovery and
reconciliation path.
"""

import threading
import time
import traceback

from config import WS_PRIORITY_COPY
from feeds.core import LIVE_WS_QUEUE
from feeds.market_book import prime_books
from feeds.race import snapshot as race_snapshot
from trading.cursor import is_new, advance_cursor
from trading.ledger import process_trade
from utils import now, trade_side, trade_asset


STATE_LOCK = threading.RLock()
STOP = threading.Event()
READY = threading.Event()
THREAD = None
STATUS = {
    "enabled": WS_PRIORITY_COPY,
    "processed": 0,
    "copied": 0,
    "rejected": 0,
    "queue_wait_ms_sum": 0.0,
    "queue_wait_ms_max": 0.0,
    "last_queue_wait_ms": 0.0,
    "last_error": "",
}


def _process_one(state, trade):
    received = float(trade.get("_ws_received_at") or now())
    observed = now()
    queue_wait_ms = max(0.0, (observed - received) * 1000.0)
    STATUS["last_queue_wait_ms"] = queue_wait_ms
    STATUS["queue_wait_ms_sum"] += queue_wait_ms
    STATUS["queue_wait_ms_max"] = max(STATUS["queue_wait_ms_max"], queue_wait_ms)

    with STATE_LOCK:
        if not READY.is_set() or state.get("cursor_ts") is None:
            return False
        if not is_new(state, trade):
            state["duplicates_ignored"] = state.get("duplicates_ignored", 0) + 1
            return False

        # Prime the CLOB subscription before the first local-book lookup.
        # The first execution may still use REST if its snapshot has not yet
        # arrived; subsequent executions on the asset use the local book.
        prime_books([trade])

        side = trade_side(trade)
        print("")
        print(f"🔔 FAST {side} | {trade.get('title') or trade.get('slug') or 'market'} | ${float(trade.get('size', 0) or 0) * float(trade.get('price', 0) or 0):.4f}")
        copy_started = now()
        copied = process_trade(state, trade, observed, "ws")
        copy_elapsed_ms = max(0.0, (now() - copy_started) * 1000.0)
        state["priority_events"] = state.get("priority_events", 0) + 1
        state["priority_queue_wait_ms"] = state.get("priority_queue_wait_ms", 0.0) + queue_wait_ms
        state["priority_copy_ms"] = state.get("priority_copy_ms", 0.0) + copy_elapsed_ms
        state["processing_count"] = state.get("processing_count", 0) + 1
        state["processing_sum_ms"] = state.get("processing_sum_ms", 0.0) + copy_elapsed_ms
        state["processing_max_ms"] = max(state.get("processing_max_ms", 0.0), copy_elapsed_ms)
        state["priority_copy_max_ms"] = max(state.get("priority_copy_max_ms", 0.0), copy_elapsed_ms)
        state["priority_last_copy_ms"] = copy_elapsed_ms

        if copied:
            STATUS["copied"] += 1
        else:
            STATUS["rejected"] += 1

        # A BUY/rejected SELL is still consumed from the main feed cursor in
        # the same way as the existing polling path. A failed SELL stays in
        # pending_sells and can be recovered by the REST path.
        if copied or side != "SELL":
            advance_cursor(state, trade)
        STATUS["processed"] += 1
        return copied


def _worker(state):
    while not STOP.is_set():
        try:
            trade = LIVE_WS_QUEUE.get(timeout=0.25)
        except Exception:
            continue
        try:
            _process_one(state, trade)
        except Exception as exc:
            STATUS["last_error"] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()


def start(state):
    global THREAD
    if not WS_PRIORITY_COPY:
        return
    if THREAD and THREAD.is_alive():
        return
    STOP.clear()
    READY.clear()
    THREAD = threading.Thread(target=_worker, args=(state,), name="priority-copy", daemon=True)
    THREAD.start()


def set_ready():
    READY.set()


def stop():
    STOP.set()
    try:
        if THREAD and THREAD.is_alive():
            THREAD.join(timeout=2)
    except Exception:
        pass


def status():
    out = dict(STATUS)
    out["race"] = race_snapshot()
    processed = max(1, int(out["processed"]))
    out["queue_wait_avg_ms"] = out["queue_wait_ms_sum"] / processed
    return out
