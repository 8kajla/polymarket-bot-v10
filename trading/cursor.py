from __future__ import annotations
from config import *
from utils import *

def initialize_cursor(state, feed):
    """
    Establish a live boundary exactly once.

    CRITICAL: an empty first API response must NOT leave cursor_ts as None.
    The previous implementation did that, which caused the main loop to call
    initialize_cursor() forever and never reach new-trade processing.

    We set the boundary to the current time. Any execution returned after this
    boundary is eligible for copying. Existing executions at startup are
    ignored, while a trade that happens during a subsequent poll is detected.
    """
    if state["cursor_ts"] is not None:
        return

    boundary = now()
    state["cursor_ts"] = boundary
    state["cursor_id"] = ""

    # Seed IDs only for executions that were already visible at startup.
    # Do NOT seed future executions because those must be copyable.
    startup_ids = []
    for x in feed:
        if trade_ts(x) <= boundary:
            startup_ids.append(trade_id(x))
            startup_ids.append(
                "ECON|" + economic_trade_fingerprint(x)
            )
    state["seen_ids"] = startup_ids[-20000:]

    if feed:
        newest = max(feed, key=lambda x: (trade_ts(x), trade_id(x)))
        age = max(0, boundary - trade_ts(newest))
        print("")
        print("🟡 LIVE CURSOR INITIALIZED")
        print(f"   Startup newest fill: {ist(trade_ts(newest))}")
        print(f"   Startup age: {age:.1f} seconds")
    else:
        print("")
        print("🟡 LIVE CURSOR INITIALIZED")
        print(f"   Boundary: {ist(boundary)}")
        print("   Startup feed was empty — waiting for the NEXT fill.")

    print("   Historical executions ignored.")
    print("")

    save(FILES["state"], state)

def is_new(state, t):
    k = trade_id(t)
    econ = "ECON|" + economic_trade_fingerprint(t)
    ts = trade_ts(t)

    if not k:
        return False

    if k in state["seen_ids"] or econ in state["seen_ids"]:
        return False

    cursor_ts = num(state["cursor_ts"])
    cursor_id = str(state["cursor_id"] or "")

    overlap = max(10, int(os.getenv("FEED_TIMESTAMP_OVERLAP", "15")))

    # Within the overlap window, a never-seen execution is eligible even if
    # its API timestamp is slightly older than the last cursor timestamp.
    # This handles delayed API publication without replaying startup history.
    if ts >= cursor_ts - overlap:
        return True

    # SELLs get a much wider replay window. A delayed REST/Activity SELL must
    # still be copied even if newer BUYs have already advanced the main cursor.
    # Startup executions are already seeded into seen_ids, so this does not
    # replay historical SELLs after a fresh start.
    if trade_side(t) == "SELL" and ts >= now() - SELL_REPLAY_SECONDS:
        return True

    return (ts, k) > (cursor_ts, cursor_id)

def advance_cursor(state, t):
    ts = trade_ts(t)
    k = trade_id(t)

    current = (
        num(state["cursor_ts"]),
        str(state["cursor_id"] or ""),
    )

    if (ts, k) > current:
        state["cursor_ts"] = ts
        state["cursor_id"] = k

    state["seen_ids"].append(k)
    state["seen_ids"].append(
        "ECON|" + economic_trade_fingerprint(t)
    )
    state["seen_ids"] = state["seen_ids"][-20000:]


# ============================================================
# RECONCILIATION
# ============================================================

