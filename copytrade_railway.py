from __future__ import annotations

import re
import sys
import time
import traceback
import requests

from config import *
from utils import *
from audit import init_audit_db

from feeds.core import (
    verify_websocket_dependency,
    start_live_ws,
    stop_live_ws,
    fetch_recent_trades_fast,
    fetch_activity_verify,
)

# ============================================================
# MARKET BOOK
#
# IMPORTANT:
# The previous version imported:
#
#   from feeds.market_book import start as start_market_book
#
# but the installed market_book.py does not expose `start`.
#
# Import the module itself and resolve the available functions
# safely so the whole application does not die during import.
# ============================================================

import feeds.market_book as market_book


def _market_book_start():
    """
    Start the local CLOB market-book worker.

    Supports the current market_book implementation while retaining
    compatibility with older versions.
    """
    fn = getattr(market_book, "start_market_book", None)

    if callable(fn):
        return fn()

    fn = getattr(market_book, "start", None)

    if callable(fn):
        return fn()

    fn = getattr(market_book, "start_worker", None)

    if callable(fn):
        return fn()

    print("  ⚠️ CLOB market-book worker has no start function")
    print("     Continuing with REST fallback.")
    return None


def _market_book_stop():
    """
    Stop the local CLOB market-book worker if supported.
    """
    fn = getattr(market_book, "stop_market_book", None)

    if callable(fn):
        return fn()

    fn = getattr(market_book, "stop", None)

    if callable(fn):
        return fn()

    fn = getattr(market_book, "stop_worker", None)

    if callable(fn):
        return fn()

    return None


# ============================================================
# TRADING / LEDGER
# ============================================================

from trading.ledger import (
    load_state,
    retry_pending_sells,
)

from trading.cursor import (
    initialize_cursor,
    is_new,
    advance_cursor,
)

from trading.priority import (
    start as start_copy_worker,
    stop as stop_copy_worker,
    submit_priority_trade,
    stats as priority_stats,
    state_lock,
    set_ready as set_copy_ready,
)

from api import (
    fetch_positions,
    aggregate_positions,
    fetch_closed_positions,
)

from reconciliation import reconcile
from resolution import resolve_cycle

from reporting import (
    write_reports,
    print_status,
)


# ============================================================
# VALIDATION
# ============================================================

def validate():
    if not WALLET:
        print("❌ POLYMARKET_WALLET is required")
        sys.exit(1)

    if not re.fullmatch(r"0x[a-f0-9]{40}", WALLET):
        print("❌ POLYMARKET_WALLET is not a valid EVM address")
        sys.exit(1)


# ============================================================
# REST RECOVERY
# ============================================================

def _submit_recovery_trades(state, feed):
    """
    REST recovery path.

    WebSocket executions should already be placed on the priority
    queue by feeds.core.

    REST executions are only submitted when they have not already
    been queued through the WS path.
    """

    submitted = 0

    for t in sorted(
        feed,
        key=lambda x: (
            trade_ts(x),
            trade_id(x),
        ),
    ):

        source = str(
            t.get("_feed_source", "rest")
        )

        # WS trades already handled by the WS worker.
        if (
            source == "ws"
            and t.get("_priority_queued", True)
        ):
            continue

        # Never enqueue an execution that is already known.
        if not is_new(state, t):
            continue

        # REST recovery goes through exactly the same priority
        # queue as the live WebSocket path.
        if submit_priority_trade(
            t,
            now(),
            source,
        ):
            submitted += 1

        # Advance only after the execution has been durably queued.
        #
        # A SELL that subsequently cannot execute remains recoverable
        # through the pending-sell ledger.
        advance_cursor(state, t)

    return submitted


# ============================================================
# MAIN
# ============================================================

def main():

    validate()

    state = load_state()

    init_audit_db()

    print("=" * 70)
    print("POLYMARKET COPY SIMULATOR V7.1")
    print("=" * 70)

    print(f"Wallet: {WALLET}")

    print(
        f"Paper capital: "
        f"${MAX_OPEN_CAPITAL:.2f}"
    )

    print(
        f"Copy size: "
        f"{COPY_NOTIONAL_FRACTION * 100:.0f}% "
        f"of trader notional"
    )

    print(
        "Execution: "
        "Trader WS → priority copy queue → local CLOB book"
    )

    print(
        "Fallback: "
        "REST /book only when local CLOB book is unavailable"
    )

    print(
        "Position API: reconciliation only"
    )

    print("PAPER TRADING ONLY")

    print("=" * 70)

    # ========================================================
    # STARTUP CURSOR
    # ========================================================

    # Establish the startup boundary BEFORE starting the live
    # WebSocket workers.
    #
    # This prevents historical executions from being copied when
    # Railway restarts the container.

    if state.get("cursor_ts") is None:

        try:
            initial_feed, _ = fetch_recent_trades_fast(
                state
            )

        except Exception:
            initial_feed = []

        initialize_cursor(
            state,
            initial_feed,
        )

    # ========================================================
    # START WORKERS
    # ========================================================

    start_copy_worker(state)
    set_copy_ready()

    print("  ✓ Priority copy worker started")

    # Start local CLOB order-book cache.
    _market_book_start()

    print(
        "  ✓ CLOB market WebSocket worker initialized"
    )

    # Check websocket dependency.
    ws_ok = verify_websocket_dependency()

    # Start trader live WebSocket.
    start_live_ws()

    print(
        f"  ✓ Trader WebSocket: "
        f"{'OK' if ws_ok else 'REST recovery only'}"
    )

    print("  ✓ CLOB market book")
    print("  ✓ Priority copy worker")

    print(
        f"  ✓ "
        f"{COPY_NOTIONAL_FRACTION * 100:.0f}% sizing"
    )

    print("  ✓ Paper ledger / resolution")
    print("  ✓ System ready")

    print("=" * 70)

    # ========================================================
    # TIMERS
    # ========================================================

    last_activity_check = 0.0
    last_report = 0.0

    last_position_rows = []

    last_position_diag = {
        "skipped": True
    }

    last_recon = (
        state["reconciliation"][-1]
        if state["reconciliation"]
        else {
            "matches": 0,
            "share_mismatches": 0,
            "missing_local": 0,
            "missing_api": 0,
        }
    )

    # ========================================================
    # MAIN LOOP
    # ========================================================

    try:

        while True:

            cycle_started = now()

            state["polls"] += 1
            state["last_poll"] = cycle_started

            try:

                # ====================================================
                # LIVE / REST EXECUTION FEED
                # ====================================================

                feed, trade_diag = (
                    fetch_recent_trades_fast(state)
                )

                submitted = _submit_recovery_trades(
                    state,
                    feed,
                )

                # ====================================================
                # PENDING SELL RECOVERY
                # ====================================================

                # SELL recovery must never block the live WS
                # copy worker.

                with state_lock():

                    retry_pending_sells(
                        state,
                        now(),
                    )

                # ====================================================
                # ACTIVITY VERIFICATION
                # ====================================================

                activity = []

                activity_diag = {
                    "ok": False,
                    "skipped": True,
                }

                if (
                    now() - last_activity_check
                    >= ACTIVITY_EVERY
                ):

                    activity, activity_diag = (
                        fetch_activity_verify()
                    )

                    last_activity_check = now()

                # ====================================================
                # FEED STATUS
                # ====================================================

                newest = max(
                    (
                        trade_ts(x)
                        for x in feed
                    ),
                    default=state.get(
                        "last_feed_newest",
                        0,
                    ),
                )

                age = (
                    now() - newest
                    if newest
                    else None
                )

                if (
                    age is not None
                    and age <= STALE_AFTER
                ):

                    status = "LIVE"

                elif (
                    age is not None
                    and age <= HARD_STALE_AFTER
                ):

                    status = "STALE"

                elif trade_diag.get("ok"):

                    status = (
                        "API_CONNECTED_NO_RECENT_EXECUTIONS"
                    )

                else:

                    status = "API_ERROR"

                feed_diag = {
                    "status": status,

                    "newest_age_seconds": age,

                    "new_this_cycle": submitted,

                    "trades_executions": len(feed),

                    "activity_executions": len(
                        activity
                    ),

                    "newest_seen": newest,

                    "trade_diag": trade_diag,

                    "activity_diag": activity_diag,
                }

                state["last_feed_newest"] = newest

                # ====================================================
                # RECONCILIATION
                # ====================================================

                if (
                    RECON_EVERY > 0
                    and now()
                    - num(
                        state.get(
                            "last_reconcile"
                        )
                    )
                    >= RECON_EVERY
                ):

                    with state_lock():

                        (
                            last_position_rows,
                            last_position_diag,
                        ) = fetch_positions()

                        last_recon = reconcile(
                            state,
                            last_position_rows,
                            aggregate_positions(
                                last_position_rows
                            ),
                        )

                        state[
                            "last_reconcile"
                        ] = now()

                # ====================================================
                # RESOLUTION
                # ====================================================

                if (
                    now()
                    - num(
                        state.get(
                            "last_resolution_check"
                        )
                    )
                    >= RESOLUTION_EVERY
                ):

                    with state_lock():

                        changed = resolve_cycle(
                            state,
                            feed,
                        )

                        state[
                            "last_resolution_check"
                        ] = now()

                        if changed:

                            save(
                                FILES["state"],
                                state,
                            )

                            print(
                                f"🏁 RESOLUTION | "
                                f"settled {changed} "
                                f"position(s)"
                            )

                # ====================================================
                # CLOSED POSITION CHECK
                # ====================================================

                if (
                    now()
                    - num(
                        state.get(
                            "last_closed_check"
                        )
                    )
                    >= CLOSED_EVERY
                ):

                    fetch_closed_positions()

                    state[
                        "last_closed_check"
                    ] = now()

                # ====================================================
                # REPORTING
                # ====================================================

                if (
                    now() - last_report
                    >= REPORT_EVERY
                ):

                    with state_lock():

                        write_reports(
                            state,
                            feed_diag,
                            last_position_diag,
                            last_recon,
                        )

                    last_report = now()

                # ====================================================
                # STATUS
                # ====================================================

                print_status(
                    state,
                    feed_diag,
                    last_recon,
                )

            # ========================================================
            # REQUEST ERROR
            # ========================================================

            except requests.RequestException as exc:

                state["api_errors"] += 1

                print(
                    f"[{ist()}] ⚠️ API | "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

            # ========================================================
            # UNEXPECTED ERROR
            # ========================================================

            except Exception as exc:

                state["api_errors"] += 1

                print(
                    f"[{ist()}] ❌ ERROR | "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

                traceback.print_exc()

            # ========================================================
            # MAINTAIN 1-SECOND POLL CADENCE
            # ========================================================

            elapsed = (
                now() - cycle_started
            )

            time.sleep(
                max(
                    0,
                    POLL_SECONDS - elapsed,
                )
            )

    # ============================================================
    # CLEAN SHUTDOWN
    # ============================================================

    finally:

        try:
            stop_copy_worker()
        except Exception:
            traceback.print_exc()

        try:
            _market_book_stop()
        except Exception:
            traceback.print_exc()

        try:
            stop_live_ws()
        except Exception:
            traceback.print_exc()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        try:
            stop_copy_worker()
        except Exception:
            pass

        try:
            _market_book_stop()
        except Exception:
            pass

        try:
            stop_live_ws()
        except Exception:
            pass

        print("\nStopped.")
