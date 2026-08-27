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
from feeds.market_book import start as start_market_book, stop as stop_market_book
from trading.ledger import load_state, retry_pending_sells
from trading.cursor import initialize_cursor, is_new, advance_cursor
from trading.priority import start as start_copy_worker, stop as stop_copy_worker, submit_priority_trade, stats as priority_stats, state_lock
from api import fetch_positions, aggregate_positions, fetch_closed_positions
from reconciliation import reconcile
from resolution import resolve_cycle
from reporting import write_reports, print_status


def validate():
    if not WALLET:
        print("❌ POLYMARKET_WALLET is required")
        sys.exit(1)
    if not re.fullmatch(r"0x[a-f0-9]{40}", WALLET):
        print("❌ POLYMARKET_WALLET is not a valid EVM address")
        sys.exit(1)


def _submit_recovery_trades(state, feed):
    """REST recovery path. WebSocket trades are already on the priority path."""
    submitted = 0
    for t in sorted(feed, key=lambda x: (trade_ts(x), trade_id(x))):
        source = str(t.get("_feed_source", "rest"))
        if source == "ws" and t.get("_priority_queued", True):
            continue
        if not is_new(state, t):
            continue

        # REST is recovery only. Put it on exactly the same priority queue as WS.
        if submit_priority_trade(t, now(), source):
            submitted += 1

        # Once the execution is durably queued, advance the cursor. Failed
        # SELLs remain recoverable through the pending-sell ledger.
        advance_cursor(state, t)
    return submitted


def main():
    validate()
    state = load_state()
    init_audit_db()

    print("=" * 70)
    print("POLYMARKET COPY SIMULATOR V7.1")
    print("=" * 70)
    print(f"Wallet: {WALLET}")
    print(f"Paper capital: ${MAX_OPEN_CAPITAL:.2f}")
    print(f"Copy size: {COPY_NOTIONAL_FRACTION * 100:.0f}% of trader notional")
    print("Execution: Trader WS → priority copy queue → local CLOB book")
    print("Fallback: REST /book only when local CLOB book is unavailable")
    print("Position API: reconciliation only")
    print("PAPER TRADING ONLY")
    print("=" * 70)

    # Establish the startup boundary BEFORE starting live WS workers. This
    # guarantees historical executions cannot race into the copy path during
    # startup.
    if state.get("cursor_ts") is None:
        try:
            initial_feed, _ = fetch_recent_trades_fast(state)
        except Exception:
            initial_feed = []
        initialize_cursor(state, initial_feed)

    start_copy_worker(state)
    start_market_book()
    ws_ok = verify_websocket_dependency()
    start_live_ws()

    print(f"  ✓ Trader WebSocket: {'OK' if ws_ok else 'REST recovery only'}")
    print("  ✓ CLOB market WebSocket: local order-book cache")
    print("  ✓ Priority copy worker")
    print("  ✓ 10% sizing")
    print("  ✓ Paper ledger / resolution")
    print("  ✓ System ready")
    print("=" * 70)

    last_activity_check = 0.0
    last_report = 0.0
    last_position_rows = []
    last_position_diag = {"skipped": True}
    last_recon = (
        state["reconciliation"][-1]
        if state["reconciliation"]
        else {"matches": 0, "share_mismatches": 0, "missing_local": 0, "missing_api": 0}
    )

    try:
        while True:
            cycle_started = now()
            state["polls"] += 1
            state["last_poll"] = cycle_started

            try:
                feed, trade_diag = fetch_recent_trades_fast(state)
                submitted = _submit_recovery_trades(state, feed)

                # SELL retries are secondary bookkeeping/recovery and never
                # block the WebSocket copy worker.
                with state_lock():
                    retry_pending_sells(state, now())

                activity = []
                activity_diag = {"ok": False, "skipped": True}
                if now() - last_activity_check >= ACTIVITY_EVERY:
                    activity, activity_diag = fetch_activity_verify()
                    last_activity_check = now()

                newest = max(
                    (trade_ts(x) for x in feed),
                    default=state.get("last_feed_newest", 0),
                )
                age = now() - newest if newest else None
                status = (
                    "LIVE"
                    if age is not None and age <= STALE_AFTER
                    else "STALE"
                    if age is not None and age <= HARD_STALE_AFTER
                    else "API_CONNECTED_NO_RECENT_EXECUTIONS"
                    if trade_diag.get("ok")
                    else "API_ERROR"
                )

                feed_diag = {
                    "status": status,
                    "newest_age_seconds": age,
                    "new_this_cycle": submitted,
                    "trades_executions": len(feed),
                    "activity_executions": len(activity),
                    "newest_seen": newest,
                    "trade_diag": trade_diag,
                }
                state["last_feed_newest"] = newest

                if RECON_EVERY > 0 and now() - num(state.get("last_reconcile")) >= RECON_EVERY:
                    with state_lock():
                        last_position_rows, last_position_diag = fetch_positions()
                        last_recon = reconcile(
                            state,
                            last_position_rows,
                            aggregate_positions(last_position_rows),
                        )
                        state["last_reconcile"] = now()

                if now() - num(state.get("last_resolution_check")) >= RESOLUTION_EVERY:
                    with state_lock():
                        changed = resolve_cycle(state, feed)
                        state["last_resolution_check"] = now()
                        if changed:
                            save(FILES["state"], state)
                            print(f"🏁 RESOLUTION | settled {changed} position(s)")

                if now() - num(state.get("last_closed_check")) >= CLOSED_EVERY:
                    fetch_closed_positions()
                    state["last_closed_check"] = now()

                if now() - last_report >= REPORT_EVERY:
                    with state_lock():
                        write_reports(state, feed_diag, last_position_diag, last_recon)
                    last_report = now()

                print_status(state, feed_diag, last_recon)

            except requests.RequestException as exc:
                state["api_errors"] += 1
                print(f"[{ist()}] ⚠️ API | {type(exc).__name__}: {exc}")
            except Exception as exc:
                state["api_errors"] += 1
                print(f"[{ist()}] ❌ ERROR | {type(exc).__name__}: {exc}")
                traceback.print_exc()

            time.sleep(max(0, POLL_SECONDS - (now() - cycle_started)))

    finally:
        stop_copy_worker()
        stop_market_book()
        stop_live_ws()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        stop_copy_worker()
        stop_market_book()
        stop_live_ws()
        print("\nStopped.")
