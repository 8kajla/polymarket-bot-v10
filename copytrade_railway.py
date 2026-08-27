from __future__ import annotations
import re, sys, time, traceback, requests
from config import *
from utils import *
from audit import init_audit_db, audit_event
from feeds.core import verify_websocket_dependency, start_live_ws, stop_live_ws, fetch_recent_trades_fast, fetch_activity_verify
from feeds.market_book import start_market_book, stop_market_book, prime_books, status as market_book_status
from feeds.struct_shadow import start as start_struct_shadow, stop as stop_struct_shadow, status as struct_status
from trading.priority import start as start_priority_worker, set_ready as set_priority_ready, stop as stop_priority_worker, STATE_LOCK, status as priority_status
from trading.ledger import load_state, open_capital, process_trade, retry_pending_sells
from trading.cursor import initialize_cursor, is_new, advance_cursor
from api import fetch_positions, aggregate_positions, fetch_closed_positions, fetch_redeemable_positions
from reconciliation import reconcile
from resolution import resolve_cycle
from reporting import write_reports, print_status

def validate():
    if not WALLET:
        print("❌ POLYMARKET_WALLET is required"); sys.exit(1)
    if not re.fullmatch(r"0x[a-f0-9]{40}",WALLET):
        print("❌ POLYMARKET_WALLET is not a valid EVM address"); sys.exit(1)

def main():
    validate()
    state=load_state()
    init_audit_db()
    print("="*70); print("POLYMARKET COPY SIMULATOR V7.0"); print("="*70)
    print(f"Wallet: {WALLET}"); print(f"Paper capital: ${MAX_OPEN_CAPITAL:.2f}")
    print(f"Polling: every {POLL_SECONDS:.1f}s")
    print(f"Fast mode: {FAST_MODE} | Recent window: {RECENT_SECONDS}s | Reconcile every: {RECON_EVERY}s | Resolution every: {RESOLUTION_EVERY}s")
    print("Live feed: WS primary + ACTIVITY/TRADES REST recovery")
    print("BUY: ask VWAP | SELL: bid VWAP"); print("Position API: reconciliation only"); print("PAPER TRADING ONLY")
    print("="*70)
    print("SELF-TEST")
    print(f"  ✓ Paper ledger: ${MAX_OPEN_CAPITAL:.2f}")
    print(f"  ✓ WebSocket dependency: {'OK' if verify_websocket_dependency() else 'FAILED — REST recovery only'}")
    start_live_ws()
    start_market_book()
    start_struct_shadow()
    print("  ✓ Trader WebSocket worker started")
    print("  ✓ CLOB market-book WebSocket worker started")
    print(f"  ✓ Struct shadow feed: {'ENABLED' if STRUCT_SHADOW_ENABLED else 'OFF (set STRUCT_API_KEY to enable)'}")
    print("  ✓ BUY engine loaded")
    print("  ✓ SELL engine loaded")
    print("  ✓ SELL diagnostics loaded")
    print("  ✓ Resolution engine loaded")
    print("  ✓ System ready — PAPER TRADING")
    print("="*70)

    last_activity_check=0.0; last_report=0.0
    last_position_rows=[]; last_position_diag={"skipped":True}
    last_recon=state["reconciliation"][-1] if state["reconciliation"] else {"matches":0,"share_mismatches":0,"missing_local":0,"missing_api":0}

    try:
        while True:
            started=now(); state["polls"]+=1; state["last_poll"]=started
            try:
                feed,trade_diag=fetch_recent_trades_fast(state)

                if state["cursor_ts"] is None:
                    initialize_cursor(state,feed)
                    last_position_rows,last_position_diag=fetch_positions()
                    last_recon=reconcile(state,last_position_rows,aggregate_positions(last_position_rows))
                    state["last_reconcile"]=now()
                    feed_diag={"status":"STARTING","newest_age_seconds":None,"new_this_cycle":0,
                               "trades_executions":len(feed),"activity_executions":0,"newest_seen":max((trade_ts(t) for t in feed),default=0)}
                    write_reports(state,feed_diag,last_position_diag,last_recon)
                    # Prime the CLOB book subscriptions from the startup feed,
                    # then release the live WS queue to the immediate copy worker.
                    prime_books(feed)
                    start_priority_worker(state)
                    set_priority_ready()
                    print_status(state,feed_diag,last_recon,force=True)
                else:
                    new_trades=[t for t in feed if is_new(state,t)]
                    new_trades.sort(key=lambda x:(trade_ts(x),trade_id(x)))
                    for t in new_trades:
                        side=trade_side(t)
                        with STATE_LOCK:
                            # The priority WS worker may have consumed this trade
                            # after new_trades was built but before recovery got
                            # the lock. Re-check here to prevent FAST+RECOVERY
                            # double copies.
                            if not is_new(state, t):
                                state["recovery_duplicates_skipped"] = state.get("recovery_duplicates_skipped", 0) + 1
                                continue
                            print("")
                            print(f"🔔 RECOVERY {side} | {market_name(t)} | ${trade_size(t)*trade_price(t):.4f}")
                            copy_started=now()
                            copied=process_trade(state,t,now(),trade_diag.get("source","unknown"))
                            elapsed=(now()-copy_started)*1000
                            state["processing_count"] = state.get("processing_count", 0) + 1
                            state["processing_sum_ms"] = state.get("processing_sum_ms", 0.0) + elapsed
                            state["processing_max_ms"] = max(state.get("processing_max_ms", 0.0), elapsed)
                            if copied:
                                audit_event(t,trade_diag.get("source","unknown"),"COPIED","")
                            else:
                                audit_event(t,trade_diag.get("source","unknown"),"REJECTED",
                                            "SELL_NO_POSITION" if side=="SELL" and state["sell_rejected_no_position"] else
                                            "SELL_NO_BID" if side=="SELL" else "NO_COPY")
                            if copied or side != "SELL":
                                advance_cursor(state, t)
                            retried_sells = retry_pending_sells(state, now())
                            if retried_sells:
                                save(FILES["state"], state)

                    activity=[]; activity_diag={"ok":False,"skipped":True}
                    if now()-last_activity_check>=ACTIVITY_EVERY:
                        activity,activity_diag=fetch_activity_verify(); last_activity_check=now()
                        if activity:
                            an=max((trade_ts(x) for x in activity if trade_ts(x) > 0), default=0)
                            tn=max((trade_ts(x) for x in feed if trade_ts(x) > 0), default=0)
                            if an > 0 and tn > 0 and an > tn:
                                delta = an - tn
                                if delta <= max(RECENT_SECONDS, 3600):
                                    print(f"  ℹ️ ACTIVITY AHEAD OF TRADES by {delta:.1f}s")
                                else:
                                    print("  ℹ️ ACTIVITY/TRADES timestamp mismatch (diagnostic suppressed)")

                    newest=max((trade_ts(x) for x in feed),default=state.get("last_feed_newest",0))
                    age=now()-newest if newest else None
                    status="LIVE" if age is not None and age<=STALE_AFTER else ("STALE" if age is not None and age<=HARD_STALE_AFTER else ("API_CONNECTED_NO_RECENT_EXECUTIONS" if trade_diag.get("ok") else "API_ERROR"))
                    feed_diag={"status":status,"newest_age_seconds":age,"new_this_cycle":len(new_trades),
                               "trades_executions":len(feed),"activity_executions":len(activity),"newest_seen":newest,"trade_diag":trade_diag}
                    state["last_feed_newest"]=newest

                    if RECON_EVERY>0 and now()-num(state.get("last_reconcile"))>=RECON_EVERY:
                        last_position_rows,last_position_diag=fetch_positions()
                        last_recon=reconcile(state,last_position_rows,aggregate_positions(last_position_rows))
                        state["last_reconcile"]=now()

                    if now()-num(state.get("last_resolution_check"))>=RESOLUTION_EVERY:
                        changed = resolve_cycle(state, feed)
                        state["last_resolution_check"] = now()
                        if changed:
                            save(FILES["state"], state)
                            print(f"🏁 RESOLUTION UPDATE | settled {changed} position(s)")

                    if now()-num(state.get("last_closed_check"))>=CLOSED_EVERY:
                        fetch_closed_positions(); state["last_closed_check"]=now()

                    if now()-last_report>=REPORT_EVERY:
                        write_reports(state,feed_diag,last_position_diag,last_recon); last_report=now()

                    print_status(state,feed_diag,last_recon)
            except requests.RequestException as e:
                state["api_errors"]+=1; save(FILES["state"],state); print(f"[{ist()}] ⚠️ API ERROR: {e}")
            except Exception as e:
                state["api_errors"]+=1; save(FILES["state"],state)
                print(f"[{ist()}] ❌ ERROR: {type(e).__name__}: {e}"); traceback.print_exc()
            time.sleep(max(0,POLL_SECONDS-(now()-started)))
    finally:
        stop_priority_worker()
        stop_struct_shadow()
        stop_market_book()
        stop_live_ws()

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt:
        stop_priority_worker(); stop_struct_shadow(); stop_market_book(); stop_live_ws(); print("\nStopped.")
