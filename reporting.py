from __future__ import annotations
from config import *
from utils import *
from feeds.core import ws_status_text, LIVE_WS_STATUS
from feeds.market_book import status as market_book_status
from feeds.struct_shadow import status as struct_status
from trading.priority import status as priority_status
from trading.ledger import open_capital

def stats(values):
    values=[num(x) for x in values if x is not None]
    if not values:return {"count":0}
    return {"count":len(values),"average":money(sum(values)/len(values)),"minimum":money(min(values)),"maximum":money(max(values))}

def write_reports(state,feed_diag,position_diag,recon):
    open_cap=open_capital(state); closed=state["closed_trades"]
    wins=sum(1 for x in closed if num(x.get("pnl"))>0); losses=sum(1 for x in closed if num(x.get("pnl"))<0)
    summary={"version":"7.0","updated_ist":ist(),"wallet":WALLET,
      "simulator":{"max_capital":MAX_OPEN_CAPITAL,"open_capital":money(open_cap),"available_capital":money(max(0,MAX_OPEN_CAPITAL-open_cap)),
        "realized_pnl":money(state["our_realized_pnl"]),"copy_notional_fraction":COPY_NOTIONAL_FRACTION,"copied_buys":state["copied_buys"],"copied_sells":state["copied_sells"],
        "duplicates_ignored":state["duplicates_ignored"],"closed_trades":len(closed),"wins":wins,"losses":losses,
        "win_rate_pct":money(wins/len(closed)*100) if closed else 0,"skipped_capital":state["skipped_capital"],
        "skipped_liquidity":state["skipped_liquidity"],"api_errors":state["api_errors"]},
      "trader":{"realized_pnl":money(state["trader_realized_pnl"]),
        "open_positions":sum(1 for p in state["trader_positions"].values() if p["status"]=="OPEN" and p["shares"]>1e-9),
        "settled_positions":state.get("trader_settled_positions",0),
        "wins":state.get("trader_settlement_wins",0),
        "losses":state.get("trader_settlement_losses",0),
        "win_rate_pct":money(state.get("trader_settlement_wins",0)/max(1,state.get("trader_settlement_wins",0)+state.get("trader_settlement_losses",0))*100)},
      "live_feed":feed_diag,
      "sell_diagnostics":{"detected":state["sell_detected"],"processed":state["sell_processed"],
        "no_position":state["sell_rejected_no_position"],"liquidity":state["sell_rejected_liquidity"],"pending":len(state.get("pending_sells",{})),"resolution_due":state.get("resolution_due_positions",0),"resolution_redeemable_checked":state.get("resolution_redeemable_checked",0)},
      "latency":{"count":len(state.get("latency_ms",[])),"avg_ms":stats(state.get("latency_ms",[])).get("average",0),
        "min_ms":stats(state.get("latency_ms",[])).get("minimum"),"max_ms":stats(state.get("latency_ms",[])).get("maximum"),
        "processing_avg_ms":state.get("processing_sum_ms",0.0)/max(1,state.get("processing_count",0)),"processing_max_ms":state.get("processing_max_ms",0.0)},
      "reconciliation":recon,
      "execution":{"latency_ms":stats(state["latency_ms"]),"entry_slippage_pct":stats(state["entry_slippage_pct"]),"exit_slippage_pct":stats(state["exit_slippage_pct"])}}
    save(FILES["summary"],summary); save(FILES["trader_positions"],list(state["trader_positions"].values()))
    save(FILES["our_positions"],list(state["our_positions"].values())); save(FILES["closed_trades"],state["closed_trades"])
    save(FILES["fills"],state["fills"]); save(FILES["reconciliation"],state["reconciliation"]); save(FILES["state"],state)

def print_status(state,feed_diag,recon,force=False):
    if not force and now()-num(state.get("last_status"))<STATUS_EVERY:return
    state["last_status"]=now(); open_cap=open_capital(state); available=max(0,MAX_OPEN_CAPITAL-open_cap); age=feed_diag.get("newest_age_seconds")
    feed_text=feed_diag.get("status","UNKNOWN") if age is None else f"{feed_diag.get('status','UNKNOWN')} ({age:.1f}s)"
    lat_values=[num(x) for x in state.get("latency_ms",[]) if x is not None]
    n=len(lat_values); avg=sum(lat_values)/n if n else 0
    print("");print("="*68);print(f"[{ist()}] 60-SECOND STATUS");print("="*68)
    print(f"Feed: {feed_text} | New this cycle: {feed_diag.get('new_this_cycle',0)}")
    print(f"Live WS: {ws_status_text()}")
    print(f"WS wallet matches: {LIVE_WS_STATUS['wallet_matches']} | BUY candidates: {LIVE_WS_STATUS['buy_candidates']} | SELL candidates: {LIVE_WS_STATUS['sell_candidates']}")
    print(f"Copied: BUY {state['copied_buys']} | SELL {state['copied_sells']} | Size {COPY_NOTIONAL_FRACTION*100:.0f}% of trader")
    print(f"Capital: OPEN ${open_cap:.2f} | FREE ${available:.2f} / ${MAX_OPEN_CAPITAL:.2f}")
    print(f"P&L: OUR ${state['our_realized_pnl']:+.2f} | TRADER ${state['trader_realized_pnl']:+.2f} | Trader W/L {state.get('trader_settlement_wins',0)}/{state.get('trader_settlement_losses',0)}")
    if n: print(f"Copy latency: avg {avg:.0f}ms | min {min(lat_values):.0f}ms | max {max(lat_values):.0f}ms")
    else: print("Copy latency: no copied executions yet")
    ps=priority_status(); bs=market_book_status(); ss=struct_status()
    print(f"Priority: queue {ps.get('queue_wait_avg_ms',0):.0f}ms avg | copied {ps.get('copied',0)} | rejected {ps.get('rejected',0)}")
    print(f"Book: last {bs.get('last_lookup_source','NONE')} | fallbacks {bs.get('fallbacks',0)} | WS assets {bs.get('subscribed_assets',0)}")
    if ss.get('enabled'):
        print(f"Struct shadow: {'UP' if ss.get('connected') else 'DOWN'} | confirmed {ss.get('confirmed',0)} | reconnects {ss.get('reconnects',0)}")
    print(f"Feed rows: trades {feed_diag.get('trades_executions',0)} | activity {feed_diag.get('activity_executions',0)}")
    print(f"SELL diagnostics: detected {state['sell_detected']} | processed {state['sell_processed']} | no-position {state['sell_rejected_no_position']} | no-bid {state['sell_rejected_liquidity']} | pending {len(state.get('pending_sells',{}))}")
    print(f"Quality: wide-gap {state['wide_gap_count']} | micro-trades {state['micro_trade_count']} | API errors {state['api_errors']}")
    print(f"Exits: SELL {state['copied_sells']} | Settled {state['settled_positions']} | Wins {state['settlement_wins']} | Losses {state['settlement_losses']}")
    print(f"Resolution: due {state.get('resolution_due_positions',0)} | unresolved {state.get('resolution_unresolved_positions',0)} | markets checked {state.get('resolution_markets_checked',0)}")
    api_age=now()-state["api_last_ok"] if state["api_last_ok"] else None
    api_err = state.get("api_last_error", "")
    api_err_text = f" | Last error: {api_err[:100]}" if api_err else ""
    print(f"API: {'OK '+format(api_age,'.0f')+'s ago' if api_age is not None else 'NO SUCCESS'} | Requests {state['api_requests']} | Errors {state['api_errors']} | Rate limits {state['api_rate_limits']}{api_err_text}")
    print(f"Reconcile: match {recon.get('matches',0)} | share diff {recon.get('share_mismatches',0)} | missing local {recon.get('missing_local',0)} | missing API {recon.get('missing_api',0)}")
    print("="*68)
