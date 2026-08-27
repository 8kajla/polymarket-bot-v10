from __future__ import annotations
from config import *
from utils import *
from feeds.core import ws_status_text, LIVE_WS_STATUS
from trading.ledger import open_capital

def stats(values):
    values=[num(x) for x in values if x is not None]
    if not values:return {"count":0}
    return {"count":len(values),"average":money(sum(values)/len(values)),"minimum":money(min(values)),"maximum":money(max(values))}


def _analytics_snapshot(state):
    rows=[x for x in state.get("trade_observations",[]) if x.get("side")=="BUY"]
    lat=[num(x.get("latency_ms")) for x in rows if x.get("latency_ms") is not None]
    gaps=[num(x.get("gap_pct")) for x in rows if x.get("gap_pct") is not None]
    def bucket(v, edges):
        for i,e in enumerate(edges):
            if v < e:return i
        return len(edges)
    lat_counts=[0]*5
    for v in lat:
        lat_counts[bucket(v,[250,500,1000,2000])]+=1
    gap_counts=[0]*6
    for v in gaps:
        gap_counts[bucket(v,[-10,0,5,10,25])]+=1
    sources={}; books={}; markets={}
    for x in rows:
        sources[x.get("source","unknown")]=sources.get(x.get("source","unknown"),0)+1
        books[x.get("book","unknown")]=books.get(x.get("book","unknown"),0)+1
        m=x.get("market","unknown"); markets[m]=markets.get(m,0)+1
    return {"copies":len(rows),"latency_buckets":{"<250ms":lat_counts[0],"250-500ms":lat_counts[1],"500-1000ms":lat_counts[2],"1-2s":lat_counts[3],">=2s":lat_counts[4]},"gap_buckets":{"<-10%":gap_counts[0],"-10_to_0":gap_counts[1],"0_to_5%":gap_counts[2],"5_to_10%":gap_counts[3],"10_to_25%":gap_counts[4],">=25%":gap_counts[5]},"sources":sources,"books":books,"avg_latency_ms":(sum(lat)/len(lat) if lat else 0),"avg_gap_pct":(sum(gaps)/len(gaps) if gaps else 0)}

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
      "latency":{"count":state["latency_count"],"avg_ms":state["latency_sum_ms"]/max(1,state["latency_count"]),
        "min_ms":state["latency_min_ms"],"max_ms":state["latency_max_ms"]},
      "reconciliation":recon,
      "execution":{"latency_ms":stats(state["latency_ms"]),"entry_slippage_pct":stats(state["entry_slippage_pct"]),"exit_slippage_pct":stats(state["exit_slippage_pct"]),"analytics":_analytics_snapshot(state)}}
    save(FILES["summary"],summary); save(FILES["trader_positions"],list(state["trader_positions"].values()))
    save(FILES["our_positions"],list(state["our_positions"].values())); save(FILES["closed_trades"],state["closed_trades"])
    save(FILES["fills"],state["fills"]); save(FILES["reconciliation"],state["reconciliation"]); save(FILES["state"],state)

def print_status(state,feed_diag,recon,force=False):
    if not force and now()-num(state.get("last_status"))<STATUS_EVERY:
        return
    state["last_status"]=now()
    open_cap=open_capital(state)
    available=max(0,MAX_OPEN_CAPITAL-open_cap)
    age=feed_diag.get("newest_age_seconds")
    feed_text=feed_diag.get("status","UNKNOWN") if age is None else f"{feed_diag.get('status','UNKNOWN')} {age:.1f}s"
    n=state["latency_count"]
    avg=state["latency_sum_ms"]/n if n else 0

    try:
        from trading.priority import stats as priority_stats
        ps=priority_stats()
    except Exception:
        ps={"queue_depth":0,"processed":0,"errors":0,"last_copy_ms":0,"last_total_ms":0,"copy_avg_ms":0,"copy_p95_ms":0}
    try:
        from feeds.market_book import status as book_status
        bs=book_status()
    except Exception:
        bs={"connected":False,"subscribed":0,"snapshots":0,"price_changes":0,"fallbacks":0,"reconnects":0,"stale_reconnects":0}

    trader_w=state.get("trader_settlement_wins",0)
    trader_l=state.get("trader_settlement_losses",0)
    trader_total=trader_w+trader_l
    trader_wr=(trader_w/trader_total*100) if trader_total else 0

    print("")
    print(f"[{ist()}] STATUS | TRADER WS {ws_status_text()} | CLOB WS {'LIVE' if bs.get('connected') else 'OFF'}")
    print(f"COPY  {state['copied_buys']}B/{state['copied_sells']}S | {COPY_NOTIONAL_FRACTION*100:.0f}% size | queue {ps.get('queue_depth',0)}")
    print(f"BOOK  {bs.get('subscribed',0)} assets | snapshots {bs.get('snapshots',0)} | changes {bs.get('price_changes',0)} | REST fallbacks {bs.get('fallbacks',0)}")
    print(f"LAT   copy avg {ps.get('copy_avg_ms',0):.0f}ms | p95 {ps.get('copy_p95_ms',0):.0f}ms | last {ps.get('last_total_ms',0):.0f}ms | queue {ps.get('last_queue_ms',0):.0f}ms")
    a=_analytics_snapshot(state)
    print(f"ANALYTICS copies {a['copies']} | lat <250/{a['latency_buckets']['<250ms']} 250-500/{a['latency_buckets']['250-500ms']} 500-1s/{a['latency_buckets']['500-1000ms']} 1-2s/{a['latency_buckets']['1-2s']} >2s/{a['latency_buckets']['>=2s']}")
    print(f"ANALYTICS gap <-10/{a['gap_buckets']['<-10%']} -10..0/{a['gap_buckets']['-10_to_0']} 0..5/{a['gap_buckets']['0_to_5%']} 5..10/{a['gap_buckets']['5_to_10%']} 10..25/{a['gap_buckets']['10_to_25%']} >=25/{a['gap_buckets']['>=25%']} | avg gap {a['avg_gap_pct']:+.2f}%")
    print(f"ANALYTICS source {a['sources']} | book {a['books']}")
    print(f"CAP   open ${open_cap:.2f} | free ${available:.2f}/${MAX_OPEN_CAPITAL:.2f}")
    print(f"P&L   ours ${state['our_realized_pnl']:+.2f} | trader ${state['trader_realized_pnl']:+.2f} | trader W/L {trader_w}/{trader_l} ({trader_wr:.0f}%)")
    print(f"EXIT  settled {state['settled_positions']} | W/L {state['settlement_wins']}/{state['settlement_losses']} | open {sum(1 for p in state['our_positions'].values() if p.get('status')=='OPEN' and num(p.get('shares'))>1e-9)}")
    print(f"API   {state['api_requests']} req | {state['api_errors']} err | reconcile {recon.get('matches',0)} match/{recon.get('share_mismatches',0)} diff")
    if ps.get('errors') or state.get('api_errors') or bs.get('stale_reconnects'):
        print(f"WARN  copy {ps.get('errors',0)} errors | API {state.get('api_errors',0)} | CLOB stale reconnects {bs.get('stale_reconnects',0)}")
    print("-"*72)
