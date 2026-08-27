from __future__ import annotations
import threading

from config import *
from utils import *
from api import *
from trading.pricing import buy_vwap, sell_vwap
from feeds.market_book import fetch_book, status as market_book_status


def new_state():
    return {
        "version":"7.0","wallet":WALLET,"cursor_ts":None,"cursor_id":None,"seen_ids":[],
        "trader_positions":{},"our_positions":{},"fills":[],"closed_trades":[],"reconciliation":[],
        "our_realized_pnl":0.0,"trader_realized_pnl":0.0,"copied_buys":0,"copied_sells":0,
        "duplicates_ignored":0,"duplicate_candidates":0,"skipped_capital":0,"api_errors":0,
        "api_consecutive_failures":0,"api_last_ok":0.0,"api_last_error":"","api_rate_limits":0,
        "api_requests":0,"api_error_details":[],"last_reconcile":0.0,"last_status":0.0,"latency_count":0,"latency_sum_ms":0.0,
        "latency_max_ms":0.0,"latency_min_ms":None,"wide_gap_count":0,"micro_trade_count":0,
        "last_closed_check":0.0,"last_resolution_check":0.0,"settled_positions":0,"settlement_wins":0,
        "settlement_losses":0,"trader_settled_positions":0,"trader_settlement_wins":0,"trader_settlement_losses":0,"exit_events":0,"skipped_liquidity":0,"latency_ms":[],
        "entry_slippage_pct":[],"exit_slippage_pct":[],"polls":0,"last_poll":None,
        "ws_received":0,"ws_reconnects":0,"ws_last_message":0.0,"ws_dropped":0,"audit_events":0,
        "sell_detected":0,"sell_rejected_no_position":0,"sell_rejected_liquidity":0,
        "sell_rejected_duplicate":0,"sell_processed":0,"buy_detected":0,
        "pending_sells":{},
        "trader_ledger_seen":[],"last_sell_recovery_fetch":0.0,
        "priority_events":0,"priority_queue_wait_ms":0.0,"priority_copy_ms":0.0,"priority_copy_max_ms":0.0,"priority_last_copy_ms":0.0,
        "processing_count":0,"processing_sum_ms":0.0,"processing_max_ms":0.0,
        "resolution_wait_log":{},
    }


def load_state():
    resume=os.getenv("RESUME_PAPER_STATE","false").strip().lower() in ("1","true","yes","on")
    if resume:
        state=load(FILES["state"])
        if isinstance(state,dict) and state.get("version")=="7.0" and state.get("wallet")==WALLET:
            defaults=new_state()
            for k,v in defaults.items(): state.setdefault(k,v)
            return state
    return new_state()


def open_capital(state):
    return sum(num(p.get("total_cost")) for p in state["our_positions"].values() if p.get("status")=="OPEN")


def create_position(key,t,owner):
    duration,seconds=market_duration(t)
    return {"position_id":f"{owner}:{key}","owner":owner,"key":key,"asset":trade_asset(t),
            "condition_id":trade_condition(t),"outcome":trade_outcome(t),"market":market_name(t),
            "slug":market_slug(t),"duration":duration,"duration_seconds":seconds,"market_end_ts":market_end(t),
            "shares":0.0,"total_cost":0.0,"average_entry":0.0,"realized_pnl":0.0,
            "first_buy_timestamp":None,"last_activity_timestamp":None,"closed_timestamp":None,
            "buy_count":0,"sell_count":0,"status":"OPEN","exit_reason":None}


def add_buy(p,shares,price,ts):
    if p["first_buy_timestamp"] is None: p["first_buy_timestamp"]=ts
    p["shares"]+=shares; p["total_cost"]+=shares*price
    p["average_entry"]=p["total_cost"]/p["shares"] if p["shares"]>0 else 0
    p["last_activity_timestamp"]=ts; p["buy_count"]+=1; p["status"]="OPEN"


def sell_position(p,shares,price,ts,reason):
    shares=min(max(0,shares),num(p.get("shares")))
    if shares<=0:return 0
    cost=shares*num(p.get("average_entry")); proceeds=shares*price; pnl=proceeds-cost
    p["shares"]-=shares; p["total_cost"]=max(0,num(p.get("total_cost"))-cost); p["realized_pnl"]=num(p.get("realized_pnl"))+pnl
    p["last_activity_timestamp"]=ts; p["sell_count"]=int(p.get("sell_count",0))+1
    if p["shares"]<=1e-9:
        p["shares"]=0;p["total_cost"]=0;p["average_entry"]=0;p["status"]="CLOSED";p["closed_timestamp"]=ts;p["exit_reason"]=reason
    else:p["average_entry"]=p["total_cost"]/p["shares"]
    return pnl


def _position_matches(pos,t):
    if not isinstance(pos,dict) or pos.get("status")!="OPEN" or num(pos.get("shares"))<=1e-9:
        return False
    asset=str(trade_asset(t) or "")
    cond=str(trade_condition(t) or "")
    outcome=str(trade_outcome(t) or "")
    if asset and str(pos.get("asset", ""))!=asset: return False
    if cond and str(pos.get("condition_id", ""))!=cond: return False
    # Outcome is checked only when both sides expose it. Asset+condition remains authoritative.
    po=str(pos.get("outcome", "") or "")
    if outcome and po and outcome!=po and str(outcome).lower()!=str(po).lower():
        return False
    return bool(asset or cond)



# One global claim lock is shared by FAST WebSocket and REST recovery paths.
# Their worker locks are intentionally different, so deduplication must happen
# here at the single ledger boundary.
_COPY_CLAIM_LOCK = threading.RLock()
_COPY_CLAIM_TTL_SECONDS = 3600.0
_COPY_CLAIM_MAX = 50000


def _trade_claim_keys(t):
    """Return stable identity aliases without merging distinct fills.

    Prefer transaction/execution IDs. Only use the economic fingerprint when
    the feed provides no usable ID at all; otherwise two legitimate fills with
    identical economics could be incorrectly collapsed.
    """
    ids = []
    for k in ("transactionHash", "transaction_hash", "txHash", "hash",
              "tradeId", "trade_id", "id"):
        v = t.get(k) if isinstance(t, dict) else None
        if v not in (None, "", "0"):
            ids.append("ID|" + str(v).lower())
    if ids:
        return list(dict.fromkeys(ids))
    try:
        return ["ECON|" + economic_trade_fingerprint(t)]
    except Exception:
        return []


def claim_copy_once(state, t):
    """Atomically claim a trade so FAST/RECOVERY can never both copy it."""
    keys = _trade_claim_keys(t)
    if not keys:
        return True
    now_ts = now()
    with _COPY_CLAIM_LOCK:
        claims = state.setdefault("copy_claims", {})
        if not isinstance(claims, dict):
            claims = {}
            state["copy_claims"] = claims
        cutoff = now_ts - _COPY_CLAIM_TTL_SECONDS
        for k, ts in list(claims.items()):
            if num(ts) < cutoff:
                claims.pop(k, None)
        if any(k in claims for k in keys):
            return False
        for k in keys:
            claims[k] = now_ts
        if len(claims) > _COPY_CLAIM_MAX:
            oldest = sorted(claims.items(), key=lambda x: num(x[1]))[:len(claims)-_COPY_CLAIM_MAX]
            for k, _ in oldest:
                claims.pop(k, None)
        return True


def _find_position(state,t,owner):
    positions=state.get(owner,{})
    key=trade_key(t)
    exact=positions.get(key)
    if _position_matches(exact,t): return key,exact
    candidates=[]
    for k,pos in positions.items():
        if _position_matches(pos,t):
            score=0
            if str(pos.get("asset",""))==str(trade_asset(t)): score+=5
            if str(pos.get("condition_id",""))==str(trade_condition(t)): score+=4
            if str(pos.get("outcome",""))==str(trade_outcome(t)): score+=1
            candidates.append((score,num(pos.get("last_activity_timestamp")),k,pos))
    if not candidates:return key,None
    candidates.sort(key=lambda x:(x[0],x[1]),reverse=True)
    return candidates[0][2],candidates[0][3]


def update_trader_ledger(state,t):
    key=trade_key(t); p=state["trader_positions"].get(key)
    if trade_side(t)=="BUY":
        if not p: p=create_position(key,t,"TRADER"); state["trader_positions"][key]=p
        add_buy(p,trade_size(t),trade_price(t),trade_ts(t))
    elif trade_side(t)=="SELL":
        _,p=_find_position(state,t,"trader_positions")
        if p:
            pnl=sell_position(p,trade_size(t),trade_price(t),trade_ts(t),"TRADER_SELL")
            state["trader_realized_pnl"]+=pnl


def copy_buy(state,t,observed,source="unknown"):
    trader_notional = trade_size(t) * trade_price(t)
    notional = trader_notional * COPY_NOTIONAL_FRACTION
    available=MAX_OPEN_CAPITAL-open_capital(state)
    if notional<=0:return False
    if notional>available+1e-9:
        state["skipped_capital"]+=1; print(f"  ⚠️ SKIP BUY | ${notional:.2f} exceeds available ${max(0,available):.2f}"); return False
    book=fetch_book(trade_asset(t)); our_price,our_shares=buy_vwap(book,notional)
    if our_price is None:
        state["skipped_liquidity"]+=1; print(f"  ⚠️ SKIP BUY | no sufficient ask liquidity for ${notional:.2f}"); return False
    key=trade_key(t); p=state["our_positions"].get(key)
    if not p:p=create_position(key,t,"OUR");state["our_positions"][key]=p
    add_buy(p,our_shares,our_price,trade_ts(t))
    latency=max(0,(observed-trade_ts(t))*1000); gap=((our_price-trade_price(t))/trade_price(t)*100) if trade_price(t) else 0
    state["copied_buys"]+=1;state["latency_ms"].append(latency);state["entry_slippage_pct"].append(gap)
    state["fills"].append({"time_ist":ist(observed),"type":"OUR_BUY","trade_id":trade_id(t),"market":market_name(t),"asset":trade_asset(t),"outcome":trade_outcome(t),"trader_price":money(trade_price(t)),"trader_shares":money(trade_size(t)),"trader_notional":money(trader_notional),"copy_notional":money(notional),"our_price":money(our_price),"our_shares":money(our_shares),"our_notional":money(our_shares*our_price),"price_gap_pct":money(gap),"latency_ms":money(latency)})
    bstat = market_book_status()
    print("  ✅ COPIED BUY")
    print(f"     Trader: ${trader_notional:.4f} @ ${trade_price(t):.4f} | Copy: ${notional:.4f} ({COPY_NOTIONAL_FRACTION:.0%})")
    print(f"     Us:     ${our_shares*our_price:.4f} @ ${our_price:.4f}")
    print(f"     Gap:    {gap:+.2f}% | Latency: {latency:.0f} ms | Book: {bstat.get('last_lookup_source','?')}")
    return True


def copy_sell(state,t,observed,source="unknown"):
    state["sell_detected"]+=1
    key,p=_find_position(state,t,"our_positions")
    if not p:
        state["sell_rejected_no_position"]+=1
        print("  ❌ SELL REJECTED | local position not found")
        print(f"     Asset: {trade_asset(t)} | Condition: {trade_condition(t)} | Outcome: {trade_outcome(t)}")
        return False

    # A pending SELL may be retried after the trader ledger has already been
    # updated. Preserve the original pre-SELL mirror fraction so retries do
    # not accidentally oversell our position.
    stored_fraction = num(t.get("_mirror_fraction"), -1.0)
    if 0.0 <= stored_fraction <= 1.0:
        fraction = stored_fraction
    else:
        _,trader_p=_find_position(state,t,"trader_positions")
        trader_before=num(trader_p.get("shares")) if trader_p else 0.0
        sell_size=trade_size(t)
        if sell_size<=0:return False
        if trader_before>1e-9: fraction=min(1.0,sell_size/trader_before)
        else: fraction=min(1.0,sell_size/max(num(p.get("shares")),1e-12))
    shares=min(num(p.get("shares")),num(p.get("shares"))*fraction)
    if shares<=1e-9:
        state["sell_rejected_no_position"]+=1; print("  ❌ SELL REJECTED | zero local shares"); return False

    book=fetch_book(trade_asset(t)); our_price,proceeds=sell_vwap(book,shares)
    if our_price is None:
        state["sell_rejected_liquidity"]+=1;state["skipped_liquidity"]+=1
        print(f"  ⚠️ SKIP SELL | insufficient bid liquidity for {shares:.6f} shares")
        return False

    pnl=sell_position(p,shares,our_price,trade_ts(t),"TRADER_SELL")
    state["our_realized_pnl"]+=pnl;state["copied_sells"]+=1;state["sell_processed"]+=1
    latency=max(0,(observed-trade_ts(t))*1000);gap=((trade_price(t)-our_price)/trade_price(t)*100) if trade_price(t) else 0
    state["latency_ms"].append(latency);state["exit_slippage_pct"].append(gap)
    state["fills"].append({"time_ist":ist(observed),"type":"OUR_SELL","trade_id":trade_id(t),"market":market_name(t),"asset":trade_asset(t),"outcome":trade_outcome(t),"trader_price":money(trade_price(t)),"trader_shares":money(trade_size(t)),"our_price":money(our_price),"our_shares":money(shares),"our_proceeds":money(proceeds),"our_pnl":money(pnl),"price_gap_pct":money(gap),"latency_ms":money(latency)})
    bstat = market_book_status()
    print("  ↘️ COPIED SELL")
    print(f"     Trader: ${trade_price(t):.4f} | Us: ${our_price:.4f}")
    print(f"     Shares: {shares:.6f} | P&L: ${pnl:+.4f} | Book: {bstat.get('last_lookup_source','?')}")
    return True


def _prepare_sell_mirror(state, t):
    """Capture the trader pre-SELL state before mutating the trader ledger."""
    _, trader_p = _find_position(state, t, "trader_positions")
    trader_before = num(trader_p.get("shares")) if trader_p else 0.0
    sell_size = trade_size(t)
    if sell_size <= 0:
        return None
    if trader_before > 1e-9:
        return min(1.0, sell_size / trader_before)
    _, our_p = _find_position(state, t, "our_positions")
    our_shares = num(our_p.get("shares")) if our_p else 0.0
    return min(1.0, sell_size / max(our_shares, 1e-12))


def retry_pending_sells(state, observed=None, max_age_seconds=900):
    """Retry failed paper SELLs without re-mutating the trader ledger."""
    observed = now() if observed is None else observed
    pending = state.setdefault("pending_sells", {})
    if not isinstance(pending, dict):
        pending = {}
        state["pending_sells"] = pending

    copied_count = 0
    for key, raw in list(pending.items()):
        if not isinstance(raw, dict):
            pending.pop(key, None)
            continue
        ts = trade_ts(raw)
        if ts and observed - ts > max_age_seconds:
            # The trader ledger was updated when this SELL was first detected.
            # Evicting the retry must never update it a second time.
            state["sell_rejected_duplicate"] = int(state.get("sell_rejected_duplicate", 0)) + 1
            pending.pop(key, None)
            continue
        if copy_sell(state, raw, observed, raw.get("_feed_source", "pending")):
            pending.pop(key, None)
            copied_count += 1
    return copied_count


def process_trade(state,t,observed,source="unknown"):
    # Final atomic dedup boundary shared by FAST WS and REST recovery.
    if not claim_copy_once(state, t):
        state["copy_duplicates_skipped"] = state.get("copy_duplicates_skipped", 0) + 1
        return False
    side=trade_side(t)
    if side=="BUY":
        state["buy_detected"]+=1
        copied=copy_buy(state,t,observed,source)
        ledger_key = trade_id(t)
        seen_ledger = state.setdefault("trader_ledger_seen", [])
        if ledger_key not in seen_ledger:
            update_trader_ledger(state,t)
            seen_ledger.append(ledger_key)
            state["trader_ledger_seen"] = seen_ledger[-20000:]
        return copied
    if side=="SELL":
        # Record the original mirror ratio before the trader ledger is reduced.
        prepared = _prepare_sell_mirror(state, t)
        if prepared is not None:
            t = dict(t)
            t["_mirror_fraction"] = prepared
        # The trader really did SELL even if our paper execution temporarily
        # fails. The trader ledger must therefore advance immediately, exactly
        # once. Recovery feeds may surface the same SELL again while it is
        # pending, so guard the ledger mutation with a persistent execution ID.
        ledger_key = trade_id(t)
        seen_ledger = state.setdefault("trader_ledger_seen", [])
        if ledger_key not in seen_ledger:
            update_trader_ledger(state,t)
            seen_ledger.append(ledger_key)
            state["trader_ledger_seen"] = seen_ledger[-20000:]
        copied=copy_sell(state,t,observed,source)
        if copied:
            state.get("pending_sells",{}).pop(trade_id(t),None)
        else:
            state.setdefault("pending_sells",{})[trade_id(t)]=t
        return copied
    return False
