from __future__ import annotations
import json
from config import *
from utils import get_json, first, num, money, trade_asset, trade_condition, trade_outcome

def fetch_positions():
    rows=[]; seen=set()
    for page in range(POSITION_PAGES):
        try:
            data=get_json(POSITIONS_URL, {"user":WALLET,"limit":POSITION_PAGE_SIZE,"offset":page*POSITION_PAGE_SIZE})
        except Exception as e:
            return rows, {"raw_rows":len(rows),"error":f"{type(e).__name__}: {e}","pages":page}
        if not isinstance(data,list) or not data: break
        for p in data:
            sig=json.dumps(p,sort_keys=True,default=str)
            if sig not in seen:
                seen.add(sig); rows.append(p)
        if len(data)<POSITION_PAGE_SIZE: break
    return rows, {"raw_rows":len(rows),"error":None,"pages":min(POSITION_PAGES,max(1,(len(rows)//POSITION_PAGE_SIZE)+1))}

def position_key(p):
    return "|".join([str(first(p,"asset","assetId","tokenId","token_id",default="")),
                     str(first(p,"conditionId","condition_id",default="")),
                     str(first(p,"outcomeIndex","outcome_index","outcome",default=""))])

def position_size(p): return num(first(p,"size","shares","quantity","balance",default=0))
def position_avg_price(p): return num(first(p,"avgPrice","averagePrice","avg_price","price",default=0))

def aggregate_positions(rows):
    agg={}
    for p in rows:
        size=position_size(p); key=position_key(p)
        if size<=1e-9 or not key: continue
        a=agg.setdefault(key,{"key":key,"shares":0.0,"weighted_cost":0.0,
                              "market":first(p,"title","market","slug","eventSlug"),"raw_positive_rows":0})
        a["shares"] += size; a["weighted_cost"] += size*position_avg_price(p); a["raw_positive_rows"] += 1
    for p in agg.values():
        p["average_price"]=money(p["weighted_cost"]/p["shares"] if p["shares"] else 0)
        p["shares"]=money(p["shares"]); del p["weighted_cost"]
    return agg

def fetch_book(asset):
    if not asset: return None
    try: return get_json(CLOB_BOOK_URL, {"token_id":asset})
    except Exception:
        try: return get_json(CLOB_BOOK_URL, {"asset_id":asset})
        except Exception: return None

def fetch_closed_positions():
    try:
        data=get_json(CLOSED_POSITIONS_URL,{"user":WALLET,"limit":50,"sortBy":"TIMESTAMP","sortDirection":"DESC"},timeout=NORMAL_TIMEOUT)
        return data if isinstance(data,list) else []
    except Exception: return []

def fetch_redeemable_positions():
    try:
        data=get_json(POSITIONS_URL,{"user":WALLET,"redeemable":"true","sizeThreshold":0,"limit":500},timeout=NORMAL_TIMEOUT)
        return data if isinstance(data,list) else []
    except Exception: return []
