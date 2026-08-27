from __future__ import annotations
from utils import num

def levels(book,side):
    if not isinstance(book,dict): return []
    raw=book.get(side) or book.get(side+"s") or []
    out=[]
    for row in raw:
        if isinstance(row,dict): p=num(row.get("price")); q=num(row.get("size",row.get("quantity",row.get("shares",0))))
        elif isinstance(row,(list,tuple)) and len(row)>=2: p=num(row[0]);q=num(row[1])
        else: continue
        if p>0 and q>0:out.append((p,q))
    return sorted(out,key=lambda x:x[0],reverse=(side=="bids"))

def buy_vwap(book,notional):
    remaining=notional;shares=spent=0
    for price,available in levels(book,"asks"):
        if remaining<=1e-12:break
        take=min(remaining,price*available); take_shares=take/price
        spent+=take;shares+=take_shares;remaining-=take
    if shares<=0 or remaining>max(.01,notional*.005):return None,0
    return spent/shares,shares

def sell_vwap(book,shares):
    remaining=shares;proceeds=0
    for price,available in levels(book,"bids"):
        if remaining<=1e-12:break
        take=min(remaining,available);proceeds+=take*price;remaining-=take
    if shares<=0 or remaining>max(1e-9,shares*.005):return None,0
    return proceeds/shares,proceeds
