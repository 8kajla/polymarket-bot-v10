from __future__ import annotations

"""Shadow feed race metrics.

No trading decisions are made here.  It records when RTDS and Struct observe
what appears to be the same execution so feed selection can be evidence-based.
"""

import threading
import time
from collections import OrderedDict

from utils import trade_ts, trade_side, trade_asset, trade_condition, trade_outcome, trade_size, trade_price

LOCK = threading.RLock()
MAX_PENDING = 10000
MATCH_WINDOW = 5.0

PENDING: OrderedDict[str, dict] = OrderedDict()
STATS = {
    "rtds_seen": 0,
    "struct_seen": 0,
    "matched": 0,
    "struct_first": 0,
    "rtds_first": 0,
    "same_time": 0,
    "struct_only": 0,
    "rtds_only": 0,
    "last_struct_ms": None,
    "last_rtds_ms": None,
    "median_advantage_ms": None,
}


def _key(row):
    for k in ("transactionHash", "transaction_hash", "txHash", "hash", "tradeId", "trade_id", "id"):
        value = row.get(k) if isinstance(row, dict) else None
        if value not in (None, "", "0"):
            return "ID|" + str(value).lower()
    return "ECON|" + "|".join([
        trade_asset(row), trade_condition(row), str(trade_outcome(row)),
        trade_side(row), f"{trade_size(row):.8f}", f"{trade_price(row):.8f}",
        str(int(trade_ts(row) * 2)),
    ])


def record(source, row, received_at=None):
    source = str(source).lower()
    if source not in ("rtds", "struct") or not isinstance(row, dict):
        return
    received_at = time.time() if received_at is None else float(received_at)
    key = _key(row)
    with LOCK:
        STATS[f"{source}_seen"] += 1
        STATS[f"last_{source}_ms"] = round(received_at * 1000, 3)
        entry = PENDING.get(key)
        if entry is None:
            PENDING[key] = {source: received_at}
        elif source not in entry:
            entry[source] = received_at
            if "rtds" in entry and "struct" in entry:
                STATS["matched"] += 1
                delta_ms = (entry["rtds"] - entry["struct"]) * 1000
                if abs(delta_ms) < 1:
                    STATS["same_time"] += 1
                elif delta_ms > 0:
                    STATS["struct_first"] += 1
                else:
                    STATS["rtds_first"] += 1
                _prune_locked()
                return
        _prune_locked()


def _prune_locked():
    now = time.time()
    for key, value in list(PENDING.items()):
        times = [v for v in value.values() if isinstance(v, (int, float))]
        if times and now - max(times) > MATCH_WINDOW:
            if "rtds" in value and "struct" not in value:
                STATS["rtds_only"] += 1
            elif "struct" in value and "rtds" not in value:
                STATS["struct_only"] += 1
            PENDING.pop(key, None)
    while len(PENDING) > MAX_PENDING:
        PENDING.popitem(last=False)


def snapshot():
    with LOCK:
        data = dict(STATS)
        if STATS["matched"]:
            total = STATS["matched"]
            data["struct_first_pct"] = round(STATS["struct_first"] / total * 100, 2)
            data["rtds_first_pct"] = round(STATS["rtds_first"] / total * 100, 2)
        else:
            data["struct_first_pct"] = 0.0
            data["rtds_first_pct"] = 0.0
        return data
