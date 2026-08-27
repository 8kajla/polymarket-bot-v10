from __future__ import annotations
from config import *
from utils import *
from api import position_size

def reconcile(state, api_rows, api_agg):
    local = {
        k: p for k, p in state["trader_positions"].items()
        if p["status"] == "OPEN" and p["shares"] > 1e-9
    }

    matches = 0
    share_mismatch = 0
    missing_local = 0
    missing_api = 0

    for key in set(local) | set(api_agg):
        ls = num(local.get(key, {}).get("shares"))
        ap = num(api_agg.get(key, {}).get("shares"))

        if key in local and key in api_agg:
            if abs(ls - ap) <= 1e-6:
                matches += 1
            else:
                share_mismatch += 1
        elif key in api_agg:
            missing_local += 1
        else:
            missing_api += 1

    report = {
        "time_ist": ist(),
        "api_raw_rows": len(api_rows),
        "api_positive_rows": sum(
            1 for p in api_rows if position_size(p) > 1e-9
        ),
        "api_unique_current_positions": len(api_agg),
        "local_open_positions": len(local),

        "matches": matches,
        "share_mismatches": share_mismatch,
        "missing_local": missing_local,
        "missing_api": missing_api,
        "total_mismatches": (
            share_mismatch + missing_local + missing_api
        ),
    }

    state["reconciliation"].append(report)
    state["reconciliation"] = state["reconciliation"][-500:]

    return report


# ============================================================
# SETTLEMENT
# ============================================================

