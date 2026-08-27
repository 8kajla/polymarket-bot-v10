from __future__ import annotations

import json
from datetime import datetime

from config import *
from utils import *
from api import fetch_redeemable_positions, fetch_closed_positions

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_MARKET_BY_SLUG_URL = "https://gamma-api.polymarket.com/markets/slug/{slug}"
CLOB_MARKET_URL = "https://clob.polymarket.com/markets/{condition_id}"


def _json_list(value):
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, tuple):
        return list(value)
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        # Some API versions have returned Python-looking arrays.
        text = str(value).strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                import ast
                parsed = ast.literal_eval(text)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                pass
    return []


def position_asset(pos):
    return str(first(pos, "asset", "assetId", "asset_id", "tokenId", "token_id", default=""))


def position_condition(pos):
    return str(first(pos, "conditionId", "condition_id", "condition", default=""))


def position_outcome(pos):
    return str(first(pos, "outcome", "outcomeName", "outcomeIndex", "outcome_index", default="")).strip().lower()


def position_shares(pos):
    return num(first(pos, "shares", "size", "quantity", "balance", default=0))


def position_entry_cost(pos):
    value = first(pos, "total_cost", "cost", "cost_basis", "notional", "spent", "entry_cost", "our_cost", default=None)
    if value is not None:
        return num(value)
    avg = first(pos, "average_entry", "entry_price", "buy_price", "avgPrice", "averagePrice", "price", default=0)
    return position_shares(pos) * num(avg)


def same_market_position(a, b):
    aa, ab = position_asset(a), position_asset(b)
    ca, cb = position_condition(a), position_condition(b)
    if aa and ab and aa != ab:
        return False
    if ca and cb and ca.lower() != cb.lower():
        return False
    if not aa and not ca:
        return False
    oa, ob = position_outcome(a), position_outcome(b)
    if oa and ob and oa != ob:
        # Numeric outcome index vs label is common. Asset/condition remain
        # authoritative, so don't reject when either side is numeric.
        if not (oa.isdigit() or ob.isdigit()):
            return False
    return True


def _resolve_value_from_row(row):
    if not isinstance(row, dict):
        return None
    for k in ("winningOutcome", "winning_outcome", "result", "resolution"):
        x = row.get(k)
        if isinstance(x, (int, float)) and x in (0, 1):
            return float(x)
        if isinstance(x, str):
            y = x.strip().lower()
            if y in ("win", "won", "yes", "up", "true", "1"):
                return 1.0
            if y in ("loss", "lost", "no", "down", "false", "0"):
                return 0.0
    return None


def resolve_position_object(state, pos, win, reason):
    if not isinstance(pos, dict) or pos.get("status") in {"CLOSED", "SETTLED"}:
        return False
    shares = position_shares(pos)
    if shares <= 1e-9:
        return False

    cost = position_entry_cost(pos)
    settlement_price = 1.0 if win else 0.0
    proceeds = shares * settlement_price
    pnl = proceeds - cost
    closed_at = now()

    pos.update({
        "status": "SETTLED",
        "closed_at": closed_at,
        "closed_at_ist": ist(closed_at),
        "settlement_price": settlement_price,
        "settlement_proceeds": proceeds,
        "settlement_pnl": pnl,
        "settlement_result": "WIN" if win else "LOSS",
        "exit_reason": reason,
        "shares": 0.0,
        "total_cost": 0.0,
        "average_entry": 0.0,
    })

    state["our_realized_pnl"] = num(state.get("our_realized_pnl")) + pnl
    state["settled_positions"] = int(state.get("settled_positions", 0)) + 1
    state["settlement_wins"] = int(state.get("settlement_wins", 0)) + int(win)
    state["settlement_losses"] = int(state.get("settlement_losses", 0)) + int(not win)
    state["exit_events"] = int(state.get("exit_events", 0)) + 1

    state.setdefault("closed_trades", []).append({
        "time_ist": ist(closed_at),
        "market": pos.get("market"),
        "position_id": pos.get("position_id"),
        "asset": position_asset(pos),
        "condition_id": position_condition(pos),
        "outcome": pos.get("outcome"),
        "shares": money(shares),
        "average_entry": money(cost / shares if shares else 0),
        "cost": money(cost),
        "settlement_price": settlement_price,
        "settlement_value": money(proceeds),
        "pnl": money(pnl),
        "result": "WIN" if win else "LOSS",
        "holding_seconds": closed_at - pos["first_buy_timestamp"] if pos.get("first_buy_timestamp") else None,
        "exit_reason": reason,
    })

    print("")
    print("🏁 MARKET RESOLVED")
    print(f"   {pos.get('market', 'Unknown market')}")
    print(f"   Result: {'WIN' if win else 'LOSS'}")
    print(f"   Settlement: ${settlement_price:.2f}/share | P&L: ${pnl:+.4f}")
    return True


def resolve_matching_local_positions(state, api_row, win, reason):
    changed = 0
    for pos in list(state.get("our_positions", {}).values()):
        if pos.get("status") == "OPEN" and same_market_position(pos, api_row):
            changed += int(resolve_position_object(state, pos, win, reason))
    return changed


def settle_trader_positions(state, win, market_pos):
    """Settle observed trader-ledger positions for the same resolved market."""
    changed = 0
    settlement_price = 1.0 if win else 0.0
    for pos in list(state.get("trader_positions", {}).values()):
        if pos.get("status") != "OPEN" or position_shares(pos) <= 1e-9:
            continue
        if not same_market_position(pos, market_pos):
            continue
        shares = position_shares(pos)
        cost = position_entry_cost(pos)
        proceeds = shares * settlement_price
        pnl = proceeds - cost
        closed_at = now()
        pos.update({
            "status": "SETTLED", "closed_at": closed_at, "closed_at_ist": ist(closed_at),
            "settlement_price": settlement_price, "settlement_proceeds": proceeds,
            "settlement_pnl": pnl, "settlement_result": "WIN" if win else "LOSS",
            "exit_reason": "MARKET_RESOLUTION", "shares": 0.0,
            "total_cost": 0.0, "average_entry": 0.0,
        })
        state["trader_realized_pnl"] = num(state.get("trader_realized_pnl")) + pnl
        state["trader_settled_positions"] = int(state.get("trader_settled_positions", 0)) + 1
        state["trader_settlement_wins"] = int(state.get("trader_settlement_wins", 0)) + int(win)
        state["trader_settlement_losses"] = int(state.get("trader_settlement_losses", 0)) + int(not win)
        changed += 1
    return changed


def market_end_ts(obj):
    explicit = first(
        obj,
        "endDate", "end_date", "endDateIso", "endTime", "end_time",
        "marketEnd", "market_end", "expiration", "expirationTime",
        "expiration_time",
        default=None,
    )
    if explicit is not None:
        if isinstance(explicit, (int, float)):
            v = float(explicit)
            return v / 1000.0 if v > 10_000_000_000 else v
        try:
            return float(explicit)
        except Exception:
            try:
                return datetime.fromisoformat(str(explicit).replace("Z", "+00:00")).timestamp()
            except Exception:
                pass
    return market_end(obj)


def _market_query(pos):
    """Fetch the exact Gamma market. Slug is preferred; condition is fallback."""
    slug = str(pos.get("slug") or market_slug(pos) or "").strip()
    condition = position_condition(pos).strip()

    # Direct slug lookup avoids accidentally matching an event/adjacent
    # market when the list endpoint returns multiple records.
    if slug:
        try:
            row = get_json(
                GAMMA_MARKET_BY_SLUG_URL.format(slug=slug),
                timeout=NORMAL_TIMEOUT,
            )
            if isinstance(row, dict):
                if (
                    (not condition or str(row.get("conditionId", "")).lower() == condition.lower())
                    and (not slug or str(row.get("slug", "")) == slug)
                ):
                    return row
        except Exception:
            pass

    attempts = []
    if slug:
        attempts.append({"slug": slug, "limit": 5})
    if condition:
        attempts.append({"condition_ids": [condition], "limit": 5})
        attempts.append({"condition_ids": condition, "limit": 5})

    for params in attempts:
        try:
            data = get_json(GAMMA_MARKETS_URL, params, timeout=NORMAL_TIMEOUT)
            if isinstance(data, dict):
                # Some endpoints/wrappers return {markets:[...]}
                rows = data.get("markets", [])
                if isinstance(rows, list):
                    data = rows
                else:
                    data = [data]
            if not isinstance(data, list):
                continue

            for row in data:
                if not isinstance(row, dict):
                    continue
                if slug and str(row.get("slug", "")) == slug:
                    return row
                if condition and str(row.get("conditionId", "")).lower() == condition.lower():
                    return row
        except Exception:
            continue

    return None


def _clob_market_query(condition_id):
    """Fetch the authoritative CLOB market state by condition ID."""
    condition_id = str(condition_id or "").strip()
    if not condition_id:
        return None
    try:
        return get_json(
            CLOB_MARKET_URL.format(condition_id=condition_id),
            timeout=NORMAL_TIMEOUT,
        )
    except Exception:
        return None


def _clob_resolved_winner(market, pos):
    """
    CLOB market objects expose `closed` plus per-token `winner`.
    This is preferable to inferring a result from a live price.
    """
    if not isinstance(market, dict):
        return None

    tokens = market.get("tokens")
    if not isinstance(tokens, list):
        return None

    if market.get("closed") is not True:
        return None

    asset = position_asset(pos)
    outcome = position_outcome(pos)

    for token in tokens:
        if not isinstance(token, dict):
            continue
        token_id = str(first(token, "token_id", "tokenId", "id", default=""))
        token_outcome = str(first(token, "outcome", "label", default="")).strip().lower()
        if asset and token_id == asset:
            winner = token.get("winner")
            if isinstance(winner, bool):
                return winner
        if not asset and outcome and token_outcome == outcome:
            winner = token.get("winner")
            if isinstance(winner, bool):
                return winner

    return None


def _outcome_pairs(market):
    outcomes = _json_list(market.get("outcomes"))
    prices = _json_list(market.get("outcomePrices"))
    tokens = _json_list(market.get("clobTokenIds"))
    return outcomes, prices, tokens


def _resolved_winner(market, pos):
    """Return True/False only for an officially finalized binary outcome."""
    if not isinstance(market, dict):
        return None

    outcomes, prices, tokens = _outcome_pairs(market)
    if len(prices) < 2:
        return None

    # We intentionally do NOT settle from bestBid/bestAsk/lastTradePrice.
    # Those are market prices, not the resolution result.
    numeric_prices = []
    for p in prices:
        try:
            numeric_prices.append(float(p))
        except Exception:
            numeric_prices.append(None)

    if not any(p is not None for p in numeric_prices):
        return None

    idx = None
    asset = position_asset(pos)
    outcome = position_outcome(pos)

    if asset and tokens:
        for i, token in enumerate(tokens):
            if str(token) == asset:
                idx = i
                break

    if idx is None and outcome and outcomes:
        for i, label in enumerate(outcomes):
            if str(label).strip().lower() == outcome:
                idx = i
                break

    if idx is None:
        try:
            oi = int(float(outcome))
            if 0 <= oi < len(numeric_prices):
                idx = oi
        except Exception:
            pass

    if idx is None or idx >= len(numeric_prices):
        return None

    p = numeric_prices[idx]
    if p is None:
        return None

    # Finalized binary markets have one token at 1 and the other at 0.
    # Allow a tiny tolerance for API serialization.
    if p >= 0.999:
        return True
    if p <= 0.001:
        return False
    return None


def _resolution_status(market):
    value = first(
        market,
        "umaResolutionStatus",
        "uma_resolution_status",
        "umaResolutionStatuses",
        "uma_resolution_statuses",
        default="",
    )
    if isinstance(value, (list, tuple)):
        return ",".join(str(x).strip().lower() for x in value if x is not None)
    text = str(value or "").strip().lower()
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return ",".join(str(x).strip().lower() for x in parsed)
        except Exception:
            pass
    return text


def settle_due_positions(state, feed=None):
    """
    Primary paper expiry engine.

    Each market is queried at most once per resolution cycle. Resolution is
    based on the official CLOB winner flag first, with Gamma's finalized
    outcomePrices as a fallback. No live bid/ask or last-trade price is ever
    used as the settlement result.
    """
    changed = 0
    due = 0
    unresolved = 0
    market_cache = {}
    clob_cache = {}

    for pos in list(state.get("our_positions", {}).values()):
        if not isinstance(pos, dict) or pos.get("status") != "OPEN":
            continue

        end_ts = num(pos.get("market_end_ts"), 0) or market_end_ts(pos)
        if not end_ts:
            continue
        if now() < end_ts + EXIT_GRACE_SECONDS:
            continue

        due += 1
        condition = position_condition(pos)
        slug = str(pos.get("slug") or market_slug(pos) or "").strip()
        cache_key = condition.lower() if condition else slug.lower()

        if cache_key not in clob_cache:
            clob_cache[cache_key] = _clob_market_query(condition)
        clob_market = clob_cache[cache_key]

        # CLOB is the first authority and does not depend on Gamma being
        # queryable. This is important during brief Gamma indexing delays.
        win = _clob_resolved_winner(clob_market, pos)
        if win is not None:
            changed += int(resolve_position_object(state, pos, win, "MARKET_RESOLUTION"))
            changed += settle_trader_positions(state, win, pos)
            continue

        if cache_key not in market_cache:
            market_cache[cache_key] = _market_query(pos)
        market = market_cache[cache_key]

        if market is not None:
            win = _resolved_winner(market, pos)
            if win is not None:
                changed += int(resolve_position_object(state, pos, win, "MARKET_RESOLUTION"))
                continue

        unresolved += 1
        gamma_status = _resolution_status(market) if isinstance(market, dict) else "not_found"
        gamma_closed = market.get("closed") if isinstance(market, dict) else None
        clob_closed = clob_market.get("closed") if isinstance(clob_market, dict) else None
        wait_key = cache_key or str(pos.get("position_id") or pos.get("key") or pos.get("market") or "unknown")
        wait_log = state.setdefault("resolution_wait_log", {})
        last_wait = num(wait_log.get(wait_key), 0)
        if now() - last_wait >= RESOLUTION_WAIT_LOG_EVERY:
            print(
                f"  ⏳ RESOLUTION WAIT | {pos.get('market', 'unknown')} "
                f"gamma_closed={gamma_closed} clob_closed={clob_closed} "
                f"uma={gamma_status or 'unknown'}"
            )
            wait_log[wait_key] = now()

        # CRITICAL: unresolved is NOT a loss. Never pass win=None into
        # resolve_position_object(), because that function treats any
        # falsey value as LOSS. Keep the paper position OPEN and retry on
        # the next resolution cycle.

    # Resolve trader-ledger positions independently of OUR positions. This is
    # essential for the price-filter bot: a skipped OUR BUY is still a real
    # trader BUY and must count toward the trader benchmark P&L.
    for tpos in list(state.get("trader_positions", {}).values()):
        if not isinstance(tpos, dict) or tpos.get("status") != "OPEN":
            continue
        end_ts = num(tpos.get("market_end_ts"), 0) or market_end_ts(tpos)
        if not end_ts or now() < end_ts + EXIT_GRACE_SECONDS:
            continue
        condition = position_condition(tpos)
        slug = str(tpos.get("slug") or market_slug(tpos) or "").strip()
        cache_key = condition.lower() if condition else slug.lower()
        if cache_key not in clob_cache:
            clob_cache[cache_key] = _clob_market_query(condition)
        clob_market = clob_cache[cache_key]
        win = _clob_resolved_winner(clob_market, tpos)
        if win is None:
            if cache_key not in market_cache:
                market_cache[cache_key] = _market_query(tpos)
            market = market_cache[cache_key]
            win = _resolved_winner(market, tpos) if market is not None else None
        if win is not None:
            changed += settle_trader_positions(state, win, tpos)

    state["resolution_due_positions"] = due
    state["resolution_unresolved_positions"] = unresolved
    state["resolution_last_changed"] = changed
    state["resolution_markets_checked"] = len(market_cache)
    state["resolution_due_positions"] = due
    state["resolution_unresolved_positions"] = unresolved
    state["resolution_markets_checked"] = len(clob_cache)
    return changed


def _win_from_position_row(row):
    cur = num(first(row, "curPrice", "currentPrice", "currPrice", default=-1), -1)
    if cur >= 0.999:
        return True
    if cur <= 0.001:
        return False
    return None


def apply_redeemable_positions(state, rows):
    # Real-wallet positions are not the source of truth for paper positions.
    # Kept only as a compatibility/diagnostic path.
    changed = 0
    checked = 0
    for row in rows or []:
        if not isinstance(row, dict) or not bool(row.get("redeemable")):
            continue
        checked += 1
        win = _win_from_position_row(row)
        if win is not None:
            changed += resolve_matching_local_positions(state, row, win, "REDEEMABLE_POSITION_API")
    state["resolution_redeemable_checked"] = int(state.get("resolution_redeemable_checked", 0)) + checked
    state["resolution_last_changed"] = changed
    return changed


def apply_closed_positions(state, rows):
    changed = 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        win = _win_from_position_row(row)
        if win is not None:
            changed += resolve_matching_local_positions(state, row, win, "CLOSED_POSITION_API")
    return changed


def resolve_cycle(state, feed=None):
    """Run every exit/resolution route and return number of positions closed."""
    changed = 0
    # Paper expiry is authoritative. Real-wallet endpoints are only fallbacks.
    changed += settle_due_positions(state, feed)
    if changed == 0:
        changed += apply_redeemable_positions(state, fetch_redeemable_positions())
    if changed == 0:
        changed += apply_closed_positions(state, fetch_closed_positions())
    return changed
