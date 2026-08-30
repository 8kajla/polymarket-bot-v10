from __future__ import annotations

import statistics

from config import *
from utils import *
from feeds.core import ws_status_text
from trading.ledger import open_capital


REGIMES = ("CHEAP", "MID", "CORE", "HIGH")


def stats(values):
    values = [num(x) for x in values if x is not None]
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "average": money(sum(values) / len(values)),
        "minimum": money(min(values)),
        "maximum": money(max(values)),
    }


def _price(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def regime_from_price(value):
    p = _price(value)
    if p is None:
        return "UNKNOWN"
    if 0.01 <= p < 0.30:
        return "CHEAP"
    if 0.30 <= p < 0.70:
        return "MID"
    if 0.70 <= p < 0.90:
        return "CORE"
    if 0.90 <= p < 0.995:
        return "HIGH"
    return "OTHER"


def _field(row, *names, default=None):
    if not isinstance(row, dict):
        return default
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return default


def _notional(row):
    value = _field(
        row,
        "notional",
        "trader_notional",
        "size_usd",
        "usd",
        "cost",
        "amount",
        default=None,
    )
    if value is not None:
        return num(value)

    price = _price(
        _field(
            row,
            "price",
            "trader_price",
            "entry_price",
            "execution_price",
            default=None,
        )
    )
    size = _field(
        row,
        "size",
        "shares",
        "quantity",
        "amount_shares",
        default=None,
    )
    if price is not None and size is not None:
        return price * num(size)

    return 0.0


def _trade_price(row):
    return _price(
        _field(
            row,
            "price",
            "trader_price",
            "entry_price",
            "execution_price",
            default=None,
        )
    )


def _trade_side(row):
    return str(
        _field(
            row,
            "side",
            "action",
            "direction",
            default="",
        )
    ).upper()


def _market_key(row):
    return str(
        _field(
            row,
            "condition_id",
            "conditionId",
            "condition",
            "market_id",
            "market",
            "slug",
            default="UNKNOWN",
        )
    )


def _regime_rows_from_observations(observations):
    """
    Analyze the trader's observed BUY executions.

    Important: this is an execution-distribution analysis. P&L by regime
    for the trader is only computed where a settled trader-position record
    contains enough entry-price/cost information to attribute it.
    """
    rows = [x for x in observations if _trade_side(x) == "BUY"]

    out = {
        regime: {
            "trades": 0,
            "notional": 0.0,
            "prices": [],
            "markets": set(),
        }
        for regime in REGIMES
    }

    for row in rows:
        price = _trade_price(row)
        regime = regime_from_price(price)
        if regime not in out:
            continue

        notional = _notional(row)
        out[regime]["trades"] += 1
        out[regime]["notional"] += notional

        if price is not None:
            out[regime]["prices"].append(price)

        out[regime]["markets"].add(_market_key(row))

    return out


def _settled_by_regime(closed_rows):
    """
    Attribute our settled positions by the recorded average entry price.
    This is position-level attribution, matching the data shape available
    in closed_trades.
    """
    out = {
        regime: {
            "settled": 0,
            "wins": 0,
            "losses": 0,
            "cost": 0.0,
            "pnl": 0.0,
            "pnl_values": [],
            "roi_values": [],
        }
        for regime in REGIMES
    }

    for row in closed_rows:
        if not isinstance(row, dict):
            continue

        price = _price(
            _field(
                row,
                "average_entry",
                "entry_price",
                "price",
                default=None,
            )
        )
        regime = regime_from_price(price)
        if regime not in out:
            continue

        pnl = num(
            _field(
                row,
                "pnl",
                "settlement_pnl",
                "realized_pnl",
                default=0,
            )
        )
        cost = num(
            _field(
                row,
                "cost",
                "notional",
                "entry_cost",
                "total_cost",
                default=0,
            )
        )

        out[regime]["settled"] += 1
        out[regime]["wins"] += int(pnl > 0)
        out[regime]["losses"] += int(pnl < 0)
        out[regime]["cost"] += cost
        out[regime]["pnl"] += pnl
        out[regime]["pnl_values"].append(pnl)

        if cost:
            out[regime]["roi_values"].append(pnl / cost)

    return out


def _trader_settled_by_regime(state):
    """
    Attribute trader realized positions by their recorded average entry.

    resolve.py retains SETTLED trader positions in trader_positions, so this
    provides a useful position-level trader view without inventing unseen
    per-fill attribution.
    """
    out = {
        regime: {
            "settled": 0,
            "wins": 0,
            "losses": 0,
            "cost": 0.0,
            "pnl": 0.0,
        }
        for regime in REGIMES
    }

    for row in state.get("trader_positions", {}).values():
        if not isinstance(row, dict):
            continue
        if row.get("status") not in {"SETTLED", "CLOSED"}:
            continue

        price = _price(
            _field(
                row,
                "average_entry",
                "entry_price",
                "buy_price",
                "price",
                default=None,
            )
        )
        regime = regime_from_price(price)
        if regime not in out:
            continue

        pnl = num(
            _field(
                row,
                "settlement_pnl",
                "pnl",
                "realized_pnl",
                default=0,
            )
        )
        cost = num(
            _field(
                row,
                "entry_cost",
                "total_cost",
                "cost",
                "cost_basis",
                default=0,
            )
        )

        out[regime]["settled"] += 1
        out[regime]["wins"] += int(pnl > 0)
        out[regime]["losses"] += int(pnl < 0)
        out[regime]["cost"] += cost
        out[regime]["pnl"] += pnl

    return out


def _analytics_snapshot(state):
    rows = [
        x for x in state.get("trade_observations", [])
        if _trade_side(x) == "BUY"
    ]

    lat = [
        num(x.get("latency_ms"))
        for x in rows
        if x.get("latency_ms") is not None
    ]
    gaps = [
        num(x.get("gap_pct"))
        for x in rows
        if x.get("gap_pct") is not None
    ]

    def bucket(value, edges):
        for i, edge in enumerate(edges):
            if value < edge:
                return i
        return len(edges)

    lat_counts = [0] * 5
    for value in lat:
        lat_counts[bucket(value, [250, 500, 1000, 2000])] += 1

    gap_counts = [0] * 6
    for value in gaps:
        gap_counts[bucket(value, [-10, 0, 5, 10, 25])] += 1

    sources = {}
    books = {}
    markets = {}

    for row in rows:
        source = row.get("source", "unknown")
        book = row.get("book", "unknown")
        market = row.get("market", "unknown")

        sources[source] = sources.get(source, 0) + 1
        books[book] = books.get(book, 0) + 1
        markets[market] = markets.get(market, 0) + 1

    trader_regimes = _regime_rows_from_observations(rows)

    total_trades = sum(
        row["trades"]
        for row in trader_regimes.values()
    )

    trader_regime_view = {}

    for regime in REGIMES:
        row = trader_regimes[regime]
        prices = row["prices"]

        trader_regime_view[regime] = {
            "trades": row["trades"],
            "share_pct": (
                row["trades"] / total_trades * 100
                if total_trades
                else 0
            ),
            "notional": money(row["notional"]),
            "avg_notional": (
                money(row["notional"] / row["trades"])
                if row["trades"]
                else 0
            ),
            "median_entry_price": (
                money(statistics.median(prices))
                if prices
                else None
            ),
            "markets": len(row["markets"]),
        }

    return {
        "copies": len(rows),
        "latency_buckets": {
            "<250ms": lat_counts[0],
            "250-500ms": lat_counts[1],
            "500-1000ms": lat_counts[2],
            "1-2s": lat_counts[3],
            ">=2s": lat_counts[4],
        },
        "gap_buckets": {
            "<-10%": gap_counts[0],
            "-10_to_0": gap_counts[1],
            "0_to_5%": gap_counts[2],
            "5_to_10%": gap_counts[3],
            "10_to_25%": gap_counts[4],
            ">=25%": gap_counts[5],
        },
        "sources": sources,
        "books": books,
        "markets": markets,
        "avg_latency_ms": (
            sum(lat) / len(lat) if lat else 0
        ),
        "avg_gap_pct": (
            sum(gaps) / len(gaps) if gaps else 0
        ),
        "trader_regimes": trader_regime_view,
    }


def _write_json(path, value):
    save(path, value)


def _write_detailed_files(state, analytics, our_regimes, trader_settled):
    """
    Persist additional comparison files without changing existing file names.
    """
    try:
        regime_path = DATA_DIR / "regime_analysis_v7.json"
        comparison_path = DATA_DIR / "regime_comparison_v7.json"
        _write_json(
            regime_path,
            {
                "updated_ist": ist(),
                "trader_execution_regimes": analytics["trader_regimes"],
                "our_settled_regimes": our_regimes,
                "trader_settled_regimes": trader_settled,
            },
        )

        rows = []
        for regime in REGIMES:
            tr = analytics["trader_regimes"][regime]
            ours = our_regimes[regime]
            trader_closed = trader_settled[regime]

            rows.append(
                {
                    "regime": regime,
                    "trader_trades": tr["trades"],
                    "trader_trade_share_pct": tr["share_pct"],
                    "trader_notional": tr["notional"],
                    "trader_avg_notional": tr["avg_notional"],
                    "trader_median_entry_price": tr["median_entry_price"],
                    "trader_settled_positions": trader_closed["settled"],
                    "trader_wins": trader_closed["wins"],
                    "trader_losses": trader_closed["losses"],
                    "trader_settled_pnl": trader_closed["pnl"],
                    "our_settled_positions": ours["settled"],
                    "our_wins": ours["wins"],
                    "our_losses": ours["losses"],
                    "our_settled_cost": ours["cost"],
                    "our_settled_pnl": ours["pnl"],
                    "our_settled_roi_pct": (
                        ours["pnl"] / ours["cost"] * 100
                        if ours["cost"]
                        else 0
                    ),
                }
            )

        _write_json(comparison_path, rows)

        return True
    except Exception as exc:
        print(
            f"RESEARCH FILE ERROR | "
            f"{type(exc).__name__}: {exc}"
        )
        return False


def write_reports(state, feed_diag, position_diag, recon):
    open_cap = open_capital(state)
    closed = state["closed_trades"]

    wins = sum(
        1 for row in closed
        if num(row.get("pnl")) > 0
    )
    losses = sum(
        1 for row in closed
        if num(row.get("pnl")) < 0
    )

    analytics = _analytics_snapshot(state)

    our_settled = _settled_by_regime(closed)
    trader_settled = _trader_settled_by_regime(state)

    _write_detailed_files(
        state,
        analytics,
        our_settled,
        trader_settled,
    )

    summary = {
        "version": "7.2",
        "updated_ist": ist(),
        "wallet": WALLET,
        "simulator": {
            "max_capital": MAX_OPEN_CAPITAL,
            "open_capital": money(open_cap),
            "available_capital": money(
                max(0, MAX_OPEN_CAPITAL - open_cap)
            ),
            "realized_pnl": money(
                state["our_realized_pnl"]
            ),
            "copy_notional_fraction": COPY_NOTIONAL_FRACTION,
            "copied_buys": state["copied_buys"],
            "copied_sells": state["copied_sells"],
            "duplicates_ignored": state["duplicates_ignored"],
            "closed_trades": len(closed),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": (
                money(wins / len(closed) * 100)
                if closed else 0
            ),
            "skipped_capital": state["skipped_capital"],
            "skipped_liquidity": state["skipped_liquidity"],
            "api_errors": state["api_errors"],
        },
        "trader": {
            "realized_pnl": money(
                state["trader_realized_pnl"]
            ),
            "open_positions": sum(
                1
                for row in state["trader_positions"].values()
                if row["status"] == "OPEN"
                and row["shares"] > 1e-9
            ),
            "settled_positions": state.get(
                "trader_settled_positions",
                0,
            ),
            "wins": state.get(
                "trader_settlement_wins",
                0,
            ),
            "losses": state.get(
                "trader_settlement_losses",
                0,
            ),
            "win_rate_pct": money(
                state.get("trader_settlement_wins", 0)
                / max(
                    1,
                    state.get("trader_settlement_wins", 0)
                    + state.get("trader_settlement_losses", 0),
                )
                * 100
            ),
        },
        "live_feed": feed_diag,
        "sell_diagnostics": {
            "detected": state["sell_detected"],
            "processed": state["sell_processed"],
            "no_position": state[
                "sell_rejected_no_position"
            ],
            "liquidity": state[
                "sell_rejected_liquidity"
            ],
            "pending": len(
                state.get("pending_sells", {})
            ),
            "resolution_due": state.get(
                "resolution_due_positions",
                0,
            ),
            "resolution_redeemable_checked": state.get(
                "resolution_redeemable_checked",
                0,
            ),
        },
        "latency": {
            "count": state["latency_count"],
            "avg_ms": (
                state["latency_sum_ms"]
                / max(1, state["latency_count"])
            ),
            "min_ms": state["latency_min_ms"],
            "max_ms": state["latency_max_ms"],
        },
        "reconciliation": recon,
        "execution": {
            "latency_ms": stats(
                state["latency_ms"]
            ),
            "entry_slippage_pct": stats(
                state["entry_slippage_pct"]
            ),
            "exit_slippage_pct": stats(
                state["exit_slippage_pct"]
            ),
            "analytics": analytics,
        },
        "strategy_research": {
            "our_settled_regimes": our_settled,
            "trader_settled_regimes": trader_settled,
        },
    }

    save(
        FILES["summary"],
        summary,
    )
    save(
        FILES["trader_positions"],
        list(
            state["trader_positions"].values()
        ),
    )
    save(
        FILES["our_positions"],
        list(
            state["our_positions"].values()
        ),
    )
    save(
        FILES["closed_trades"],
        state["closed_trades"],
    )
    save(
        FILES["fills"],
        state["fills"],
    )
    save(
        FILES["reconciliation"],
        state["reconciliation"],
    )
    save(
        FILES["state"],
        state,
    )


def _print_regime_block(title, rows):
    print(title)

    for regime in REGIMES:
        row = rows[regime]

        if "share_pct" in row:
            print(
                f"  {regime:<5} | "
                f"trades={row['trades']} | "
                f"share={row['share_pct']:.1f}% | "
                f"notional=${row['notional']:.2f} | "
                f"avg_order=${row['avg_notional']:.2f} | "
                f"median_px={row['median_entry_price'] if row['median_entry_price'] is not None else 'n/a'}"
            )
        else:
            settled = row["settled"]
            wr = (
                row["wins"] / settled * 100
                if settled
                else 0
            )
            roi = (
                row["pnl"] / row["cost"] * 100
                if row["cost"]
                else 0
            )

            print(
                f"  {regime:<5} | "
                f"settled={settled} | "
                f"W/L={row['wins']}/{row['losses']} "
                f"({wr:.0f}%) | "
                f"cost=${row['cost']:.2f} | "
                f"pnl=${row['pnl']:+.4f} | "
                f"ROI={roi:+.2f}%"
            )


def print_status(state, feed_diag, recon, force=False):
    if (
        not force
        and now() - num(state.get("last_status"))
        < STATUS_EVERY
    ):
        return

    state["last_status"] = now()

    open_cap = open_capital(state)
    available = max(
        0,
        MAX_OPEN_CAPITAL - open_cap,
    )

    try:
        from trading.priority import stats as priority_stats
        ps = priority_stats()
    except Exception:
        ps = {
            "queue_depth": 0,
            "processed": 0,
            "errors": 0,
            "last_copy_ms": 0,
            "last_total_ms": 0,
            "copy_avg_ms": 0,
            "copy_p95_ms": 0,
            "last_queue_ms": 0,
        }

    try:
        from feeds.market_book import status as book_status
        bs = book_status()
    except Exception:
        bs = {
            "connected": False,
            "subscribed": 0,
            "snapshots": 0,
            "price_changes": 0,
            "fallbacks": 0,
            "reconnects": 0,
            "stale_reconnects": 0,
        }

    trader_w = state.get(
        "trader_settlement_wins",
        0,
    )
    trader_l = state.get(
        "trader_settlement_losses",
        0,
    )
    trader_total = trader_w + trader_l
    trader_wr = (
        trader_w / trader_total * 100
        if trader_total
        else 0
    )

    analytics = _analytics_snapshot(state)

    our_settled = _settled_by_regime(
        state.get("closed_trades", [])
    )
    trader_settled = _trader_settled_by_regime(
        state
    )

    print("")
    print(
        f"[{ist()}] STATUS | "
        f"TRADER WS {ws_status_text()} | "
        f"CLOB WS "
        f"{'LIVE' if bs.get('connected') else 'OFF'}"
    )

    print(
        f"COPY  "
        f"{state['copied_buys']}B/"
        f"{state['copied_sells']}S | "
        f"{COPY_NOTIONAL_FRACTION * 100:.0f}% size | "
        f"queue {ps.get('queue_depth', 0)}"
    )

    print(
        f"BOOK  "
        f"{bs.get('subscribed', 0)} assets | "
        f"snapshots {bs.get('snapshots', 0)} | "
        f"changes {bs.get('price_changes', 0)} | "
        f"REST fallbacks {bs.get('fallbacks', 0)}"
    )

    print(
        f"LAT   "
        f"copy avg {ps.get('copy_avg_ms', 0):.0f}ms | "
        f"p95 {ps.get('copy_p95_ms', 0):.0f}ms | "
        f"last {ps.get('last_total_ms', 0):.0f}ms | "
        f"queue {ps.get('last_queue_ms', 0):.0f}ms"
    )

    print(
        f"CAP   open ${open_cap:.2f} | "
        f"free ${available:.2f}/${MAX_OPEN_CAPITAL:.2f}"
    )

    print(
        f"P&L   ours "
        f"${state['our_realized_pnl']:+.2f} | "
        f"trader "
        f"${state['trader_realized_pnl']:+.2f} | "
        f"trader W/L {trader_w}/{trader_l} "
        f"({trader_wr:.0f}%)"
    )

    print(
        f"EXIT  settled {state['settled_positions']} | "
        f"W/L {state['settlement_wins']}/"
        f"{state['settlement_losses']} | "
        f"open "
        f"{sum(1 for row in state['our_positions'].values() if row.get('status') == 'OPEN' and num(row.get('shares')) > 1e-9)}"
    )

    trader_rows = analytics["trader_regimes"]

    print(
        "TRADER REGIME | "
        "price-based execution distribution"
    )

    for regime in REGIMES:
        row = trader_rows[regime]
        settled = trader_settled[regime]
        settled_wr = (
            settled["wins"] / settled["settled"] * 100
            if settled["settled"]
            else 0
        )

        print(
            f"  {regime:<5} | "
            f"trades={row['trades']} | "
            f"share={row['share_pct']:.1f}% | "
            f"notional=${row['notional']:.2f} | "
            f"avg=${row['avg_notional']:.2f} | "
            f"median_px={row['median_entry_price'] if row['median_entry_price'] is not None else 'n/a'} | "
            f"settled W/L={settled['wins']}/{settled['losses']} "
            f"({settled_wr:.0f}%) | "
            f"settled_pnl=${settled['pnl']:+.4f}"
        )

    print(
        "OUR REGIME | "
        "settled position attribution"
    )

    for regime in REGIMES:
        row = our_settled[regime]
        settled = row["settled"]
        wr = (
            row["wins"] / settled * 100
            if settled
            else 0
        )
        roi = (
            row["pnl"] / row["cost"] * 100
            if row["cost"]
            else 0
        )

        print(
            f"  {regime:<5} | "
            f"settled={settled} | "
            f"W/L={row['wins']}/{row['losses']} "
            f"({wr:.0f}%) | "
            f"cost=${row['cost']:.2f} | "
            f"pnl=${row['pnl']:+.4f} | "
            f"ROI={roi:+.2f}%"
        )

    total_settled_pnl = sum(
        row["pnl"]
        for row in our_settled.values()
    )

    print(
        f"REGIME RECON | "
        f"our_settled_pnl=${total_settled_pnl:+.4f} | "
        f"ledger_realized="
        f"${state['our_realized_pnl']:+.4f} | "
        f"diff="
        f"${total_settled_pnl - num(state['our_realized_pnl']):+.4f}"
    )

    a = analytics

    print(
        f"ANALYTICS copies {a['copies']} | "
        f"lat <250/{a['latency_buckets']['<250ms']} "
        f"250-500/{a['latency_buckets']['250-500ms']} "
        f"500-1s/{a['latency_buckets']['500-1000ms']} "
        f"1-2s/{a['latency_buckets']['1-2s']} "
        f">2s/{a['latency_buckets']['>=2s']}"
    )

    print(
        f"ANALYTICS gap "
        f"<-10/{a['gap_buckets']['<-10%']} "
        f"-10..0/{a['gap_buckets']['-10_to_0']} "
        f"0..5/{a['gap_buckets']['0_to_5%']} "
        f"5..10/{a['gap_buckets']['5_to_10%']} "
        f"10..25/{a['gap_buckets']['10_to_25%']} "
        f">=25/{a['gap_buckets']['>=25%']} | "
        f"avg gap {a['avg_gap_pct']:+.2f}%"
    )

    print(
        f"API   {state['api_requests']} req | "
        f"{state['api_errors']} err | "
        f"reconcile {recon.get('matches', 0)} "
        f"match/{recon.get('share_mismatches', 0)} diff"
    )

    if (
        ps.get("errors")
        or state.get("api_errors")
        or bs.get("stale_reconnects")
    ):
        print(
            f"WARN  copy {ps.get('errors', 0)} errors | "
            f"API {state.get('api_errors', 0)} | "
            f"CLOB stale reconnects "
            f"{bs.get('stale_reconnects', 0)}"
        )

    print("-" * 72)
