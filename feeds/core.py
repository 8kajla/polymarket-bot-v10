from __future__ import annotations

import json
import os
import queue
import random
import threading
import time

from config import *
from utils import *
from audit import audit_event
from feeds.race import record as record_feed_race


# ============================================================
# LIVE WEBSOCKET STATE
# ============================================================

LIVE_WS_QUEUE = queue.Queue(maxsize=WS_QUEUE_MAX)

LIVE_WS_THREAD = None
LIVE_WS_STOP = threading.Event()

LIVE_WS_STATUS = {
    "enabled": USE_WEBSOCKET,
    "available": False,
    "connected": False,
    "last_message": 0.0,
    "last_error": "",
    "reconnects": 0,
    "stale_reconnects": 0,
    "received": 0,
    "ignored_wallet": 0,
    "malformed": 0,
    "wallet_matches": 0,
    "buy_candidates": 0,
    "sell_candidates": 0,
    "queued": 0,
    "dropped": 0,
    "duplicates_ignored": 0,
}


# ============================================================
# DEDUP CONFIG
# ============================================================

DEDUP_TTL_SECONDS = max(
    30,
    int(
        os.getenv(
            "FEED_DEDUP_TTL_SECONDS",
            "180",
        )
    ),
)

ECONOMIC_MATCH_SECONDS = max(
    1.0,
    float(
        os.getenv(
            "FEED_ECONOMIC_MATCH_SECONDS",
            "3.0",
        )
    ),
)


# ============================================================
# BASIC HELPERS
# ============================================================

def _source(row):
    return str(
        row.get(
            "_feed_source",
            "unknown",
        )
    ).lower()


def _explicit_execution_id(row):
    """
    Prefer identifiers that identify a real blockchain/fill
    execution. These are much safer than economic matching.
    """

    for key in (
        "transactionHash",
        "transaction_hash",
        "txHash",
        "tx_hash",
        "hash",
        "tradeId",
        "trade_id",
        "matchId",
        "match_id",
        "fillId",
        "fill_id",
    ):
        value = row.get(key)

        if value not in (
            None,
            "",
            "0",
        ):
            return str(value).strip().lower()

    return ""


def _economic_key(row):
    """
    Fallback economic fingerprint.

    Used only when no explicit execution identifier exists.
    """

    return "|".join(
        [
            trade_asset(row),
            trade_condition(row),
            str(trade_outcome(row)),
            trade_side(row),
            f"{trade_size(row):.10f}",
            f"{trade_price(row):.10f}",
        ]
    )


def _execution_key(row):
    """
    Stable execution key.

    Explicit transaction/fill ID has priority.

    Otherwise use the economic key plus timestamp.
    """

    explicit = _explicit_execution_id(row)

    if explicit:
        return "ID|" + explicit

    ts = trade_ts(row)

    # Millisecond precision when there is no explicit ID.
    return (
        "ECON|"
        + _economic_key(row)
        + "|"
        + str(int(ts * 1000))
    )


def _economic_bucket(row):
    """
    Coarser fallback key used to identify the same execution
    when Activity/Trades/WS timestamps differ slightly.
    """

    ts = trade_ts(row)

    bucket = round(
        ts / ECONOMIC_MATCH_SECONDS
    )

    return (
        "E|"
        + _economic_key(row)
        + "|"
        + str(bucket)
    )


def _recent_state(state):
    """
    Persistent recent execution cache.

    Stored in state so the copy engine can remember executions
    between polling cycles.
    """

    cache = state.setdefault(
        "feed_seen_executions",
        {},
    )

    if not isinstance(cache, dict):
        cache = {}
        state["feed_seen_executions"] = cache

    return cache


def _cleanup_seen(state, current_ts):
    cache = _recent_state(state)

    cutoff = current_ts - DEDUP_TTL_SECONDS

    stale = [
        key
        for key, value in cache.items()
        if num(value, 0) < cutoff
    ]

    for key in stale:
        cache.pop(key, None)

    # Hard safety limit.
    if len(cache) > 50000:
        ordered = sorted(
            cache.items(),
            key=lambda x: num(x[1], 0),
        )

        remove_count = len(cache) - 40000

        for key, _ in ordered[:remove_count]:
            cache.pop(key, None)


def _mark_seen(state, row, current_ts=None):
    if current_ts is None:
        current_ts = now()

    cache = _recent_state(state)

    key = _execution_key(row)

    cache[key] = current_ts

    return key


def _already_seen(state, row, current_ts=None):
    """
    Check persistent execution cache.

    Explicit IDs are exact.

    Economic fallback uses a short timestamp tolerance.
    """

    if current_ts is None:
        current_ts = now()

    cache = _recent_state(state)

    key = _execution_key(row)

    if key in cache:
        return True

    explicit = _explicit_execution_id(row)

    if explicit:
        return False

    econ = _economic_key(row)
    ts = trade_ts(row)

    # Only compare fallback economic matches in a short window.
    # This avoids treating every identical fill throughout the
    # entire 3-minute feed window as one trade.
    for old_key, old_seen in cache.items():

        if not old_key.startswith("ECON|"):
            continue

        if abs(
            current_ts - num(old_seen, 0)
        ) > ECONOMIC_MATCH_SECONDS:
            continue

        parts = old_key.split("|")

        if len(parts) < 3:
            continue

        # Reconstruct the economic portion.
        old_econ = "|".join(
            parts[1:-1]
        )

        if old_econ != econ:
            continue

        return True

    return False


# ============================================================
# WEBSOCKET STATUS
# ============================================================

def ws_status_text():
    s = LIVE_WS_STATUS

    if not s["enabled"]:
        return "WS OFF"

    if s["connected"]:
        age = (
            now() - s["last_message"]
            if s["last_message"]
            else None
        )

        if age is None:
            return (
                "WS CONNECTED | "
                f"received {s['received']}"
            )

        return (
            "WS CONNECTED | "
            f"msg {age:.1f}s ago | "
            f"received {s['received']} | "
            f"duplicates {s['duplicates_ignored']}"
        )

    if s["available"]:
        return (
            "WS DISCONNECTED — REST RECOVERY | "
            f"reconnects {s['reconnects']} | "
            f"stale {s.get('stale_reconnects', 0)} | "
            f"{str(s['last_error'] or 'reconnecting')[:120]}"
        )

    return (
        "WS UNAVAILABLE | "
        f"{str(s['last_error'] or 'not started')[:140]}"
    )


# ============================================================
# WEBSOCKET DEPENDENCY
# ============================================================

def verify_websocket_dependency():
    if not USE_WEBSOCKET:
        return False

    try:
        import websocket

        LIVE_WS_STATUS["available"] = True
        LIVE_WS_STATUS["last_error"] = (
            "import OK "
            + str(
                getattr(
                    websocket,
                    "__version__",
                    "unknown",
                )
            )
        )

        return True

    except Exception as exc:

        LIVE_WS_STATUS["available"] = False
        LIVE_WS_STATUS["last_error"] = (
            f"IMPORT_FAILED "
            f"{type(exc).__name__}: {exc}"
        )

        return False


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def _ws_worker():

    try:
        import websocket

    except Exception as exc:

        LIVE_WS_STATUS["available"] = False
        LIVE_WS_STATUS["last_error"] = (
            f"IMPORT_FAILED "
            f"{type(exc).__name__}: {exc}"
        )

        return

    LIVE_WS_STATUS["available"] = True

    attempt = 0

    while not LIVE_WS_STOP.is_set():

        ws = None

        try:

            ws = websocket.create_connection(
                LIVE_WS_URL,
                timeout=max(
                    8,
                    FAST_TIMEOUT,
                ),
                enable_multithread=True,
                origin="https://polymarket.com",
            )

            ws.settimeout(1.0)

            ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": "activity",
                                "type": "trades",
                            },
                            {
                                "topic": "activity",
                                "type": "orders_matched",
                            },
                        ],
                    }
                )
            )

            LIVE_WS_STATUS["connected"] = True
            LIVE_WS_STATUS["last_error"] = ""
            attempt = 0

            # A WebSocket can remain OPEN while the RTDS application-data
            # stream silently stops. Track data on THIS connection separately
            # from the global last_message value so a dead stream cannot look
            # healthy for hours.
            connection_started = now()
            connection_last_data = connection_started
            last_ping = connection_started

            while not LIVE_WS_STOP.is_set():

                current = now()

                if (
                    current - last_ping
                    >= WS_HEARTBEAT_SECONDS
                ):
                    # RTDS uses a JSON PING heartbeat.
                    ws.send(
                        json.dumps(
                            {
                                "type": "PING"
                            }
                        )
                    )
                    last_ping = current

                # Ping/pong health is not the same as receiving RTDS data.
                # Force a reconnect when application data has gone stale.
                if (
                    current - connection_last_data
                    >= WS_DATA_STALE_AFTER
                ):
                    raise RuntimeError(
                        "RTDS data stale for "
                        f"{current - connection_last_data:.1f}s "
                        f"(threshold {WS_DATA_STALE_AFTER:.1f}s)"
                    )

                try:
                    raw = ws.recv()

                except Exception as exc:

                    text = str(exc).lower()

                    if (
                        "timed out"
                        in text
                        or "timeout"
                        in text
                    ):
                        continue

                    raise

                if raw is None:
                    raise RuntimeError(
                        "RTDS websocket returned EOF"
                    )

                if isinstance(
                    raw,
                    bytes,
                ):
                    raw = raw.decode(
                        "utf-8",
                        "replace",
                    )

                if not str(raw).strip():
                    continue

                try:
                    msg = json.loads(raw)

                except Exception:

                    LIVE_WS_STATUS[
                        "malformed"
                    ] += 1

                    continue

                # Any valid RTDS application message proves that this
                # connection is still delivering data. Do not use ping/pong
                # as the application-data freshness signal.
                connection_last_data = now()

                LIVE_WS_STATUS[
                    "last_message"
                ] = connection_last_data

                LIVE_WS_STATUS[
                    "received"
                ] += 1

                if not isinstance(
                    msg,
                    dict,
                ):
                    continue

                if (
                    str(
                        msg.get(
                            "topic",
                            "",
                        )
                    ).lower()
                    != "activity"
                ):
                    continue

                payload = msg.get(
                    "payload",
                    msg.get(
                        "data",
                        msg,
                    ),
                )

                rows = (
                    payload
                    if isinstance(
                        payload,
                        list,
                    )
                    else [payload]
                )

                for item in rows:

                    if not isinstance(
                        item,
                        dict,
                    ):
                        continue

                    trader = item.get(
                        "trader"
                    )

                    nested = (
                        trader.get(
                            "address"
                        )
                        if isinstance(
                            trader,
                            dict,
                        )
                        else ""
                    )

                    wallet = str(
                        first(
                            item,
                            "proxyWallet",
                            "proxy_wallet",
                            "user",
                            "wallet",
                            default=nested,
                        )
                    ).lower()

                    if (
                        wallet
                        and wallet != WALLET
                    ):

                        LIVE_WS_STATUS[
                            "ignored_wallet"
                        ] += 1

                        continue

                    if (
                        not wallet
                        and str(
                            nested
                        ).lower()
                        != WALLET
                    ):

                        LIVE_WS_STATUS[
                            "ignored_wallet"
                        ] += 1

                        continue

                    LIVE_WS_STATUS[
                        "wallet_matches"
                    ] += 1

                    normalized = (
                        normalize_feed_rows(
                            [item]
                        )
                    )

                    for trade in normalized:

                        # Mark source explicitly and preserve the exact local
                        # receive timestamp for latency instrumentation.
                        trade = dict(trade)
                        trade[
                            "_feed_source"
                        ] = "ws"
                        trade[
                            "_ws_received_at"
                        ] = now()

                        side = trade_side(
                            trade
                        )

                        LIVE_WS_STATUS[
                            "buy_candidates"
                        ] += int(
                            side == "BUY"
                        )

                        LIVE_WS_STATUS[
                            "sell_candidates"
                        ] += int(
                            side == "SELL"
                        )

                        # ------------------------------------------------
                        # WS-local duplicate protection.
                        #
                        # The queue itself has no state parameter, so use
                        # a short-lived module cache.
                        # ------------------------------------------------

                        record_feed_race("rtds", trade, trade.get("_ws_received_at"))

                        key = _execution_key(
                            trade
                        )

                        if not hasattr(
                            _ws_worker,
                            "_seen",
                        ):
                            _ws_worker._seen = {}

                        ws_seen = (
                            _ws_worker._seen
                        )

                        current = now()

                        # Cleanup.
                        for old_key in list(
                            ws_seen
                        ):
                            if (
                                current
                                - num(
                                    ws_seen[
                                        old_key
                                    ],
                                    0,
                                )
                                > DEDUP_TTL_SECONDS
                            ):
                                ws_seen.pop(
                                    old_key,
                                    None,
                                )

                        if key in ws_seen:

                            LIVE_WS_STATUS[
                                "duplicates_ignored"
                            ] += 1

                            continue

                        ws_seen[key] = current

                        try:

                            LIVE_WS_QUEUE.put_nowait(
                                trade
                            )

                            LIVE_WS_STATUS[
                                "queued"
                            ] += 1

                        except queue.Full:

                            LIVE_WS_STATUS[
                                "dropped"
                            ] += 1

                            try:

                                LIVE_WS_QUEUE.get_nowait()

                                LIVE_WS_QUEUE.put_nowait(
                                    trade
                                )

                            except Exception:
                                pass

        except Exception as exc:

            LIVE_WS_STATUS[
                "connected"
            ] = False

            LIVE_WS_STATUS[
                "available"
            ] = True

            LIVE_WS_STATUS[
                "last_error"
            ] = (
                f"{type(exc).__name__}: {exc}"
            )

            LIVE_WS_STATUS[
                "reconnects"
            ] += 1

            if "RTDS data stale" in str(exc):
                LIVE_WS_STATUS[
                    "stale_reconnects"
                ] += 1

            attempt += 1

        finally:

            LIVE_WS_STATUS[
                "connected"
            ] = False

            try:

                if ws:
                    ws.close()

            except Exception:
                pass

        if not LIVE_WS_STOP.is_set():

            delay = min(
                WS_RECONNECT_MAX,
                max(
                    1,
                    2 ** min(
                        attempt,
                        5,
                    ),
                ),
            )

            LIVE_WS_STOP.wait(
                delay
                + random.uniform(
                    0,
                    0.5,
                )
            )


# ============================================================
# START / STOP
# ============================================================

def start_live_ws():

    global LIVE_WS_THREAD

    if not USE_WEBSOCKET:
        return

    if (
        LIVE_WS_THREAD
        and LIVE_WS_THREAD.is_alive()
    ):
        return

    LIVE_WS_STOP.clear()

    LIVE_WS_THREAD = threading.Thread(
        target=_ws_worker,
        name="polymarket-rtds",
        daemon=True,
    )

    LIVE_WS_THREAD.start()


def stop_live_ws():

    LIVE_WS_STOP.set()

    try:

        if (
            LIVE_WS_THREAD
            and LIVE_WS_THREAD.is_alive()
        ):
            LIVE_WS_THREAD.join(
                timeout=2
            )

    except Exception:
        pass


# ============================================================
# DRAIN WEBSOCKET
# ============================================================

def drain_live_ws():

    rows = []

    while True:

        try:
            row = LIVE_WS_QUEUE.get_nowait()

        except queue.Empty:
            break

        if isinstance(
            row,
            dict,
        ):
            row = dict(row)
            row[
                "_feed_source"
            ] = "ws"

        rows.append(row)

    return rows


# ============================================================
# MERGE FEEDS
# ============================================================

def _merge_feed_rows(
    rows,
    state,
):
    """
    Merge WS + Activity + Trades.

    Important:
      1. Explicit execution IDs are exact.
      2. Economic fallback is time-limited.
      3. Previously processed executions are not emitted again.
      4. We do NOT deduplicate all identical fills forever.
    """

    current = now()

    _cleanup_seen(
        state,
        current,
    )

    ordered = sorted(
        rows,
        key=lambda x: (
            trade_ts(x),
            _execution_key(x),
        ),
    )

    merged = []

    cycle_exact = set()
    cycle_economic = {}

    duplicates = 0
    persistent_duplicates = 0

    for row in ordered:

        if not isinstance(
            row,
            dict,
        ):
            continue

        row = dict(row)

        key = _execution_key(row)
        econ = _economic_key(row)
        ts = trade_ts(row)

        # ----------------------------------------------------
        # Exact duplicate within this polling cycle.
        # ----------------------------------------------------

        if key in cycle_exact:

            duplicates += 1
            continue

        # ----------------------------------------------------
        # Economic duplicate within this cycle.
        #
        # Only use this fallback when there is no explicit
        # execution identifier.
        # ----------------------------------------------------

        explicit = _explicit_execution_id(
            row
        )

        if not explicit:

            duplicate_econ = False

            for old_ts in cycle_economic.get(
                econ,
                [],
            ):

                if (
                    abs(ts - old_ts)
                    <= ECONOMIC_MATCH_SECONDS
                ):
                    duplicate_econ = True
                    break

            if duplicate_econ:

                duplicates += 1
                continue

        # ----------------------------------------------------
        # Persistent duplicate from an earlier polling cycle.
        # ----------------------------------------------------

        if _already_seen(
            state,
            row,
            current,
        ):

            persistent_duplicates += 1
            duplicates += 1
            continue

        # ----------------------------------------------------
        # Accept.
        # ----------------------------------------------------

        cycle_exact.add(key)

        if not explicit:
            cycle_economic.setdefault(
                econ,
                [],
            ).append(ts)

        _mark_seen(
            state,
            row,
            current,
        )

        merged.append(row)

    state[
        "feed_duplicates_ignored"
    ] = (
        state.get(
            "feed_duplicates_ignored",
            0,
        )
        + duplicates
    )

    state[
        "feed_persistent_duplicates"
    ] = (
        state.get(
            "feed_persistent_duplicates",
            0,
        )
        + persistent_duplicates
    )

    return (
        merged,
        {
            "duplicates_ignored": duplicates,
            "persistent_duplicates": (
                persistent_duplicates
            ),
            "accepted": len(merged),
        },
    )


# ============================================================
# FAST FEED
# ============================================================

def fetch_recent_trades_fast(state):

    now_ts = int(now())

    cutoff = (
        now_ts
        - RECENT_SECONDS
    )

    overlap = max(
        10,
        int(
            os.getenv(
                "FEED_TIMESTAMP_OVERLAP",
                "20",
            )
        ),
    )

    cursor_ts = num(
        state.get(
            "cursor_ts"
        ),
        0,
    )

    local_cutoff = max(
        0,
        cutoff,
    )

    if cursor_ts > 0:

        local_cutoff = max(
            0,
            min(
                local_cutoff,
                int(
                    cursor_ts
                )
                - overlap,
            ),
        )

    # --------------------------------------------------------
    # WS
    # --------------------------------------------------------

    ws_rows = []

    # When the priority worker is enabled, it consumes the live WS queue
    # immediately. The polling loop must not drain that queue a second time.
    if not WS_PRIORITY_COPY:
        for row in drain_live_ws():
            if trade_ts(row) >= local_cutoff:
                ws_rows.append(row)

    state[
        "ws_received"
    ] = LIVE_WS_STATUS[
        "received"
    ]

    state[
        "ws_reconnects"
    ] = LIVE_WS_STATUS[
        "reconnects"
    ]

    state[
        "ws_last_message"
    ] = LIVE_WS_STATUS[
        "last_message"
    ]

    # --------------------------------------------------------
    # REST
    # --------------------------------------------------------

    activity_rows = []
    trade_rows = []

    raw_activity_rows = 0
    raw_trade_rows = 0

    errors = []
    sources = []

    # --------------------------------------------------------
    # Activity API
    # --------------------------------------------------------

    try:

        data = get_json(
            ACTIVITY_URL,
            {
                "user": WALLET,
                "start": max(
                    0,
                    cutoff,
                ),
                "end": now_ts,
                "limit": min(
                    500,
                    RECENT_LIMIT,
                ),
                "type": "TRADE",
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            },
        )

        if not isinstance(
            data,
            list,
        ):
            raise RuntimeError(
                "Activity API returned non-list response"
            )

        raw_activity_rows = len(
            data
        )

        for row in normalize_feed_rows(
            data
        ):

            if (
                trade_ts(row)
                >= local_cutoff
            ):

                row = dict(row)

                row[
                    "_feed_source"
                ] = "activity"

                activity_rows.append(
                    row
                )

        sources.append(
            "activity"
        )

        state[
            "api_requests"
        ] = (
            state.get(
                "api_requests",
                0,
            )
            + 1
        )

    except Exception as e:

        errors.append(
            f"activity:"
            f"{type(e).__name__}: "
            f"{e}"
        )

        state[
            "api_errors"
        ] = (
            state.get(
                "api_errors",
                0,
            )
            + 1
        )

        state.setdefault("api_error_details", []).append(errors[-1])
        state["api_error_details"] = state["api_error_details"][-20:]
        state["api_last_error"] = errors[-1]

    # --------------------------------------------------------
    # Trades API
    # --------------------------------------------------------

    try:

        data = get_json(
            TRADES_URL,
            {
                "user": WALLET,
                "limit": min(
                    10000,
                    TRADE_PAGE_LIMIT,
                ),
                "offset": 0,
                "takerOnly": False,
            },
        )

        if not isinstance(
            data,
            list,
        ):
            raise RuntimeError(
                "Trades API returned non-list response"
            )

        raw_trade_rows = len(
            data
        )

        for row in normalize_feed_rows(
            data
        ):

            if (
                trade_ts(row)
                >= local_cutoff
            ):

                row = dict(row)

                row[
                    "_feed_source"
                ] = "trades"

                trade_rows.append(
                    row
                )

        sources.append(
            "trades"
        )

        state[
            "api_requests"
        ] = (
            state.get(
                "api_requests",
                0,
            )
            + 1
        )

    except Exception as e:

        errors.append(
            f"trades:"
            f"{type(e).__name__}: "
            f"{e}"
        )

        state[
            "api_errors"
        ] = (
            state.get(
                "api_errors",
                0,
            )
            + 1
        )

        state.setdefault("api_error_details", []).append(errors[-1])
        state["api_error_details"] = state["api_error_details"][-20:]
        state["api_last_error"] = errors[-1]

    # --------------------------------------------------------
    # DEDICATED SELL RECOVERY
    #
    # Polymarket's documented Data API supports side=SELL. Keep this as an
    # independent recovery path because a SELL can be absent from one live
    # feed while still being available from the user-scoped trades endpoint.
    # --------------------------------------------------------
    sell_rows = []
    last_sell_fetch = num(state.get("last_sell_recovery_fetch"), 0.0)
    if now() - last_sell_fetch >= SELL_POLL_EVERY:
        try:
            data = get_json(
                TRADES_URL,
                {
                    "user": WALLET,
                    "side": "SELL",
                    "limit": min(10000, TRADE_PAGE_LIMIT),
                    "offset": 0,
                    "takerOnly": False,
                },
            )
            if not isinstance(data, list):
                raise RuntimeError("SELL trades API returned non-list response")
            for row in normalize_feed_rows(data):
                # Some endpoint versions may ignore side=SELL. Filter again
                # locally so the recovery path can never turn a BUY into a
                # SELL candidate.
                if trade_side(row) != "SELL":
                    continue
                if trade_ts(row) >= local_cutoff:
                    row = dict(row)
                    row["_feed_source"] = "trades_sell"
                    sell_rows.append(row)
            state["api_requests"] = state.get("api_requests", 0) + 1
            state["last_sell_recovery_fetch"] = now()
            sources.append("trades_sell")
        except Exception as e:
            errors.append(f"trades_sell:{type(e).__name__}: {e}")
            state["api_errors"] = state.get("api_errors", 0) + 1
            state["last_sell_recovery_fetch"] = now()

    # --------------------------------------------------------
    # MERGE
    # --------------------------------------------------------

    combined = (
        ws_rows
        + activity_rows
        + trade_rows
        + sell_rows
    )

    merged, merge_diag = (
        _merge_feed_rows(
            combined,
            state,
        )
    )

    newest = max(
        (
            trade_ts(r)
            for r in merged
            if trade_ts(r) > 0
        ),
        default=0,
    )

    # --------------------------------------------------------
    # API health
    # --------------------------------------------------------

    if sources or ws_rows:

        state[
            "api_consecutive_failures"
        ] = 0

        state[
            "api_last_ok"
        ] = now()

        state[
            "api_last_error"
        ] = ""

    else:

        state[
            "api_consecutive_failures"
        ] = (
            state.get(
                "api_consecutive_failures",
                0,
            )
            + 1
        )

        state[
            "api_last_error"
        ] = "; ".join(
            errors
        )

    # --------------------------------------------------------
    # Source label
    # --------------------------------------------------------

    if (
        ws_rows
        and activity_rows
        and trade_rows
    ):
        source = (
            "ws+activity+trades"
        )

    elif (
        ws_rows
        and trade_rows
    ):
        source = "ws+trades"

    elif (
        ws_rows
        and activity_rows
    ):
        source = "ws+activity"

    elif ws_rows:
        source = "ws"

    elif (
        activity_rows
        and trade_rows
    ):
        source = "activity+trades"

    elif activity_rows:
        source = "activity"

    elif trade_rows:
        source = "trades"

    else:
        source = "none"

    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------

    return (
        merged,
        {
            "ok": bool(
                sources
                or ws_rows
            ),
            "source": source,

            "ws_rows": len(
                ws_rows
            ),

            "raw_rows": raw_trade_rows,

            "rows": len(
                trade_rows
            ),

            "activity_raw_rows":
                raw_activity_rows,

            "activity_rows":
                len(activity_rows),

            "sell_rows":
                len(sell_rows),

            "merged_rows":
                len(merged),

            "duplicates_ignored":
                merge_diag[
                    "duplicates_ignored"
                ],

            "persistent_duplicates":
                merge_diag[
                    "persistent_duplicates"
                ],

            "accepted":
                merge_diag[
                    "accepted"
                ],

            "newest_ts":
                newest,

            "feed_age":
                (
                    now() - newest
                    if newest
                    else None
                ),

            "cutoff":
                local_cutoff,

            "end":
                now_ts,

            "errors":
                errors,
        },
    )


# ============================================================
# ACTIVITY VERIFICATION
# ============================================================

def fetch_activity_verify():

    end_ts = int(
        now()
    )

    start_ts = max(
        0,
        end_ts - RECENT_SECONDS,
    )

    params = {
        "user": WALLET,
        "start": start_ts,
        "end": end_ts,
        "limit": min(
            500,
            RECENT_LIMIT,
        ),
        "type": "TRADE",
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }

    try:

        data = get_json(
            ACTIVITY_URL,
            params,
        )

        if not isinstance(
            data,
            list,
        ):
            raise RuntimeError(
                "Activity API returned non-list response"
            )

        rows = []

        for row in normalize_feed_rows(
            data
        ):

            row = dict(row)

            row[
                "_feed_source"
            ] = "activity"

            rows.append(row)

        return (
            rows,
            {
                "ok": True,
                "source": "activity",
                "params": params,
                "raw_rows": len(
                    data
                ),
            },
        )

    except Exception as e:

        return (
            [],
            {
                "ok": False,
                "source": "activity",
                "params": params,
                "error": (
                    f"{type(e).__name__}: "
                    f"{e}"
                ),
            },
        )


# ============================================================
# FEED DIAGNOSTICS
# ============================================================

def build_feed_diag(
    trades,
    activity,
    trade_diag,
    activity_diag,
    previous_newest,
):

    timestamps = [
        trade_ts(t)
        for t in trades
        if trade_ts(t) > 0
    ]

    newest = max(
        timestamps,
        default=previous_newest,
    )

    age = (
        max(
            0,
            now() - newest,
        )
        if newest
        else None
    )

    if (
        trade_diag.get("ok")
        and age is not None
        and age <= STALE_AFTER
    ):

        status = "LIVE"

    elif (
        trade_diag.get("ok")
        and age is not None
        and age <= HARD_STALE_AFTER
    ):

        status = "STALE"

    elif trade_diag.get("ok"):

        status = (
            "API_CONNECTED_NO_RECENT_EXECUTIONS"
        )

    else:

        status = "API_ERROR"

    return {
        "current_time":
            now(),

        "current_time_ist":
            ist(),

        "window_seconds":
            RECENT_SECONDS,

        "trades_executions":
            len(trades),

        "activity_executions":
            len(activity),

        "merged_executions":
            len(trades),

        "newest_seen":
            newest,

        "newest_seen_ist":
            (
                ist(newest)
                if newest
                else None
            ),

        "newest_age_seconds":
            age,

        "status":
            status,

        "trades_endpoint":
            trade_diag,

        "activity_endpoint":
            activity_diag,

        "previous_newest":
            previous_newest,
    }
