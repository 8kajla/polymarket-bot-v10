from __future__ import annotations
import json, re, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from config import *

def now():
    return time.time()

def ist(ts=None):
    ts = now() if ts is None else float(ts)
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(IST).strftime(
        "%Y-%m-%d %I:%M:%S %p IST"
    )

def num(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def money(v):
    return round(num(v), 8)

def save(path, obj):
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(
        json.dumps(obj, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    tmp.replace(path)

def load(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def get_json(url, params=None, timeout=None):
    if timeout is None:
        timeout = FAST_TIMEOUT if FAST_MODE else NORMAL_TIMEOUT
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def first(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            return d[k]
    return default


# ============================================================
# TRADE NORMALIZATION
# ============================================================

def trade_ts(t):
    value = first(t, "timestamp", "createdAt", "created_at", "time")
    # Normalize seconds, milliseconds, numeric strings, and ISO timestamps.
    return parse_timestamp(value)

def trade_side(t):
    """Return an explicit BUY/SELL side from common feed shapes."""
    if not isinstance(t, dict):
        return ""

    keys = (
        "side", "action", "direction", "orderSide", "order_side",
        "tradeSide", "trade_side", "executionSide", "execution_side",
        "type", "eventType", "event_type", "tradeType", "trade_type",
    )

    nested = []
    for name in ("trade", "execution", "order", "matchedOrder", "matched_order", "details"):
        value = t.get(name)
        if isinstance(value, dict):
            nested.append(value)

    for obj in [t, *nested]:
        for key in keys:
            value = obj.get(key)
            if value is None:
                continue
            side = str(value).strip().upper()
            if side in ("BUY", "SELL"):
                return side

    return ""

def trade_size(t):
    return num(first(t, "size", "shares", "quantity", "amount"))

def trade_price(t):
    return num(first(t, "price", "avgPrice", "averagePrice"))

def trade_asset(t):
    return str(first(t, "asset", "assetId", "tokenId", "token_id", default=""))

def trade_condition(t):
    return str(first(t, "conditionId", "condition_id", default=""))

def trade_outcome(t):
    return first(t, "outcome", "outcomeIndex", "outcome_index", default="")

def trade_key(t):
    asset = trade_asset(t)
    condition = trade_condition(t)
    outcome = str(trade_outcome(t))
    return f"{asset}|{condition}|{outcome}"

def canonical_trade_id(t):
    """
    Canonical execution fingerprint.

    We prefer transaction/fill identifiers when present, but we always
    include the economic execution fields. This prevents the same fill
    appearing from Activity and Trades with different wrapper IDs from
    being copied twice.
    """
    tx = first(
        t,
        "transactionHash",
        "transaction_hash",
        "txHash",
        "hash",
        default="",
    )
    explicit = first(t, "id", "tradeId", "trade_id", default="")

    # Normalize timestamp to milliseconds; feeds can differ slightly in
    # timestamp precision.
    ts_ms = int(trade_ts(t) * 1000)

    return "|".join([
        str(tx).lower(),
        str(explicit),
        trade_asset(t),
        trade_condition(t),
        str(trade_outcome(t)),
        trade_side(t),
        f"{trade_size(t):.8f}",
        f"{trade_price(t):.8f}",
        str(ts_ms),
    ])

def economic_trade_fingerprint(t):
    """
    Fingerprint used to catch cross-feed duplicates even when Activity and
    Trades expose different IDs. A small time bucket tolerates sub-second
    representation differences.
    """
    ts_bucket = int(trade_ts(t) * 2)  # 500 ms buckets
    return "|".join([
        trade_asset(t),
        trade_condition(t),
        str(trade_outcome(t)),
        trade_side(t),
        f"{trade_size(t):.8f}",
        f"{trade_price(t):.8f}",
        str(ts_bucket),
    ])

def trade_id(t):
    return canonical_trade_id(t)

def market_name(t):
    return str(first(
        t,
        "title",
        "market",
        "marketTitle",
        "slug",
        "eventSlug",
        default="Unknown market",
    ))

def market_slug(t):
    return str(first(t, "slug", "eventSlug", "marketSlug", default=""))

def market_duration(t):
    text = (market_slug(t) + " " + market_name(t)).lower()

    for pattern, label, seconds in (
        (r"15m", "15m", 900),
        (r"10m", "10m", 600),
        (r"5m", "5m", 300),
        (r"15[- ]minute", "15m", 900),
        (r"10[- ]minute", "10m", 600),
        (r"5[- ]minute", "5m", 300),
    ):
        if re.search(pattern, text):
            return label, seconds

    return "unknown", None

def market_end(t):
    label, seconds = market_duration(t)
    if seconds is None:
        return None

    # 1) Prefer an explicit Unix timestamp in the slug.
    numbers = re.findall(r"(?<!\d)(\d{9,13})(?!\d)", market_slug(t))
    if numbers:
        try:
            start_ts = int(numbers[-1])
            if start_ts > 10_000_000_000:
                start_ts //= 1000
            return start_ts + seconds
        except Exception:
            pass

    # 2) Parse titles such as:
    #    "Bitcoin Up or Down - August 25, 6:00PM-6:05PM ET"
    title = market_name(t)
    m = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),\s*"
        r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*"
        r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
        title,
        re.IGNORECASE,
    )
    if not m:
        return None

    month_name, day, sh, sm, sap, eh, em, eap = m.groups()
    try:
        year = datetime.now(ZoneInfo("America/New_York")).year
        month = datetime.strptime(month_name, "%B").month
        eastern = ZoneInfo("America/New_York")

        end_dt = datetime(
            year,
            month,
            int(day),
            int(eh) % 12 + (12 if eap.upper() == "PM" else 0),
            int(em),
            tzinfo=eastern,
        )
        return end_dt.timestamp()
    except Exception:
        return None


# ============================================================
# LIVE FEED
# ============================================================

def parse_timestamp(value):
    """Best-effort timestamp parser used only by diagnostics/filtering."""
    if value is None:
        return 0.0

    if isinstance(value, (int, float)):
        v = float(value)
        # Treat millisecond Unix timestamps correctly.
        if v > 10_000_000_000:
            v /= 1000.0
        return v

    text = str(value).strip()
    if not text:
        return 0.0

    try:
        v = float(text)
        if v > 10_000_000_000:
            v /= 1000.0
        return v
    except (TypeError, ValueError):
        pass

    # ISO-8601 support, including trailing Z.
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError, OverflowError):
        return 0.0

def normalize_feed_rows(rows):
    """
    Keep only real BUY/SELL-looking execution records and remove duplicates
    inside one API response.
    """
    out = []
    seen_ids = set()
    seen_economic = set()

    for raw in rows:
        if not isinstance(raw, dict):
            continue

        side = trade_side(raw)
        ts = trade_ts(raw)
        size = trade_size(raw)
        price = trade_price(raw)

        if side not in ("BUY", "SELL"):
            continue
        if ts <= 0 or size <= 0 or price <= 0:
            continue

        k = trade_id(raw)
        econ = economic_trade_fingerprint(raw)

        if k in seen_ids or econ in seen_economic:
            continue

        seen_ids.add(k)
        seen_economic.add(econ)
        out.append(raw)

    out.sort(key=lambda x: (trade_ts(x), trade_id(x)))
    return out
