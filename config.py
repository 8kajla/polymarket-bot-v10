from __future__ import annotations
import os, queue, threading
from pathlib import Path
import requests
from zoneinfo import ZoneInfo

WALLET = os.getenv("POLYMARKET_WALLET", "").strip().lower()
MAX_OPEN_CAPITAL = float(os.getenv("MAX_OPEN_CAPITAL", "300"))
POLL_SECONDS = max(1.0, float(os.getenv("POLL_SECONDS", "1")))
TRADE_PAGE_LIMIT = max(25, min(10000, int(os.getenv("TRADE_PAGE_LIMIT", "500"))))
FAST_MODE = os.getenv("FAST_MODE", "true").lower() == "true"
FAST_TIMEOUT = float(os.getenv("FAST_TIMEOUT", "4"))
NORMAL_TIMEOUT = float(os.getenv("NORMAL_TIMEOUT", "8"))
RECON_EVERY = max(0, int(os.getenv("RECON_EVERY", "30")))
ACTIVITY_EVERY = max(5, int(os.getenv("ACTIVITY_EVERY", "15")))
CLOSED_EVERY = max(5, int(os.getenv("CLOSED_EVERY", "15")))
RESOLUTION_EVERY = max(2, int(os.getenv("RESOLUTION_EVERY", "5")))
RESOLUTION_WAIT_LOG_EVERY = max(10, float(os.getenv("RESOLUTION_WAIT_LOG_EVERY", "30")))
SELL_POLL_EVERY = max(1, float(os.getenv("SELL_POLL_EVERY", "2")))
SELL_REPLAY_SECONDS = max(60, int(os.getenv("SELL_REPLAY_SECONDS", "900")))
EXIT_GRACE_SECONDS = max(0, int(os.getenv("EXIT_GRACE_SECONDS", "20")))
REPORT_EVERY = max(1, int(os.getenv("REPORT_EVERY", "5")))
STATUS_EVERY = max(10, int(os.getenv("STATUS_EVERY", "60")))
STALE_AFTER = float(os.getenv("STALE_AFTER", "5"))
HARD_STALE_AFTER = float(os.getenv("HARD_STALE_AFTER", "30"))
WIDE_GAP_PCT = float(os.getenv("WIDE_GAP_PCT", "100"))
MICRO_NOTIONAL = float(os.getenv("MICRO_NOTIONAL", "0.25"))
COPY_NOTIONAL_FRACTION = min(1.0, max(0.0, float(os.getenv("COPY_NOTIONAL_FRACTION", "0.10"))))

# RTDS watchdog:
# - JSON PING keeps the RTDS connection alive.
# - DATA_STALE forces a reconnect when the socket remains open but stops
#   delivering application messages. This specifically addresses the
#   "WS CONNECTED | msg N seconds ago" failure mode.
WS_HEARTBEAT_SECONDS = max(
    2.0, float(os.getenv("WS_HEARTBEAT_SECONDS", "5"))
)
WS_DATA_STALE_AFTER = max(
    15.0, float(os.getenv("WS_DATA_STALE_AFTER", "60"))
)
WS_PRIORITY_COPY = os.getenv("WS_PRIORITY_COPY", "true").lower() == "true"
BOOK_MAX_AGE_SECONDS = max(0.25, float(os.getenv("BOOK_MAX_AGE_SECONDS", "5")))
BOOK_STALE_AFTER_SECONDS = max(15.0, float(os.getenv("BOOK_STALE_AFTER_SECONDS", "30")))
BOOK_MAX_ASSETS = max(10, int(os.getenv("BOOK_MAX_ASSETS", "1000")))
BOOK_LOOKUP_GRACE_MS = max(0.0, float(os.getenv("BOOK_LOOKUP_GRACE_MS", "75")))
STRUCT_API_KEY = os.getenv("STRUCT_API_KEY", "").strip()
STRUCT_SHADOW_ENABLED = bool(STRUCT_API_KEY) and os.getenv("STRUCT_SHADOW_ENABLED", "true").lower() == "true"

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

ACTIVITY_URL = "https://data-api.polymarket.com/activity"
TRADES_URL = "https://data-api.polymarket.com/trades"
POSITIONS_URL = "https://data-api.polymarket.com/positions"
CLOSED_POSITIONS_URL = "https://data-api.polymarket.com/closed-positions"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
CLOB_MARKET_URL = "https://clob.polymarket.com/markets/{condition_id}"
LIVE_WS_URL = "wss://ws-live-data.polymarket.com"

USE_WEBSOCKET = os.getenv("USE_WEBSOCKET", "true").lower() == "true"
WS_RECONNECT_MAX = max(5, int(os.getenv("WS_RECONNECT_MAX", "30")))
WS_QUEUE_MAX = max(100, int(os.getenv("WS_QUEUE_MAX", "5000")))
RECENT_SECONDS = max(60, int(os.getenv("RECENT_SECONDS", "900")))
RECENT_LIMIT = max(100, int(os.getenv("RECENT_LIMIT", "500")))
POSITION_PAGES = max(1, int(os.getenv("POSITION_PAGES", "10")))
POSITION_PAGE_SIZE = max(100, int(os.getenv("POSITION_PAGE_SIZE", "500")))

IST = ZoneInfo("Asia/Kolkata")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "polymarket-copy-simulator-v7/1.0"})

FILES = {
    "summary": DATA_DIR / "summary_v7.json",
    "trader_positions": DATA_DIR / "trader_positions_v7.json",
    "our_positions": DATA_DIR / "our_positions_v7.json",
    "closed_trades": DATA_DIR / "closed_trades_v7.json",
    "fills": DATA_DIR / "fills_v7.json",
    "reconciliation": DATA_DIR / "reconciliation_v7.json",
    "state": DATA_DIR / "state_v7.json",
    "audit": DATA_DIR / "copytrade_v7_events.sqlite3",
}
