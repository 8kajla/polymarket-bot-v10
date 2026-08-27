#!/usr/bin/env python3
"""Offline V7 smoke test. No network calls and no trades are placed."""
import os
os.environ.setdefault("POLYMARKET_WALLET", "0x0000000000000000000000000000000000000001")
os.environ.setdefault("DATA_DIR", "/app/data")

from trading import ledger
from config import COPY_NOTIONAL_FRACTION
assert abs(COPY_NOTIONAL_FRACTION - 0.10) < 1e-12

ledger.fetch_book = lambda asset: {
    "asks": [{"price": "0.50", "size": "100"}],
    "bids": [{"price": "0.60", "size": "100"}],
}

state = ledger.new_state()
assert ledger.open_capital(state) == 0

buy = {"id":"smoke-buy","timestamp":1000,"side":"BUY","size":10,"price":0.50,
       "asset":"ASSET","conditionId":"COND","outcome":"YES","title":"Smoke"}
sell = {"id":"smoke-sell","timestamp":1010,"side":"SELL","size":4,"price":0.60,
        "asset":"ASSET","conditionId":"COND","outcome":"YES","title":"Smoke"}

assert ledger.process_trade(state, buy, 1000.5) is True
assert state["copied_buys"] == 1
assert abs(ledger.open_capital(state) - 0.5) < 1e-9

assert ledger.process_trade(state, sell, 1010.5) is True
assert state["copied_sells"] == 1
assert state["sell_detected"] == 1
assert state["sell_processed"] == 1
assert state["sell_rejected_no_position"] == 0
assert abs(state["our_realized_pnl"] - 0.04) < 1e-9
assert abs(ledger.open_capital(state) - 0.3) < 1e-9
assert abs(state["trader_realized_pnl"] - 0.4) < 1e-9

print("V7 SMOKE TEST: PASS")
print("  Copy size: 10% of trader notional")
print("  BUY path: PASS")
print("  SELL detection: PASS")
print("  SELL execution: PASS")
print("  Proportional SELL math: PASS")
print("  Local-position fallback: PASS")

# Resolution test: official CLOB winner settles the paper position at
# $1/share without relying on a live market price.
import resolution
resolution.now = lambda: 1000.0
resolution._market_query = lambda pos: {
    "closed": False,
    "outcomes": "[\"Up\",\"Down\"]",
    "outcomePrices": "[\"0.40\",\"0.60\"]",
    "clobTokenIds": "[\"ASSET\",\"OTHER\"]",
}
resolution._clob_market_query = lambda condition: {
    "closed": True,
    "tokens": [
        {"token_id": "ASSET", "outcome": "Up", "winner": True},
        {"token_id": "OTHER", "outcome": "Down", "winner": False},
    ],
}
resolution.fetch_redeemable_positions = lambda: []
resolution.fetch_closed_positions = lambda: []

rs = ledger.new_state()
rs["trader_positions"]["tk"] = {
    "position_id": "TRADER:tk", "owner": "TRADER", "key": "tk",
    "asset": "ASSET", "condition_id": "COND", "outcome": "Up",
    "market": "Smoke 5m", "slug": "smoke-5m-700",
    "duration": "5m", "duration_seconds": 300,
    "market_end_ts": 500, "shares": 10.0, "total_cost": 4.0,
    "average_entry": 0.4, "realized_pnl": 0.0,
    "first_buy_timestamp": 1.0, "last_activity_timestamp": 1.0,
    "status": "OPEN", "exit_reason": None,
}
rs["our_positions"]["k"] = {
    "position_id": "OUR:k", "owner": "OUR", "key": "k",
    "asset": "ASSET", "condition_id": "COND", "outcome": "Up",
    "market": "Smoke 5m", "slug": "smoke-5m-700",
    "duration": "5m", "duration_seconds": 300,
    "market_end_ts": 500, "shares": 10.0, "total_cost": 4.0,
    "average_entry": 0.4, "realized_pnl": 0.0,
    "first_buy_timestamp": 1.0, "last_activity_timestamp": 1.0,
    "status": "OPEN", "exit_reason": None,
}
assert resolution.resolve_cycle(rs) == 2
assert rs["our_positions"]["k"]["status"] == "SETTLED"
assert rs["settlement_wins"] == 1
assert abs(rs["our_realized_pnl"] - 6.0) < 1e-9
assert abs(rs["trader_realized_pnl"] - 6.0) < 1e-9
assert rs["trader_settlement_wins"] == 1
print("  CLOB winner resolution: PASS")
print("  5-minute settlement path: PASS")
print("  Trader resolution P&L: PASS")

# Regression test: failed SELL must update the trader ledger exactly once,
# while OUR execution remains pending and can be retried later.
fs = ledger.new_state()
book_no_bid = {"asks": [{"price": "0.50", "size": "100"}], "bids": []}
book_with_bid = {"asks": [{"price": "0.50", "size": "100"}],
                 "bids": [{"price": "0.60", "size": "100"}]}

ledger.fetch_book = lambda asset: book_no_bid
buy2 = {"id":"retry-buy","timestamp":2000,"side":"BUY","size":10,"price":0.50,
        "asset":"RETRY_ASSET","conditionId":"RETRY_COND","outcome":"YES","title":"Retry"}
sell2 = {"id":"retry-sell","timestamp":2010,"side":"SELL","size":4,"price":0.60,
         "asset":"RETRY_ASSET","conditionId":"RETRY_COND","outcome":"YES","title":"Retry"}

assert ledger.process_trade(fs, buy2, 2000.5) is True
trader_before = fs["trader_positions"][ledger.trade_key(buy2)]["shares"]
assert trader_before == 10

assert ledger.process_trade(fs, sell2, 2010.5) is False
trader_after_failed = fs["trader_positions"][ledger.trade_key(sell2)]["shares"]
assert abs(trader_after_failed - 6.0) < 1e-9
assert len(fs["pending_sells"]) == 1

ledger.fetch_book = lambda asset: book_with_bid
assert ledger.retry_pending_sells(fs, 2011.0) == 1
assert abs(fs["trader_positions"][ledger.trade_key(sell2)]["shares"] - 6.0) < 1e-9
assert len(fs["pending_sells"]) == 0
assert fs["copied_sells"] == 1

print("  Failed SELL trader-ledger integrity: PASS")
print("  Pending SELL retry: PASS")

# Regression test: unresolved expiry must remain OPEN, never silently become
# a $0 LOSS.
resolution.now = lambda: 2000.0
resolution._clob_market_query = lambda condition: {"closed": False, "tokens": []}
resolution._market_query = lambda pos: {
    "closed": False,
    "outcomes": '[\"Up\",\"Down\"]',
    "outcomePrices": '[\"0.50\",\"0.50\"]',
    "clobTokenIds": '[\"UNRES_ASSET\",\"OTHER\"]',
}
us = ledger.new_state()
us["our_positions"]["u"] = {
    "position_id": "OUR:u", "owner": "OUR", "key": "u",
    "asset": "UNRES_ASSET", "condition_id": "UNRES_COND", "outcome": "Up",
    "market": "Unresolved 5m", "slug": "unresolved-5m",
    "duration": "5m", "duration_seconds": 300,
    "market_end_ts": 1000, "shares": 10.0, "total_cost": 4.0,
    "average_entry": 0.4, "realized_pnl": 0.0,
    "first_buy_timestamp": 1.0, "last_activity_timestamp": 1.0,
    "status": "OPEN", "exit_reason": None,
}
assert resolution.resolve_cycle(us) == 0
assert us["our_positions"]["u"]["status"] == "OPEN"
assert us["settlement_losses"] == 0
assert us["settled_positions"] == 0
assert abs(us["our_realized_pnl"]) < 1e-9

print("  Unresolved market stays OPEN: PASS")

# New watchdog configuration sanity check.
from config import WS_HEARTBEAT_SECONDS, WS_DATA_STALE_AFTER
assert WS_HEARTBEAT_SECONDS >= 2
assert WS_DATA_STALE_AFTER >= 15
print("  WS heartbeat/watchdog config: PASS")
