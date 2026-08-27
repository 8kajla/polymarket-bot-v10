from __future__ import annotations
import sqlite3, json
from config import FILES
from utils import ist, now, trade_id, trade_side, trade_key

def init_audit_db():
    con=sqlite3.connect(FILES["audit"])
    con.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_ts REAL NOT NULL,
        created_ist TEXT NOT NULL,
        trade_id TEXT,
        side TEXT,
        trade_key TEXT,
        source TEXT,
        action TEXT,
        reason TEXT,
        payload TEXT
    )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_events_trade_id ON events(trade_id)")
    con.commit(); con.close()

def audit_event(t, source, action, reason="", extra=None):
    con=sqlite3.connect(FILES["audit"])
    con.execute("INSERT INTO events(created_ts,created_ist,trade_id,side,trade_key,source,action,reason,payload) VALUES(?,?,?,?,?,?,?,?,?)",
                (now(),ist(),trade_id(t),trade_side(t),trade_key(t),source,action,reason,json.dumps(extra or {},default=str)))
    con.commit(); con.close()
