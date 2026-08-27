# Polymarket Copy Simulator — Low-Latency Build

Paper-trading only. No private key or live order submission is included.

## What changed

- Live RTDS events are consumed by a dedicated priority worker instead of waiting for the 1-second polling loop.
- Polymarket CLOB market WebSocket maintains local L2 books; REST `/book` is retained as a fallback.
- CLOB book subscriptions are added dynamically as trader assets appear and are replayed after reconnect.
- RTDS watchdog/reconnect remains enabled.
- Optional Struct shadow feed can race RTDS without ever triggering copies.
- Latency reporting now measures actual trader-event-to-copy latency, while queue/processing latency is reported separately.
- Added offline tests for book snapshots/deltas, REST fallback, feed racing, configuration, and priority processing.

## Railway variables

Required:

- `POLYMARKET_WALLET`

Defaults already suitable for this build:

- `COPY_NOTIONAL_FRACTION=0.10`
- `WS_PRIORITY_COPY=true`
- `BOOK_MAX_AGE_SECONDS=5`
- `BOOK_STALE_AFTER_SECONDS=30`
- `BOOK_MAX_ASSETS=1000`
- `WS_DATA_STALE_AFTER=60`

Optional Struct shadow comparison:

- `STRUCT_API_KEY=<your Struct key>`
- `STRUCT_SHADOW_ENABLED=true`

Struct is shadow-only in this build. It cannot trigger copies.

## Tests

```bash
python3 -m unittest discover -s tests -v
python3 smoke_test.py
```
