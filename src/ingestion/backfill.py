"""Backfill historical klines from Binance Futures REST.

Public endpoint — no auth needed. Inserts into the same `klines` table
that the live WS writer uses, with INSERT OR REPLACE so re-running is
idempotent and live data overwrites backfilled rows once they overlap.

Usage:
    python -m src.ingestion.backfill                # 7 days, all intervals
    python -m src.ingestion.backfill --days 30      # 30 days
    python -m src.ingestion.backfill --interval 1h  # only one interval
"""

import argparse
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import orjson

from src import config

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
}
REQUEST_LIMIT = 1000   # Binance hard cap for /api/v1/klines
REQUEST_PAUSE_S = 0.1  # be nice to the API; well under the rate limit


def _fetch(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": REQUEST_LIMIT,
    }
    url = f"{config.REST_KLINES_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "btc-scalping/0.1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return orjson.loads(resp.read())


def _connect(db_path: Path, schema_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(schema_path.read_text())
    return conn


def backfill_interval(
    conn: sqlite3.Connection, symbol: str, interval: str, days: int
) -> int:
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 24 * 60 * 60 * 1000
    step_ms = INTERVAL_MS[interval] * REQUEST_LIMIT

    total = 0
    cursor = start_ms
    while cursor < now_ms:
        batch_end = min(cursor + step_ms, now_ms)
        klines = _fetch(symbol, interval, cursor, batch_end)
        if not klines:
            break

        # Binance returns each kline as a positional array:
        # [openTime, open, high, low, close, volume, closeTime,
        #  quoteVolume, tradesCount, takerBuyBase, takerBuyQuote, ignore]
        rows = [
            (
                interval,
                k[0], k[6],
                float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                float(k[5]), float(k[7]), int(k[8]),
                float(k[9]), float(k[10]),
                k[6],                              # event_time = close_time
                int(time.time() * 1000),           # recv_time
            )
            for k in klines
        ]
        conn.execute("BEGIN")
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO klines "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        total += len(rows)
        # Advance past the last open_time we got, plus one interval, to
        # avoid re-pulling the same final row.
        cursor = klines[-1][0] + INTERVAL_MS[interval]
        time.sleep(REQUEST_PAUSE_S)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7,
                        help="how many days back to fetch (default: 7)")
    parser.add_argument("--symbol", default=config.SYMBOL,
                        help=f"symbol (default: {config.SYMBOL})")
    parser.add_argument("--interval", choices=list(INTERVAL_MS),
                        help="only backfill this interval (default: all)")
    args = parser.parse_args()

    intervals = [args.interval] if args.interval else config.INTERVALS

    conn = _connect(config.DB_PATH, config.SCHEMA_PATH)
    try:
        for interval in intervals:
            t0 = time.monotonic()
            print(f"  {interval}: ", end="", flush=True)
            n = backfill_interval(conn, args.symbol, interval, args.days)
            dt = time.monotonic() - t0
            print(f"{n} klines in {dt:.1f}s")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
