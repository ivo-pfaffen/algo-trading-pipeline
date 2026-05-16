import asyncio
import logging
import sqlite3
import time
from pathlib import Path

from src import config
from src.models import Kline

log = logging.getLogger(__name__)


class Storage:
    """Batched SQLite writer for klines.

    The websocket reader pushes Klines onto an asyncio.Queue. A worker
    drains that queue into time/size-bounded batches and flushes them
    to SQLite from a worker thread so the event loop is never blocked
    on disk I/O.

    With only closed klines the throughput is tiny (≈1 row per interval
    per minute), but the batched-writer pattern stays useful: it keeps
    fsyncs grouped and makes shutdown clean.
    """

    def __init__(self, db_path: Path, schema_path: Path) -> None:
        self.db_path = db_path
        self.schema_path = schema_path
        self.queue: asyncio.Queue[Kline] = asyncio.Queue(maxsize=config.QUEUE_MAXSIZE)
        self._conn: sqlite3.Connection | None = None
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._klines_written = 0

    async def start(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await asyncio.to_thread(self._connect)
        self._task = asyncio.create_task(self._run(), name="storage-writer")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)

    async def put(self, event: Kline) -> None:
        await self.queue.put(event)

    def stats(self) -> dict[str, int]:
        return {
            "queue_depth": self.queue.qsize(),
            "klines_written": self._klines_written,
        }

    # ---------- internals ----------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")  # 256 MiB
        conn.executescript(self.schema_path.read_text())
        return conn

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                batch = await self._drain_batch()
                if batch:
                    await asyncio.to_thread(self._flush, batch)
            tail: list[Kline] = []
            while not self.queue.empty():
                tail.append(self.queue.get_nowait())
            if tail:
                await asyncio.to_thread(self._flush, tail)
        except Exception:
            log.exception("storage writer crashed")
            raise

    async def _drain_batch(self) -> list[Kline]:
        batch: list[Kline] = []
        deadline = time.monotonic() + config.BATCH_INTERVAL_MS / 1000
        while len(batch) < config.BATCH_SIZE:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                ev = await asyncio.wait_for(self.queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            batch.append(ev)
        return batch

    def _flush(self, batch: list[Kline]) -> None:
        rows = [
            (
                ev.interval, ev.open_time, ev.close_time,
                ev.open, ev.high, ev.low, ev.close,
                ev.volume, ev.quote_volume, ev.trades_count,
                ev.taker_buy_base_volume, ev.taker_buy_quote_volume,
                ev.event_time, ev.recv_time,
            )
            for ev in batch
        ]
        conn = self._conn
        assert conn is not None
        try:
            conn.execute("BEGIN")
            conn.executemany(
                "INSERT OR REPLACE INTO klines "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        self._klines_written += len(rows)
