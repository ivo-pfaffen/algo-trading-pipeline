import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

from src import config
from src.ingestion.client import BinanceFuturesClient
from src.ingestion.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ingestion")

LIVE_REFRESH_S = 1.0


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}GB"


def _db_total_bytes(db_path: Path) -> int:
    total = 0
    for p in (db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm")):
        if p.exists():
            total += p.stat().st_size
    return total


async def _live_display(
    client: BinanceFuturesClient,
    storage: Storage,
    db_path: Path,
    stop: asyncio.Event,
) -> None:
    prev_t = time.monotonic()
    prev = {"ws_messages": 0, "klines_written": 0}

    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=LIVE_REFRESH_S)
            break
        except asyncio.TimeoutError:
            pass

        now = time.monotonic()
        dt = now - prev_t
        prev_t = now

        c = client.stats()
        s = storage.stats()
        cur = {"ws_messages": c["ws_messages"], "klines_written": s["klines_written"]}
        rate = {k: (cur[k] - prev[k]) / dt for k in cur}
        prev = cur

        line = (
            f"\rmsgs={cur['ws_messages']} ({rate['ws_messages']:.0f}/s) | "
            f"klines={cur['klines_written']} ({rate['klines_written']:.1f}/s) | "
            f"queue={s['queue_depth']} | "
            f"db={_human_bytes(_db_total_bytes(db_path))}"
        )
        sys.stdout.write(line.ljust(120))
        sys.stdout.flush()

    sys.stdout.write("\n")
    sys.stdout.flush()


async def main() -> None:
    storage = Storage(config.DB_PATH, config.SCHEMA_PATH)
    await storage.start()
    client = BinanceFuturesClient(config.WS_URL, storage)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    def _shutdown() -> None:
        log.info("shutdown requested")
        stop.set()
        client.stop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    display_task = asyncio.create_task(
        _live_display(client, storage, config.DB_PATH, stop), name="live-display"
    )
    try:
        await client.run()
    finally:
        stop.set()
        display_task.cancel()
        try:
            await display_task
        except asyncio.CancelledError:
            pass
        await storage.stop()
        log.info("stopped. final stats: %s | %s", client.stats(), storage.stats())


if __name__ == "__main__":
    asyncio.run(main())
