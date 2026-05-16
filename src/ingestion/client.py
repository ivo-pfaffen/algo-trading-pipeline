import asyncio
import logging
import time

import orjson
import websockets
from websockets.asyncio.client import ClientConnection

from src import config
from src.ingestion.storage import Storage
from src.models import Kline

log = logging.getLogger(__name__)

_KLINE_PREFIX = "@kline_"


class BinanceFuturesClient:
    """Reads Binance USDm Futures combined-stream WS, parses each kline
    message, stamps a local recv_time, and pushes closed Klines onto Storage.

    Reconnects with exponential backoff. The same URL is used on every
    reconnect. Since we only consume public streams, the server resumes
    the live feed; no resubscribe is needed.
    """

    def __init__(self, url: str, storage: Storage) -> None:
        self.url = url
        self.storage = storage
        self._stop = asyncio.Event()
        self._messages_received = 0

    def stop(self) -> None:
        self._stop.set()

    def stats(self) -> dict[str, int]:
        return {"ws_messages": self._messages_received}

    async def run(self) -> None:
        delay = config.RECONNECT_BASE_DELAY_S
        while not self._stop.is_set():
            try:
                log.info("connecting to %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=config.WS_PING_INTERVAL_S,
                    ping_timeout=config.WS_PING_TIMEOUT_S,
                    max_size=2**20,
                ) as ws:
                    log.info("ws connected")
                    delay = config.RECONNECT_BASE_DELAY_S
                    await self._read_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._stop.is_set():
                    break
                log.warning("ws error: %s — reconnecting in %.1fs", e, delay)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, config.RECONNECT_MAX_DELAY_S)

    async def _read_loop(self, ws: ClientConnection) -> None:
        async for raw in ws:
            recv_time = int(time.time() * 1000)
            try:
                msg = orjson.loads(raw)
            except orjson.JSONDecodeError:
                log.warning("dropping non-JSON message")
                continue
            self._messages_received += 1
            event = self._parse(msg.get("stream", ""), msg.get("data") or {}, recv_time)
            if event is not None:
                await self.storage.put(event)
            if self._stop.is_set():
                return

    @staticmethod
    def _parse(stream: str, data: dict, recv_time: int) -> Kline | None:
        idx = stream.find(_KLINE_PREFIX)
        if idx < 0:
            return None
        interval = stream[idx + len(_KLINE_PREFIX):]
        k = data.get("k")
        if not k or not k.get("x"):
            # In-progress kline updates fire every ~250ms. We only store
            # closed bars; live signal logic can subscribe to the queue
            # directly if it needs the in-progress bar.
            return None
        return Kline(
            interval=interval,
            open_time=k["t"],
            close_time=k["T"],
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            quote_volume=float(k["q"]),
            trades_count=k["n"],
            taker_buy_base_volume=float(k["V"]),
            taker_buy_quote_volume=float(k["Q"]),
            event_time=data.get("E", k["T"]),
            recv_time=recv_time,
        )
