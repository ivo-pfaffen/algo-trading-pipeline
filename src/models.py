from dataclasses import dataclass


@dataclass(slots=True)
class Kline:
    interval: str          # "1m", "5m", "15m", "1h", "4h"
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades_count: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float
    event_time: int        # exchange event time (ms); for backfill we set this to close_time
    recv_time: int         # local receive time (ms)
