-- Binance Futures klines for multiple intervals in a single table.
-- Times are unix milliseconds. Only closed klines are stored
-- (both from WS live and from REST backfill).

CREATE TABLE IF NOT EXISTS klines (
    interval                TEXT    NOT NULL,
    open_time               INTEGER NOT NULL,
    close_time              INTEGER NOT NULL,
    open                    REAL    NOT NULL,
    high                    REAL    NOT NULL,
    low                     REAL    NOT NULL,
    close                   REAL    NOT NULL,
    volume                  REAL    NOT NULL,
    quote_volume            REAL    NOT NULL,
    trades_count            INTEGER NOT NULL,
    taker_buy_base_volume   REAL    NOT NULL,
    taker_buy_quote_volume  REAL    NOT NULL,
    event_time              INTEGER NOT NULL,
    recv_time               INTEGER NOT NULL,
    PRIMARY KEY (interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_klines_open_time
    ON klines(open_time);
