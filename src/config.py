from pathlib import Path

SYMBOL = "btcusdt"
INTERVALS = ["1m", "5m", "15m", "1h", "4h"]
STREAMS = [f"{SYMBOL}@kline_{iv}" for iv in INTERVALS]
WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(STREAMS)

# REST endpoint used by the backfill script.
REST_KLINES_URL = "https://api.binance.com/api/v3/klines"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "market.db"
SCHEMA_PATH = PROJECT_ROOT / "src" / "ingestion" / "schema.sql"

# Writer batching: flush when batch is full OR the interval expires.
# With only closed klines the throughput is tiny — these mostly just
# bound shutdown latency now.
BATCH_SIZE = 50
BATCH_INTERVAL_MS = 1000

QUEUE_MAXSIZE = 1_000

RECONNECT_BASE_DELAY_S = 1.0
RECONNECT_MAX_DELAY_S = 60.0

WS_PING_INTERVAL_S = 20
WS_PING_TIMEOUT_S = 20
