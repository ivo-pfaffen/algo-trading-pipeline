"""CLI: market map snapshot.

Reads klines from the project SQLite DB and prints:
  - directional bias from an EMA on a higher timeframe (default: EMA50 on 1h)
  - rolling Volume Profile on 15m (default: last 7 days)
  - current price taken from the latest 1m close
plus an ASCII histogram of the volume distribution by price, with the
POC, value area, and current-price location marked inline.

Run from project root:
    python -m tools.show_vp
    python -m tools.show_vp --vp-days 3 --vp-bins 80 --ema-interval 4h --ema-period 20
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, timezone

import numpy as np

from src import config
from src.analytics.indicators import VolumeProfile, ema, volume_profile


def fetch_klines(
    conn: sqlite3.Connection, interval: str, since_ms: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Return (highs, lows, closes, volumes) for `interval` since `since_ms`, ascending."""
    rows = conn.execute(
        "SELECT high, low, close, volume FROM klines "
        "WHERE interval = ? AND open_time >= ? "
        "ORDER BY open_time ASC",
        (interval, since_ms),
    ).fetchall()
    if not rows:
        return None
    arr = np.array(rows, dtype=np.float64)
    return arr[:, 0], arr[:, 1], arr[:, 2], arr[:, 3]


def latest_1m_close(conn: sqlite3.Connection) -> tuple[float, int]:
    row = conn.execute(
        "SELECT close, open_time FROM klines WHERE interval = '1m' "
        "ORDER BY open_time DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise SystemExit("no 1m klines in DB — run ingestion first")
    return float(row[0]), int(row[1])


def render_histogram(
    vp: VolumeProfile,
    current_price: float,
    bar_width: int = 42,
) -> str:
    """ASCII histogram, highest price on top, with VP/POC/NOW markers."""
    edges = vp.bin_edges
    vols = vp.bin_volumes
    n = len(vols)
    peak = float(vols.max()) if vols.max() > 0 else 1.0

    # Locate the bin containing the current price; may be outside the VP range
    # if the market has moved past the rolling window's extremes.
    above_range = current_price >= edges[-1]
    below_range = current_price < edges[0]
    cur_bin = None
    if not (above_range or below_range):
        cur_bin = min(int((current_price - edges[0]) / (edges[1] - edges[0])), n - 1)

    lines: list[str] = []
    if above_range:
        lines.append(f"  {'(above VP range)':>12} {'·' * bar_width}  ← NOW @ {current_price:,.2f}")
        lines.append("")

    for i in range(n - 1, -1, -1):
        bin_lo = edges[i]
        bin_hi = edges[i + 1]
        center = (bin_lo + bin_hi) / 2
        bar_len = int(round(bar_width * vols[i] / peak))

        in_va = vp.val <= center <= vp.vah
        is_poc = i == int(np.argmax(vols))

        # Fill char differs inside the value area so the 70% zone reads
        # at a glance; POC row gets a heavier accent.
        if is_poc:
            fill = "█"
        elif in_va:
            fill = "▓"
        else:
            fill = "░"
        bar = fill * bar_len + " " * (bar_width - bar_len)

        tags = []
        if is_poc:
            tags.append("◀ POC")
        if i == cur_bin:
            tags.append("◀ NOW")
        tag = "  " + "  ".join(tags) if tags else ""

        lines.append(f"  {center:>10,.2f}  {bar}  {vols[i]:>8,.2f}{tag}")

    if below_range:
        lines.append("")
        lines.append(f"  {'(below VP range)':>12} {'·' * bar_width}  ← NOW @ {current_price:,.2f}")

    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--vp-days", type=int, default=7, help="VP rolling window in days (default 7)")
    p.add_argument("--vp-bins", type=int, default=60, help="VP price bins (default 60)")
    p.add_argument(
        "--ema-interval",
        choices=["5m", "15m", "1h", "4h"],
        default="1h",
        help="timeframe for the bias EMA (default 1h)",
    )
    p.add_argument("--ema-period", type=int, default=50, help="EMA period for the bias (default 50)")
    args = p.parse_args()

    now_ms = int(time.time() * 1000)
    vp_since = now_ms - args.vp_days * 86_400_000

    # Pull enough EMA-timeframe candles to warm up: 3x the period is a comfortable
    # margin where the EMA has effectively converged.
    interval_ms = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}[args.ema_interval]
    ema_since = now_ms - (args.ema_period * 3) * interval_ms

    conn = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    try:
        m15 = fetch_klines(conn, "15m", vp_since)
        if m15 is None:
            raise SystemExit("no 15m klines in DB — run backfill + ingestion first")
        ema_data = fetch_klines(conn, args.ema_interval, ema_since)
        if ema_data is None:
            raise SystemExit(f"no {args.ema_interval} klines in DB")
        cur_price, cur_t = latest_1m_close(conn)
    finally:
        conn.close()

    highs, lows, _, volumes = m15
    vp = volume_profile(highs, lows, volumes, bins=args.vp_bins)

    _, _, ema_closes, _ = ema_data
    ema_arr = ema(ema_closes, args.ema_period)
    ema_now = float(ema_arr[-1])

    if np.isnan(ema_now):
        bias = "INDEFINIDO"
        bias_note = (
            f"not enough {args.ema_interval} candles for EMA{args.ema_period}: "
            f"have {len(ema_closes)}, need {args.ema_period}"
        )
    else:
        bias = "ALCISTA" if cur_price > ema_now else "BAJISTA"
        bias_note = f"ema = {ema_now:,.2f}   distance = {(cur_price - ema_now):+,.2f} "\
                    f"({(cur_price - ema_now) / ema_now * 100:+.2f}%)"

    # Distances to key levels, in % — useful to eyeball entry proximity.
    dist_poc = (cur_price - vp.poc) / vp.poc * 100
    dist_vah = (cur_price - vp.vah) / vp.vah * 100
    dist_val = (cur_price - vp.val) / vp.val * 100

    # Trade hint: only fires when the setup looks live.
    if bias == "ALCISTA" and cur_price <= vp.val * 1.001:
        hint = "ALCISTA + tocando demanda (≤ VAL) → setup LONG (mean reversion)"
    elif bias == "BAJISTA" and cur_price >= vp.vah * 0.999:
        hint = "BAJISTA + tocando oferta (≥ VAH) → setup SHORT (mean reversion)"
    elif bias == "ALCISTA":
        hint = "alcista pero precio aún no toca demanda — sin setup"
    elif bias == "BAJISTA":
        hint = "bajista pero precio aún no toca oferta — sin setup"
    else:
        hint = "(bias indefinido — agregá más historia o bajá --ema-period)"

    ts = datetime.fromtimestamp(cur_t / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print()
    print(f"  BTCUSDT — snapshot @ {ts}")
    print(f"  current price  :  {cur_price:,.2f}")
    print(f"  bias (EMA{args.ema_period} {args.ema_interval})  :  {bias}   {bias_note}")
    print()
    print(f"  Volume Profile — last {args.vp_days}d on 15m, {args.vp_bins} bins,"
          f" total vol = {vp.total_volume:,.0f} BTC")
    print(f"    POC : {vp.poc:>10,.2f}   ({dist_poc:+.2f}% from now)")
    print(f"    VAH : {vp.vah:>10,.2f}   ({dist_vah:+.2f}% from now)   oferta / resistencia")
    print(f"    VAL : {vp.val:>10,.2f}   ({dist_val:+.2f}% from now)   demanda / soporte")
    print()
    print(f"  setup : {hint}")
    print()
    print(f"  {'price':>10}  {'volume distribution':<42}  {'vol (BTC)':>8}")
    print(f"  {'─' * 10}  {'─' * 42}  {'─' * 8}")
    print(render_histogram(vp, cur_price))
    print()


if __name__ == "__main__":
    main()
