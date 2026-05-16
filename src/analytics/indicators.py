"""Pure indicator functions: EMA and Volume Profile.

Operate on plain numpy arrays — no DB, no I/O — so they can be unit-tested
and reused unchanged by the CLI tool, the dashboard, and the backtester.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


def ema(closes: np.ndarray, period: int) -> np.ndarray:
    """Exponential moving average, TradingView-compatible.

    Seeded with the SMA of the first `period` values, then recursive with
    alpha = 2 / (period + 1). The first `period - 1` outputs are NaN.
    """
    closes = np.asarray(closes, dtype=np.float64)
    n = closes.size
    out = np.full(n, np.nan)
    if n < period:
        return out
    alpha = 2.0 / (period + 1)
    out[period - 1] = closes[:period].mean()
    for i in range(period, n):
        out[i] = alpha * closes[i] + (1 - alpha) * out[i - 1]
    return out


@dataclass(slots=True)
class VolumeProfile:
    poc: float               # bin center with peak volume — the "magnet"
    val: float               # value area low: lower edge of the 70% volume region
    vah: float               # value area high: upper edge of the 70% volume region
    bin_edges: np.ndarray    # length N+1, ascending price edges
    bin_volumes: np.ndarray  # length N, total volume in each bin
    total_volume: float


def volume_profile(
    highs: np.ndarray,
    lows: np.ndarray,
    volumes: np.ndarray,
    bins: int = 60,
    value_area_pct: float = 0.70,
) -> VolumeProfile:
    """Price-bucketed volume profile.

    Each candle's volume is distributed across the price bins it overlaps,
    proportional to the overlap with the candle's [low, high] range. That
    is more faithful than dumping all volume at the typical price — at 15m
    a BTC candle can span dozens of dollars, which is several bins wide.

    Returns POC (peak bin), VAL/VAH (edges of the value area covering
    `value_area_pct` of total volume), and the raw histogram + bin edges
    so callers can plot or inspect.
    """
    highs = np.asarray(highs, dtype=np.float64)
    lows = np.asarray(lows, dtype=np.float64)
    volumes = np.asarray(volumes, dtype=np.float64)
    if highs.size == 0:
        raise ValueError("volume_profile: empty input")
    if not (highs.size == lows.size == volumes.size):
        raise ValueError("volume_profile: highs, lows, volumes must have equal length")

    price_min = float(lows.min())
    price_max = float(highs.max())
    if price_max <= price_min:
        # Pathological flat market — collapse to a single bin.
        return VolumeProfile(
            poc=price_min,
            val=price_min,
            vah=price_max,
            bin_edges=np.array([price_min, price_max + 1e-9]),
            bin_volumes=np.array([volumes.sum()]),
            total_volume=float(volumes.sum()),
        )

    edges = np.linspace(price_min, price_max, bins + 1)
    bin_width = edges[1] - edges[0]
    hist = np.zeros(bins, dtype=np.float64)

    for low, high, vol in zip(lows, highs, volumes):
        if vol <= 0:
            continue
        rng = high - low
        if rng <= 0:
            # Zero-range candle: dump volume into the single bin containing `low`.
            idx = min(int((low - price_min) / bin_width), bins - 1)
            hist[idx] += vol
            continue
        lo_idx = max(int((low - price_min) / bin_width), 0)
        hi_idx = min(int((high - price_min) / bin_width), bins - 1)
        for b in range(lo_idx, hi_idx + 1):
            overlap = min(edges[b + 1], high) - max(edges[b], low)
            if overlap > 0:
                hist[b] += vol * (overlap / rng)

    total = float(hist.sum())
    poc_idx = int(np.argmax(hist))
    poc = (edges[poc_idx] + edges[poc_idx + 1]) / 2

    # Build the value area by expanding outward from the POC, always taking
    # the higher-volume neighbor, until cumulative volume crosses the target.
    target = total * value_area_pct
    cum = float(hist[poc_idx])
    lo, hi = poc_idx, poc_idx
    while cum < target and (lo > 0 or hi < bins - 1):
        left = float(hist[lo - 1]) if lo > 0 else -1.0
        right = float(hist[hi + 1]) if hi < bins - 1 else -1.0
        if right >= left:
            hi += 1
            cum += float(hist[hi])
        else:
            lo -= 1
            cum += float(hist[lo])

    return VolumeProfile(
        poc=float(poc),
        val=float(edges[lo]),
        vah=float(edges[hi + 1]),
        bin_edges=edges,
        bin_volumes=hist,
        total_volume=total,
    )
