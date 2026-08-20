"""Windowed access to the local tick archive.

Signals are replayed in time order, so weeks are cached with a small LRU and a
window spanning a week boundary is stitched from adjacent files.
"""

import os
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from backtest.symbols import ARCHIVE_DIR

CACHE_WEEKS = 8


class PolledSeries:
    """A window sub-sampled to a polling timer, queried like the full series.

    Indices returned are indices into the parent window, so a caller can move
    between the polled view and the tick-exact one without translating.
    """

    __slots__ = ("idx", "val", "_cummax", "_cummin")

    def __init__(self, idx: np.ndarray, val: np.ndarray):
        self.idx = idx
        self.val = val
        self._cummax = self._cummin = None

    def _from(self, start: int) -> int:
        """First polled position whose tick index is at or after `start`."""
        return int(np.searchsorted(self.idx, start, side="left"))

    def first_at_or_above(self, level: float, start: int = 0) -> int:
        j = self._from(start)
        seg = self.val[j:]
        if not len(seg):
            return -1
        hit = np.flatnonzero(np.maximum.accumulate(seg) >= level)
        return int(self.idx[j + hit[0]]) if len(hit) else -1

    def first_at_or_below(self, level: float, start: int = 0) -> int:
        j = self._from(start)
        seg = self.val[j:]
        if not len(seg):
            return -1
        hit = np.flatnonzero(np.minimum.accumulate(seg) <= level)
        return int(self.idx[j + hit[0]]) if len(hit) else -1


class TickWindow:
    """Bid/ask arrays for one time window, with cumulative extrema for level queries.

    Every question the replay asks is "when did price first reach level X", so the
    running extrema are built once per window and each level becomes a binary
    search instead of a scan.
    """

    __slots__ = ("time_ms", "bid", "ask", "_cummax_bid", "_cummin_bid",
                 "_cummax_ask", "_cummin_ask", "_polled")

    def __init__(self, time_ms: np.ndarray, bid: np.ndarray, ask: np.ndarray):
        self.time_ms = time_ms
        self.bid = bid
        self.ask = ask
        self._cummax_bid = self._cummin_bid = None
        self._cummax_ask = self._cummin_ask = None
        self._polled = {}

    def polled(self, seconds: float, series: str) -> "PolledSeries":
        """The window as a process polling `series` every `seconds` would see it.

        The execution bot closes a take-profit by checking price on a timer and
        then selling at market, so it cannot act on a spike that lives and dies
        between two checks. Replaying such an exit tick-exact credits the
        strategy with prices no order ever got.
        """
        key = (seconds, series)
        view = self._polled.get(key)
        if view is None:
            src = self.bid if series == "bid" else self.ask
            step = int(seconds * 1000)
            grid = np.arange(self.time_ms[0], self.time_ms[-1] + step, step, dtype=np.int64)
            # The price a poll sees is the last tick at or before it.
            idx = np.searchsorted(self.time_ms, grid, side="right") - 1
            idx = np.unique(idx[idx >= 0])
            view = PolledSeries(idx, src[idx])
            self._polled[key] = view
        return view

    def __len__(self) -> int:
        return len(self.time_ms)

    def _extremum(self, attr: str, source: np.ndarray, fn) -> np.ndarray:
        cached = getattr(self, attr)
        if cached is None:
            cached = fn(source)
            setattr(self, attr, cached)
        return cached

    def first_at_or_below(self, series: str, level: float, start: int = 0) -> int:
        """Index of the first tick at or after `start` where `series` <= level, or -1."""
        src = self.bid if series == "bid" else self.ask
        attr = "_cummin_bid" if series == "bid" else "_cummin_ask"
        running = self._extremum(attr, src, np.minimum.accumulate)
        # running[i] is the min up to i; the first i where it drops to level is the
        # first touch. Searching the whole array then filtering on start is wrong
        # when the level was already breached before start, so search the suffix.
        if start:
            suffix_min = np.minimum.accumulate(src[start:])
            idx = int(np.searchsorted(-suffix_min, -level, side="left"))
            return start + idx if idx < len(suffix_min) else -1
        idx = int(np.searchsorted(-running, -level, side="left"))
        return idx if idx < len(running) else -1

    def first_at_or_above(self, series: str, level: float, start: int = 0) -> int:
        """Index of the first tick at or after `start` where `series` >= level, or -1."""
        src = self.bid if series == "bid" else self.ask
        attr = "_cummax_bid" if series == "bid" else "_cummax_ask"
        running = self._extremum(attr, src, np.maximum.accumulate)
        if start:
            suffix_max = np.maximum.accumulate(src[start:])
            idx = int(np.searchsorted(suffix_max, level, side="left"))
            return start + idx if idx < len(suffix_max) else -1
        idx = int(np.searchsorted(running, level, side="left"))
        return idx if idx < len(running) else -1

    def at(self, idx: int) -> tuple:
        """(datetime_utc, bid, ask) at an index."""
        return (
            datetime.fromtimestamp(self.time_ms[idx] / 1000, tz=timezone.utc),
            float(self.bid[idx]),
            float(self.ask[idx]),
        )


class TickStore:
    def __init__(self, archive_dir: str = ARCHIVE_DIR):
        self.dir = archive_dir
        self._cache: OrderedDict = OrderedDict()
        self.misses = 0

    def available_symbols(self) -> set:
        return {f.rsplit("_", 1)[0] for f in os.listdir(self.dir) if f.endswith(".parquet")}

    def _week_key(self, dt: datetime) -> datetime:
        d = dt.astimezone(timezone.utc)
        monday = d - timedelta(days=d.weekday())
        return monday.replace(hour=0, minute=0, second=0, microsecond=0)

    def _load_week(self, symbol: str, week: datetime):
        key = (symbol, week)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]

        path = os.path.join(self.dir, f"{symbol}_{week:%Y%m%d}.parquet")
        if not os.path.exists(path):
            data = None
        else:
            df = pd.read_parquet(path)
            data = (df.time_msc.to_numpy(np.int64),
                    df.bid.to_numpy(np.float64),
                    df.ask.to_numpy(np.float64))

        self._cache[key] = data
        if len(self._cache) > CACHE_WEEKS:
            self._cache.popitem(last=False)
        return data

    def window(self, symbol: str, start: datetime, end: datetime):
        """Ticks in [start, end). Returns None when the archive has no data there."""
        if end <= start:
            return None

        # Files are named by the server-time week they were requested for, but
        # their contents are true UTC, so a file can hold ticks up to 3h either
        # side of its nominal week. A day of slack covers that; a full week of it
        # made every short window concatenate three files' worth of ticks.
        weeks, cur = [], self._week_key(start - timedelta(days=1))
        last = self._week_key(end + timedelta(days=1))
        while cur <= last:
            weeks.append(cur)
            cur += timedelta(days=7)

        parts = [self._load_week(symbol, w) for w in weeks]
        parts = [p for p in parts if p is not None]
        if not parts:
            self.misses += 1
            return None

        if len(parts) == 1:
            t, b, a = parts[0]
        else:
            t = np.concatenate([p[0] for p in parts])
            b = np.concatenate([p[1] for p in parts])
            a = np.concatenate([p[2] for p in parts])

        lo = int(np.searchsorted(t, int(start.timestamp() * 1000), side="left"))
        hi = int(np.searchsorted(t, int(end.timestamp() * 1000), side="left"))
        if hi <= lo:
            return None
        return TickWindow(t[lo:hi], b[lo:hi], a[lo:hi])
