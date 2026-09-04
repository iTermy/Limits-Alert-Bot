"""Archive ICMarkets bid/ask ticks to local parquet, one file per symbol-week.

The terminal's tick history is a rolling window that recedes as time passes (the
M1 window lost three weeks between 2026-08-04 and 2026-08-20), so this pulls the
data out of MT5 and onto disk once. Resumable: existing files are skipped, so the
script can be interrupted and re-run.

Usage:  python -m backtest.archive_ticks [--symbols XAUUSD,EURUSD] [--refresh-last]
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtest.symbols import ARCHIVE_DIR, ARCHIVE_SYMBOLS, IC_TERMINAL, TICK_HISTORY_START

# Weekly chunks: ~2.7M gold ticks per week, which MT5 returns in one call without
# the per-day request overhead of ~2,800 separate fetches.
CHUNK_DAYS = 7

# MT5 stamps ticks in the broker's server time, not UTC. ICMarkets runs EET/EEST,
# so a tick rendered as UTC reads 2-3h ahead of the instant it happened. Left
# uncorrected this put recorded gold fills $34 away from the archived price;
# converting through the server's zone brings the median error to under $1, which
# is then just the pooler's write latency. Verified by backtest.calibrate.
SERVER_TZ = "Europe/Athens"


def server_ms_to_utc_ms(time_msc):
    """Reinterpret server-time epoch ms as true UTC epoch ms."""
    wall = pd.DatetimeIndex(pd.to_datetime(np.asarray(time_msc), unit="ms"))
    utc = wall.tz_localize(SERVER_TZ, ambiguous=True, nonexistent="shift_forward").tz_convert("UTC")
    # Cast through an explicit ms dtype: pandas picks the resolution of the input
    # (datetime64[ms] here), so asi8 is not reliably nanoseconds.
    return np.asarray(utc.tz_localize(None), dtype="datetime64[ms]").astype("int64")


def week_starts(start: datetime, end: datetime):
    """Monday-aligned week starts covering [start, end]."""
    cur = start - timedelta(days=start.weekday())
    while cur <= end:
        yield cur
        cur += timedelta(days=CHUNK_DAYS)


def fetch_week(ic_symbol: str, week_start: datetime) -> pd.DataFrame | None:
    ticks = mt5.copy_ticks_range(
        ic_symbol, week_start, week_start + timedelta(days=CHUNK_DAYS), mt5.COPY_TICKS_ALL
    )
    if ticks is None or not len(ticks):
        return None
    df = pd.DataFrame(ticks)[["time_msc", "bid", "ask"]]
    # A tick with no bid or no ask can't be used for fill logic on either side.
    df = df[(df.bid > 0) & (df.ask > 0)]
    if not len(df):
        return None
    df = df.reset_index(drop=True)
    df["time_msc"] = server_ms_to_utc_ms(df["time_msc"])
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", help="comma-separated internal names; default all")
    ap.add_argument("--refresh-last", action="store_true",
                    help="re-fetch the most recent week even if it exists (it is still filling)")
    args = ap.parse_args()

    wanted = ARCHIVE_SYMBOLS
    if args.symbols:
        names = [s.strip().upper() for s in args.symbols.split(",")]
        wanted = {k: v for k, v in ARCHIVE_SYMBOLS.items() if k in names}
        missing = set(names) - set(wanted)
        if missing:
            print(f"unknown symbols: {sorted(missing)}", flush=True)
            return 1

    if not mt5.initialize(path=IC_TERMINAL):
        print(f"MT5 initialize failed: {mt5.last_error()}", flush=True)
        return 1
    company = mt5.terminal_info().company
    print(f"connected: {company} | archiving {len(wanted)} symbols -> {ARCHIVE_DIR}", flush=True)
    if "Raw Trading" not in company:
        print("WARNING: not the ICMarkets terminal — aborting rather than mixing brokers", flush=True)
        mt5.shutdown()
        return 1

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    start = datetime.fromisoformat(TICK_HISTORY_START).replace(tzinfo=timezone.utc)
    end = datetime.now(timezone.utc)
    weeks = list(week_starts(start, end))
    last_week = weeks[-1]

    total_rows = total_bytes = 0
    t_begin = time.time()

    for internal, ic_symbol in wanted.items():
        info = mt5.symbol_info(ic_symbol)
        if info is None:
            print(f"{internal:10s} SKIP — {ic_symbol} not found on this terminal", flush=True)
            continue
        if not info.visible:
            mt5.symbol_select(ic_symbol, True)

        sym_rows = sym_files = 0
        t_sym = time.time()
        for ws in weeks:
            path = os.path.join(ARCHIVE_DIR, f"{internal}_{ws:%Y%m%d}.parquet")
            if os.path.exists(path) and not (args.refresh_last and ws == last_week):
                continue
            df = fetch_week(ic_symbol, ws)
            if df is None:
                continue
            df.to_parquet(path, compression="zstd", index=False)
            sym_rows += len(df)
            sym_files += 1
            total_bytes += os.path.getsize(path)

        total_rows += sym_rows
        print(f"{internal:10s} ({ic_symbol:8s}) {sym_files:3d} new weeks, "
              f"{sym_rows:>10,} ticks, {time.time() - t_sym:6.1f}s", flush=True)

    mt5.shutdown()
    print(f"\ndone: {total_rows:,} ticks written, {total_bytes / 1e9:.2f} GB, "
          f"{(time.time() - t_begin) / 60:.1f} min", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
