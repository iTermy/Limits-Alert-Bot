"""Prove the tick archive lines up with what the bot actually recorded.

Every real fill carries `hit_time` and `hit_price` from the live feed. Looking the
archive up at that instant tests three things at once:

  1. Clock. MT5 stores tick times in server time (EET/EEST); if the archive were
     offset by 2-3h, gold would be tens of dollars away. Candidate offsets are
     scanned and the winner reported per symbol.
  2. Side of the book. A long fills on the ask, a short on the bid - unless the
     spread buffer flipped it to the other side.
  3. Broker basis. Indices, oil and crypto were priced off OANDA/Exness/Binance
     but replay off ICMarkets, so their levels differ by an offset that has to be
     measured before their results mean anything.

Usage:  python -m backtest.calibrate
"""

import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtest.engine import pip_size
from backtest.ticks import TickStore

SIG_DIR = r"C:\Python Stuff\TM-Backtest-Data\signals"
OFFSETS_HOURS = [-3, -2, -1, 0, 1, 2, 3]
LOOKUP_TOLERANCE = timedelta(seconds=30)

# hit_time is the DB write, which trails the tick by ~2s on the Supabase pooler
# (the error curve bottoms cleanly there). Comparisons look back by that much.
WRITE_LATENCY_S = -2


def load():
    sig = pd.read_pickle(os.path.join(SIG_DIR, "signals.pkl"))
    lim = pd.read_pickle(os.path.join(SIG_DIR, "limits.pkl"))
    return sig, lim


def price_at(store: TickStore, symbol: str, when, offset_h: int):
    """(bid, ask) at the archived tick nearest `when` shifted by `offset_h`."""
    t = when + timedelta(hours=offset_h)
    win = store.window(symbol, t - LOOKUP_TOLERANCE, t + LOOKUP_TOLERANCE)
    if win is None or not len(win):
        return None
    target = int(t.timestamp() * 1000)
    i = int(np.searchsorted(win.time_ms, target))
    i = min(max(i, 0), len(win) - 1)
    return float(win.bid[i]), float(win.ask[i])


def main() -> int:
    sig, lim = load()
    store = TickStore()
    have = store.available_symbols()

    hits = lim[(lim.status == "hit") & lim.hit_time.notna() & lim.hit_price.notna()]
    sig_idx = sig.set_index("id")[["instrument", "direction"]]
    hits = hits.join(sig_idx, on="signal_id", how="inner")
    hits = hits[hits.instrument.isin(have)]
    print(f"recorded fills usable for calibration: {len(hits):,} "
          f"across {hits.instrument.nunique()} symbols\n")

    # --- 1. clock offset, decided on gold (deepest sample, IC-native) ---
    gold = hits[hits.instrument == "XAUUSD"].head(400)
    print("clock offset scan on XAUUSD (median |archive mid - recorded hit_price|):")
    best_off, best_err = 0, None
    for off in OFFSETS_HOURS:
        errs = []
        for _, r in gold.iterrows():
            px = price_at(store, "XAUUSD", r.hit_time, off)
            if px:
                errs.append(abs((px[0] + px[1]) / 2 - r.hit_price))
        if errs:
            med = float(np.median(errs))
            print(f"   {off:+d}h : {med:10.3f}   (n={len(errs)})")
            if best_err is None or med < best_err:
                best_off, best_err = off, med
    print(f"\n-> best offset {best_off:+d}h, median error {best_err:.3f} "
          f"({'UTC, no shift needed' if best_off == 0 else 'SHIFT REQUIRED'})\n")

    # --- 2 & 3. per-symbol basis and side-of-book, at the winning offset ---
    rows = []
    for sym, grp in hits.groupby("instrument"):
        grp = grp.head(300)
        d_mid, d_bid, d_ask, d_side = [], [], [], []
        for _, r in grp.iterrows():
            px = price_at(store, sym, r.hit_time + timedelta(seconds=WRITE_LATENCY_S), best_off)
            if not px:
                continue
            bid, ask = px
            d_mid.append((bid + ask) / 2 - r.hit_price)
            d_bid.append(bid - r.hit_price)
            d_ask.append(ask - r.hit_price)
            # The bot records the ask for a long and the bid for a short, so the
            # residual on that side is the true broker basis rather than a
            # half-spread artifact.
            d_side.append((ask if r.direction == "long" else bid) - r.hit_price)
        if not d_mid:
            continue
        p = pip_size(sym)
        rows.append(dict(
            symbol=sym, n=len(d_mid),
            med_mid_pips=float(np.median(d_mid)) / p,
            med_bid_pips=float(np.median(d_bid)) / p,
            med_ask_pips=float(np.median(d_ask)) / p,
            basis_price=float(np.median(d_side)),
            basis_pips=float(np.median(d_side)) / p,
            iqr_mid_pips=float(np.subtract(*np.percentile(d_mid, [75, 25]))) / p,
        ))

    out = pd.DataFrame(rows).sort_values("n", ascending=False)
    out[["med_mid_pips", "med_bid_pips", "med_ask_pips", "iqr_mid_pips", "basis_pips"]] =         out[["med_mid_pips", "med_bid_pips", "med_ask_pips", "iqr_mid_pips", "basis_pips"]].round(2)
    print("per-symbol basis vs recorded fill price (pips of that symbol):")
    print(out.to_string(index=False))
    out.to_pickle(os.path.join(SIG_DIR, "calibration.pkl"))
    print(f"\nsaved -> {os.path.join(SIG_DIR, 'calibration.pkl')}")
    print("\nreading it: med_bid/med_ask near 0 identifies which side the recorded")
    print("fill sat on; a large med_mid on a non-IC symbol is the broker basis.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
