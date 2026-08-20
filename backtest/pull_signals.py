"""One-shot pull of the signal tables covering the tick-archive window.

Supabase egress is near its limit (DATA_ANALYSIS.md §7), so this runs once and
every downstream step reads the local pickles. Re-run only when you need data
newer than the last pull.

Usage:  python -m backtest.pull_signals
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtest.symbols import TICK_HISTORY_START

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = r"C:\Python Stuff\TM-Backtest-Data\signals"

# Everything is scoped to signals inside the tick window; tp_outcomes is joined
# through signal_id so execution rows for out-of-window signals don't come along.
TABLES = {
    "signals": "SELECT * FROM signals WHERE created_at >= $1",
    "limits": "SELECT l.* FROM limits l JOIN signals s ON s.id = l.signal_id WHERE s.created_at >= $1",
    "status_changes": "SELECT c.* FROM status_changes c JOIN signals s ON s.id = c.signal_id "
                      "WHERE s.created_at >= $1",
    "excursions": "SELECT e.* FROM signal_excursions e JOIN signals s ON s.id = e.signal_id "
                  "WHERE s.created_at >= $1",
    "trailing": "SELECT t.* FROM trailing_simulations t JOIN signals s ON s.id = t.signal_id "
                "WHERE s.created_at >= $1",
    "tp_outcomes": "SELECT o.* FROM tp_outcomes o JOIN signals s ON s.id = o.signal_id "
                   "WHERE s.created_at >= $1",
    "config_history": "SELECT * FROM config_history WHERE changed_at >= $1",
}


async def main() -> int:
    load_dotenv(os.path.join(REPO, ".env"))
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        print("SUPABASE_DB_URL not set", flush=True)
        return 1

    os.makedirs(OUT, exist_ok=True)
    since = datetime.fromisoformat(TICK_HISTORY_START).replace(tzinfo=timezone.utc)
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        for name, query in TABLES.items():
            rows = await conn.fetch(query, since)
            df = pd.DataFrame([dict(r) for r in rows])
            df.to_pickle(os.path.join(OUT, f"{name}.pkl"))
            print(f"{name:16s} {len(df):>7,} rows  {len(df.columns):>3} cols", flush=True)
    finally:
        await conn.close()
    print(f"\nwritten to {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
