"""Read-only reporting and presentation queries.

These back the !active, !report, and !admin views. They return plain
presentation dicts (not SignalData models) — e.g. pending_limits here is a
list of floats aggregated in SQL, not LimitData rows.
"""

from datetime import datetime, timedelta
from typing import Any, Optional

import pytz

from models.enums import SignalStatus
from utils.formatting import get_status_emoji

from .utils import _parse_dt


async def get_active_signals_detailed_sorted(
    db, instrument: Optional[str] = None, sort_by: str = "recent", limit: Optional[int] = None
) -> list[dict[str, Any]]:
    """Get detailed active signals with sorting options."""
    base_query = """
        SELECT
            s.*,
            COUNT(DISTINCT l.id) as total_limit_count,
            COUNT(DISTINCT CASE WHEN l.status = 'hit' THEN l.id END) as hit_limit_count,
            STRING_AGG(
                (CASE WHEN l.status = 'pending' THEN l.price_level END)::TEXT, ',' ORDER BY l.sequence_number) as pending_limits_str,
            STRING_AGG(
                (CASE WHEN l.status = 'hit' THEN l.price_level END)::TEXT, ',' ORDER BY l.sequence_number) as hit_limits_str
        FROM signals s
        LEFT JOIN limits l ON s.id = l.signal_id
        WHERE s.status IN ($1, $2)
    """

    params = [SignalStatus.ACTIVE, SignalStatus.HIT]

    if instrument:
        base_query += " AND s.instrument = $3"
        params.append(instrument)

    base_query += " GROUP BY s.id"

    if sort_by == "recent":
        base_query += " ORDER BY s.created_at DESC"
    elif sort_by == "oldest":
        base_query += " ORDER BY s.created_at ASC"
    elif sort_by == "progress":
        base_query += " ORDER BY hit_limit_count DESC, s.created_at DESC"
    else:
        base_query += " ORDER BY s.created_at DESC"

    if limit:
        base_query += f" LIMIT {limit}"

    rows = await db.fetch_all(base_query, tuple(params))

    for row in rows:
        row["pending_limits"] = []
        row["hit_limits"] = []

        if row.get("pending_limits_str"):
            row["pending_limits"] = [float(p) for p in row["pending_limits_str"].split(",")]

        if row.get("hit_limits_str"):
            row["hit_limits"] = [float(p) for p in row["hit_limits_str"].split(",")]

        row.pop("pending_limits_str", None)
        row.pop("hit_limits_str", None)

        if row.get("expiry_time"):
            expiry = _parse_dt(row["expiry_time"])
            now = datetime.now(pytz.UTC)
            remaining = expiry - now
            if remaining.total_seconds() > 0:
                hours = int(remaining.total_seconds() // 3600)
                minutes = int((remaining.total_seconds() % 3600) // 60)
                row["time_remaining"] = f"{hours}h {minutes}m"
            else:
                row["time_remaining"] = "Expired"
        else:
            row["time_remaining"] = "No expiry"

        row["status_emoji"] = get_status_emoji(row["status"])
        row["progress"] = f"{row['hit_limit_count']}/{row['total_limit_count']} limits hit"

    return rows


async def get_statistics(db) -> dict[str, Any]:
    """Get comprehensive database statistics."""
    stats = {}

    result = await db.fetch_one("SELECT COUNT(*) as count FROM signals")
    stats["total_signals"] = result["count"]

    status_results = await db.fetch_all(
        "SELECT status, COUNT(*) as count FROM signals GROUP BY status"
    )
    stats["by_status"] = {row["status"]: row["count"] for row in status_results}

    result = await db.fetch_one(
        "SELECT COUNT(*) as count FROM signals WHERE status IN ($1, $2)",
        (SignalStatus.ACTIVE, SignalStatus.HIT),
    )
    stats["tracking_count"] = result["count"]

    today_start = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    today_stats = await db.get_performance_stats(start_date=today_start)
    stats["today"] = today_stats["overall"]

    overall_stats = await db.get_performance_stats()
    stats["overall"] = overall_stats["overall"]
    stats["by_instrument"] = overall_stats["by_instrument"]

    return stats


async def get_trading_period_range(period: str = "week") -> dict[str, Any]:
    """
    Get the date range for the current trading period.
    Trading week starts Sunday 6:00 PM UTC and ends Sunday 5:59 PM UTC.
    'month' covers a rolling window of the past 30 days.
    """
    now = datetime.now(pytz.UTC)

    if period == "week":
        days_since_sunday = (now.weekday() + 1) % 7
        last_sunday = now - timedelta(days=days_since_sunday)

        week_start = last_sunday.replace(hour=18, minute=0, second=0, microsecond=0)

        if now < week_start:
            week_start = week_start - timedelta(days=7)

        week_end = week_start + timedelta(days=7) - timedelta(seconds=1)

        return {
            "start": week_start,
            "end": week_end,
            "display_start": week_start.strftime("%B %d, %Y"),
            "display_end": week_end.strftime("%B %d, %Y"),
        }

    if period == "month":
        month_end = now
        month_start = now - timedelta(days=30)

        return {
            "start": month_start,
            "end": month_end,
            "display_start": month_start.strftime("%B %d, %Y"),
            "display_end": month_end.strftime("%B %d, %Y"),
        }

    raise ValueError(f"Invalid period: {period}")


async def get_period_signals_with_results(db, start_date, end_date) -> list[dict[str, Any]]:
    """Get all signals with final results within a date range."""
    query = """
        SELECT
            s.id,
            s.message_id,
            s.channel_id,
            s.instrument,
            s.direction,
            s.status,
            s.type,
            s.limits_hit,
            s.total_limits,
            s.created_at,
            s.closed_at,
            CASE
                WHEN s.closed_at IS NOT NULL THEN s.closed_at
                ELSE s.updated_at
            END as completion_time
        FROM signals s
        WHERE s.status IN ($1, $2, $3)
        AND (
            (s.closed_at IS NOT NULL AND s.closed_at >= $4 AND s.closed_at <= $5)
            OR
            (s.closed_at IS NULL AND s.updated_at >= $6 AND s.updated_at <= $7)
        )
        ORDER BY completion_time DESC
    """

    params = (
        SignalStatus.PROFIT,
        SignalStatus.BREAKEVEN,
        SignalStatus.STOP_LOSS,
        start_date,
        end_date,
        start_date,
        end_date,
    )

    rows = await db.fetch_all(query, params)

    result = []
    for raw in rows:
        row = dict(raw)
        row["status_emoji"] = get_status_emoji(row["status"])
        if row["total_limits"] > 0:
            row["completion_pct"] = (row["limits_hit"] / row["total_limits"]) * 100
        else:
            row["completion_pct"] = 0
        result.append(row)

    return result
