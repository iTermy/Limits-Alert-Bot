"""
Analytics and statistics operations for signals
"""
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pytz
from database.models import SignalStatus
from utils.logger import get_logger

logger = get_logger("signal_db.analytics")


class AnalyticsManager:
    """Handles analytics and statistics for signals"""

    def __init__(self, db_manager):
        """
        Initialize analytics manager

        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager

    async def get_statistics(self, db_manager) -> Dict[str, Any]:
        """
        Get comprehensive database statistics

        Args:
            db_manager: Database manager instance

        Returns:
            Statistics dictionary
        """
        stats = {}

        # Total signals
        total_query = "SELECT COUNT(*) as count FROM signals"
        result = await db_manager.fetch_one(total_query)
        stats['total_signals'] = result['count']

        # Signals by status
        status_query = """
            SELECT status, COUNT(*) as count 
            FROM signals 
            GROUP BY status
        """
        status_results = await db_manager.fetch_all(status_query)
        stats['by_status'] = {row['status']: row['count'] for row in status_results}

        # Active tracking stats
        tracking_query = """
            SELECT COUNT(*) as count 
            FROM signals 
            WHERE status IN (?, ?)
        """
        result = await db_manager.fetch_one(
            tracking_query,
            (SignalStatus.ACTIVE, SignalStatus.HIT)
        )
        stats['tracking_count'] = result['count']

        # Today's performance
        today_start = datetime.now(pytz.UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()

        today_stats = await db_manager.get_performance_stats(
            start_date=today_start
        )
        stats['today'] = today_stats['overall']

        # Overall performance
        overall_stats = await db_manager.get_performance_stats()
        stats['overall'] = overall_stats['overall']
        stats['by_instrument'] = overall_stats['by_instrument']

        return stats

    async def get_performance_by_period(self, db_manager, period: str = 'week') -> Dict[str, Any]:
        """
        Get performance statistics for a specific period

        Args:
            db_manager: Database manager instance
            period: 'day', 'week', 'month', or 'all'

        Returns:
            Performance statistics for the period
        """
        # Calculate start date based on period
        now = datetime.now(pytz.UTC)

        if period == 'day':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == 'week':
            days_since_monday = now.weekday()
            start_date = now - timedelta(days=days_since_monday)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == 'month':
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:  # 'all'
            start_date = None

        start_date_str = start_date.isoformat() if start_date else None

        return await db_manager.get_performance_stats(start_date=start_date_str)

    async def get_instrument_performance(self, db_manager, instrument: str) -> Dict[str, Any]:
        """
        Get detailed performance for a specific instrument

        Args:
            db_manager: Database manager instance
            instrument: Trading instrument (e.g., 'GBPUSD')

        Returns:
            Performance statistics for the instrument
        """
        query = """
            SELECT 
                COUNT(*) as total_signals,
                COUNT(CASE WHEN status = 'profit' THEN 1 END) as profitable,
                COUNT(CASE WHEN status = 'breakeven' THEN 1 END) as breakeven,
                COUNT(CASE WHEN status = 'stop_loss' THEN 1 END) as stop_loss,
                COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled,
                COUNT(CASE WHEN status IN ('active', 'hit') THEN 1 END) as active,
                ROUND(
                    CAST(COUNT(CASE WHEN status = 'profit' THEN 1 END) AS FLOAT) / 
                    NULLIF(COUNT(CASE WHEN status IN ('profit', 'breakeven', 'stop_loss') THEN 1 END), 0) * 100, 2
                ) as win_rate,
                AVG(limits_hit) as avg_limits_hit,
                MAX(limits_hit) as max_limits_hit
            FROM signals
            WHERE instrument = ?
        """

        stats = await db_manager.fetch_one(query, (instrument,))

        # Get recent performance (last 10 closed trades)
        recent_query = """
            SELECT 
                id, status, created_at, closed_at,
                limits_hit, total_limits
            FROM signals
            WHERE instrument = ? 
            AND status IN ('profit', 'breakeven', 'stop_loss')
            ORDER BY closed_at DESC
            LIMIT 10
        """

        recent_trades = await db_manager.fetch_all(recent_query, (instrument,))

        return {
            'summary': dict(stats) if stats else {},
            'recent_trades': recent_trades
        }

    async def get_daily_statistics(self, db_manager, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get daily statistics for the last N days

        Args:
            db_manager: Database manager instance
            days: Number of days to retrieve

        Returns:
            List of daily statistics
        """
        query = """
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as total_signals,
                COUNT(CASE WHEN status = 'profit' THEN 1 END) as profitable,
                COUNT(CASE WHEN status = 'breakeven' THEN 1 END) as breakeven,
                COUNT(CASE WHEN status = 'stop_loss' THEN 1 END) as stop_loss,
                COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled
            FROM signals
            WHERE created_at >= datetime('now', '-' || ? || ' days')
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        """

        return await db_manager.fetch_all(query, (days,))

    async def get_hit_rate_statistics(self, db_manager) -> Dict[str, Any]:
        """
        Get statistics about limit hit rates

        Args:
            db_manager: Database manager instance

        Returns:
            Hit rate statistics
        """
        # Overall hit rate
        hit_rate_query = """
            SELECT 
                COUNT(DISTINCT s.id) as signals_with_hits,
                COUNT(DISTINCT CASE WHEN s.limits_hit = s.total_limits THEN s.id END) as all_limits_hit,
                AVG(CAST(s.limits_hit AS FLOAT) / NULLIF(s.total_limits, 0)) as avg_hit_rate
            FROM signals s
            WHERE s.status IN ('hit', 'profit', 'breakeven', 'stop_loss')
            AND s.total_limits > 0
        """

        overall_stats = await db_manager.fetch_one(hit_rate_query)

        # Hit rate by instrument
        instrument_query = """
            SELECT 
                instrument,
                AVG(CAST(limits_hit AS FLOAT) / NULLIF(total_limits, 0)) as avg_hit_rate,
                COUNT(*) as total_signals
            FROM signals
            WHERE status IN ('hit', 'profit', 'breakeven', 'stop_loss')
            AND total_limits > 0
            GROUP BY instrument
            ORDER BY avg_hit_rate DESC
        """

        by_instrument = await db_manager.fetch_all(instrument_query)

        return {
            'overall': dict(overall_stats) if overall_stats else {},
            'by_instrument': by_instrument
        }


    """
    Add these methods to your AnalyticsManager class in analytics.py
    """


    async def get_trading_period_range(self, period: str = 'week') -> Dict[str, Any]:
        """
        Get the date range for the current trading period
        Trading week starts Sunday 6:00 PM UTC and ends Sunday 5:59 PM UTC

        Args:
            period: 'week' or 'month'

        Returns:
            Dictionary with start/end dates and display strings
        """
        now = datetime.now(pytz.UTC)

        if period == 'week':
            # Find the most recent Sunday 6:00 PM
            days_since_sunday = (now.weekday() + 1) % 7  # Monday = 0, Sunday = 6
            last_sunday = now - timedelta(days=days_since_sunday)

            # Set to 6:00 PM (18:00)
            week_start = last_sunday.replace(hour=18, minute=0, second=0, microsecond=0)

            # If we're before Sunday 6:00 PM, we're still in the previous week
            if now < week_start:
                week_start = week_start - timedelta(days=7)

            # End is next Sunday at 5:59:59 PM
            week_end = week_start + timedelta(days=7) - timedelta(seconds=1)

            return {
                'start': week_start.isoformat(),
                'end': week_end.isoformat(),
                'display_start': week_start.strftime('%B %d, %Y'),
                'display_end': week_end.strftime('%B %d, %Y')
            }

        elif period == 'month':
            # Current month from the 1st at 00:00 to the last day at 23:59:59
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            # Find last day of month
            if now.month == 12:
                next_month = month_start.replace(year=now.year + 1, month=1)
            else:
                next_month = month_start.replace(month=now.month + 1)

            month_end = next_month - timedelta(seconds=1)

            return {
                'start': month_start.isoformat(),
                'end': month_end.isoformat(),
                'display_start': month_start.strftime('%B %d, %Y'),
                'display_end': month_end.strftime('%B %d, %Y')
            }

        else:
            raise ValueError(f"Invalid period: {period}")


    async def get_period_signals_with_results(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Get all signals with final results (profit/breakeven/stop_loss) within a date range

        Args:
            start_date: ISO format start date
            end_date: ISO format end date

        Returns:
            List of signals with their results
        """
        query = """
            SELECT 
                s.id,
                s.message_id,
                s.channel_id,
                s.instrument,
                s.direction,
                s.status,
                s.limits_hit,
                s.total_limits,
                s.created_at,
                s.closed_at,
                CASE 
                    WHEN s.closed_at IS NOT NULL THEN s.closed_at
                    ELSE s.updated_at
                END as completion_time
            FROM signals s
            WHERE s.status IN (?, ?, ?)
            AND (
                (s.closed_at IS NOT NULL AND s.closed_at >= ? AND s.closed_at <= ?)
                OR 
                (s.closed_at IS NULL AND s.updated_at >= ? AND s.updated_at <= ?)
            )
            ORDER BY completion_time DESC
        """

        params = (
            SignalStatus.PROFIT,
            SignalStatus.BREAKEVEN,
            SignalStatus.STOP_LOSS,
            start_date, end_date,
            start_date, end_date
        )

        signals = await self.db.fetch_all(query, params)

        # Convert to list of dicts and add additional info
        result = []
        for signal in signals:
            signal_dict = dict(signal)

            # Add status emoji for display
            from .utils import get_status_emoji
            signal_dict['status_emoji'] = get_status_emoji(signal_dict['status'])

            # Add completion percentage
            if signal_dict['total_limits'] > 0:
                signal_dict['completion_pct'] = (signal_dict['limits_hit'] / signal_dict['total_limits']) * 100
            else:
                signal_dict['completion_pct'] = 0

            result.append(signal_dict)

        return result


    async def get_week_performance_summary(self) -> Dict[str, Any]:
        """
        Get a quick summary of the current week's performance

        Returns:
            Dictionary with week's performance metrics
        """
        date_range = await self.get_trading_period_range('week')
        signals = await self.get_period_signals_with_results(
            date_range['start'],
            date_range['end']
        )

        total = len(signals)
        profit = len([s for s in signals if s['status'] == SignalStatus.PROFIT])
        breakeven = len([s for s in signals if s['status'] == SignalStatus.BREAKEVEN])
        stop_loss = len([s for s in signals if s['status'] == SignalStatus.STOP_LOSS])

        # Calculate win rate
        trades_with_outcome = profit + stop_loss
        win_rate = (profit / trades_with_outcome * 100) if trades_with_outcome > 0 else 0

        return {
            'period': 'week',
            'date_range': date_range,
            'total_signals': total,
            'profit': profit,
            'breakeven': breakeven,
            'stop_loss': stop_loss,
            'win_rate': win_rate,
            'signals': signals
        }


    async def get_month_performance_summary(self) -> Dict[str, Any]:
        """
        Get a quick summary of the current month's performance

        Returns:
            Dictionary with month's performance metrics
        """
        date_range = await self.get_trading_period_range('month')
        signals = await self.get_period_signals_with_results(
            date_range['start'],
            date_range['end']
        )

        total = len(signals)
        profit = len([s for s in signals if s['status'] == SignalStatus.PROFIT])
        breakeven = len([s for s in signals if s['status'] == SignalStatus.BREAKEVEN])
        stop_loss = len([s for s in signals if s['status'] == SignalStatus.STOP_LOSS])

        # Calculate win rate
        trades_with_outcome = profit + stop_loss
        win_rate = (profit / trades_with_outcome * 100) if trades_with_outcome > 0 else 0

        return {
            'period': 'month',
            'date_range': date_range,
            'total_signals': total,
            'profit': profit,
            'breakeven': breakeven,
            'stop_loss': stop_loss,
            'win_rate': win_rate,
            'signals': signals
        }