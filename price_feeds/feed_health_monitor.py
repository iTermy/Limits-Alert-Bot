"""
Feed Health Monitor - Tracks price feed health and handles failures
Monitors all feeds (ICMarkets, OANDA, Binance) for stale data and connection issues
"""

import asyncio
import logging
from typing import Dict, Optional, Set
from datetime import datetime, timedelta
from pathlib import Path
import json
import pytz
from collections import defaultdict

from price_feeds.symbol_mapper import SymbolMapper
from utils.logger import get_logger

logger = get_logger('feed_health')


class FeedHealthMonitor:
    """
    Monitors price feed health and handles failures

    Features:
    - Tracks last update time per feed per symbol
    - Detects stale feeds (>5 min no updates during market hours)
    - Respects market hours (no alerts on weekends/holidays)
    - Handles spread hour (5-6 PM EST daily)
    - Automatic reconnection with retry logic
    - Admin DM alerts for persistent failures
    - Alert cooldown to prevent spam
    """

    def __init__(self, stream_manager, bot, admin_user_id: int = None):
        """
        Initialize feed health monitor

        Args:
            stream_manager: PriceStreamManager instance
            bot: Discord bot instance
            admin_user_id: Discord user ID for alerts (can be set later)
        """
        self.stream_manager = stream_manager
        self.bot = bot
        self.admin_user_id = admin_user_id
        self.symbol_mapper = SymbolMapper()

        # Load configuration
        self.config = self._load_config()

        # Monitoring state
        self.running = False
        self.monitor_task = None
        self.startup_time = datetime.now()

        # Track last update times: feed -> symbol -> timestamp
        self.last_seen: Dict[str, Dict[str, datetime]] = defaultdict(dict)

        # Track feed status
        self.feed_status: Dict[str, str] = {}  # 'healthy', 'degraded', 'down'
        self.last_alert_time: Dict[str, datetime] = {}
        self.reconnect_attempts: Dict[str, int] = defaultdict(int)

        # Track alert history to prevent spam
        self.alert_history: Dict[str, datetime] = {}

        # Statistics
        self.stats = {
            'checks_performed': 0,
            'stale_detections': 0,
            'reconnections_attempted': 0,
            'reconnections_successful': 0,
            'alerts_sent': 0,
            'false_positives_avoided': 0
        }

        # Timezone for market hours
        self.est = pytz.timezone('America/New_York')

        logger.info(f"FeedHealthMonitor initialized (admin: {admin_user_id})")

    def _load_config(self) -> Dict:
        """Load health monitoring configuration"""
        config_path = Path(__file__).resolve().parent.parent / 'config' / 'health_config.json'

        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return self._get_default_config()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config: {e}, using defaults")
            return self._get_default_config()

    def _get_default_config(self) -> Dict:
        """Get default configuration"""
        return {
            "check_interval_seconds": 60,
            "stale_threshold_seconds": 300,
            "max_reconnect_attempts": 3,
            "reconnect_delay_seconds": 10,
            "admin_user_id": None,
            "alert_cooldown_minutes": 15,
            "startup_grace_period_seconds": 120,
            "market_hours": {
                "crypto": {"always_open": True},
                "stocks": {
                    "days": [1, 2, 3, 4, 5],
                    "open_time": "09:30",
                    "close_time": "17:00",
                    "timezone": "America/New_York"
                },
                "forex": {
                    "days": [0, 1, 2, 3, 4, 5],
                    "open_time": "18:00",
                    "close_time": "17:00",
                    "timezone": "America/New_York",
                    "spread_hour_start": "17:00",
                    "spread_hour_end": "18:00"
                }
            }
        }

    def set_admin_user(self, user_id: int):
        """Set admin user ID for alerts"""
        self.admin_user_id = user_id
        logger.info(f"Admin user set to: {user_id}")

    async def start_monitoring(self):
        """Start the health monitoring loop"""
        if self.running:
            logger.warning("Health monitor already running")
            return

        self.running = True
        self.startup_time = datetime.now()

        # Start monitoring task
        self.monitor_task = asyncio.create_task(self._monitoring_loop())

        logger.info("Feed health monitoring started")

    async def stop_monitoring(self):
        """Stop the health monitoring loop"""
        self.running = False

        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("Feed health monitoring stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop"""
        check_interval = self.config.get('check_interval_seconds', 60)

        while self.running:
            try:
                await self.check_feed_health()
                self.stats['checks_performed'] += 1
            except Exception as e:
                logger.error(f"Error in health check: {e}", exc_info=True)

            await asyncio.sleep(check_interval)

    def update_last_seen(self, symbol: str, feed: str):
        """
        Update last seen timestamp for a symbol on a feed
        Called by PriceStreamManager on each price update

        Args:
            symbol: Internal format symbol
            feed: Feed name
        """
        self.last_seen[feed][symbol] = datetime.now()

    async def check_feed_health(self):
        """
        Check health of all feeds
        Main health check logic
        """
        now = datetime.now()

        # Skip checks during startup grace period
        if (now - self.startup_time).total_seconds() < self.config.get('startup_grace_period_seconds', 120):
            logger.debug("Within startup grace period, skipping health checks")
            return

        stale_threshold = timedelta(seconds=self.config.get('stale_threshold_seconds', 300))

        # Check each feed
        for feed_name in ['icmarkets', 'oanda', 'binance']:
            await self._check_feed(feed_name, stale_threshold, now)

    async def _check_feed(self, feed_name: str, stale_threshold: timedelta, now: datetime):
        """Check health of a specific feed"""
        # Get subscribed symbols for this feed
        feed_symbols = self.last_seen.get(feed_name, {})

        if not feed_symbols:
            # No symbols subscribed for this feed
            self.feed_status[feed_name] = 'idle'
            return

        # Check each symbol
        stale_symbols = []

        for symbol, last_update in feed_symbols.items():
            time_since_update = now - last_update

            if time_since_update > stale_threshold:
                # Check if market should be open for this symbol
                asset_class = self.symbol_mapper.determine_asset_class(symbol)

                if self.is_market_open(asset_class):
                    stale_symbols.append({
                        'symbol': symbol,
                        'last_update': last_update,
                        'time_since': time_since_update
                    })

        # Determine feed health
        if not stale_symbols:
            # All symbols healthy
            if self.feed_status.get(feed_name) in ['degraded', 'down']:
                # Feed recovered!
                await self._handle_feed_recovery(feed_name)

            self.feed_status[feed_name] = 'healthy'
            self.reconnect_attempts[feed_name] = 0

        elif len(stale_symbols) < len(feed_symbols) * 0.5:
            # Less than 50% stale - degraded
            if self.feed_status.get(feed_name) != 'degraded':
                self.feed_status[feed_name] = 'degraded'
                logger.warning(f"{feed_name} feed degraded: {len(stale_symbols)}/{len(feed_symbols)} symbols stale")
                self.stats['false_positives_avoided'] += 1  # Might be temporary

        else:
            # 50%+ stale - feed is down
            if self.feed_status.get(feed_name) != 'down':
                self.stats['stale_detections'] += 1

            self.feed_status[feed_name] = 'down'
            await self._handle_feed_failure(feed_name, stale_symbols)

    async def _handle_feed_failure(self, feed_name: str, stale_symbols: list):
        """
        Handle feed failure
        Attempt reconnection and send alerts if needed
        """
        logger.error(f"{feed_name} feed failure detected: {len(stale_symbols)} stale symbols")

        # Check alert cooldown
        if not self._should_send_alert(feed_name):
            logger.debug(f"Alert cooldown active for {feed_name}, skipping")
            return

        # Attempt reconnection
        max_attempts = self.config.get('max_reconnect_attempts', 3)

        if self.reconnect_attempts[feed_name] < max_attempts:
            success = await self.attempt_reconnection(feed_name)

            if success:
                logger.info(f"{feed_name} reconnection successful")
                return  # Don't alert if reconnection worked
        else:
            logger.error(f"{feed_name} max reconnection attempts reached")

        # Send admin alert
        await self._send_feed_failure_alert(feed_name, stale_symbols)

    async def _handle_feed_recovery(self, feed_name: str):
        """Handle feed recovery"""
        logger.info(f"{feed_name} feed recovered")

        # Send recovery notification
        await self._send_feed_recovery_alert(feed_name)

        # Reset reconnection attempts
        self.reconnect_attempts[feed_name] = 0

    async def attempt_reconnection(self, feed_name: str) -> bool:
        """
        Attempt to reconnect a failed feed

        Args:
            feed_name: Name of the feed to reconnect

        Returns:
            True if reconnection successful, False otherwise
        """
        self.reconnect_attempts[feed_name] += 1
        self.stats['reconnections_attempted'] += 1

        logger.info(f"Attempting reconnection for {feed_name} (attempt {self.reconnect_attempts[feed_name]})")

        try:
            # Wait before reconnection attempt
            delay = self.config.get('reconnect_delay_seconds', 10)
            await asyncio.sleep(delay)

            # Attempt reconnection through stream manager
            result = await self.stream_manager.reconnect_all()

            if result.get(feed_name):
                self.stats['reconnections_successful'] += 1
                logger.info(f"{feed_name} reconnection successful")
                return True
            else:
                logger.warning(f"{feed_name} reconnection failed")
                return False

        except Exception as e:
            logger.error(f"Error reconnecting {feed_name}: {e}")
            return False

    def _should_send_alert(self, feed_name: str) -> bool:
        """Check if we should send an alert (respects cooldown)"""
        cooldown_minutes = self.config.get('alert_cooldown_minutes', 15)
        cooldown = timedelta(minutes=cooldown_minutes)

        last_alert = self.last_alert_time.get(feed_name)

        if last_alert is None:
            return True

        return (datetime.now() - last_alert) > cooldown

    async def _send_feed_failure_alert(self, feed_name: str, stale_symbols: list):
        """Send admin DM alert for feed failure"""
        if not self.admin_user_id:
            logger.warning("No admin user ID set, cannot send DM alert")
            return

        try:
            admin_user = await self.bot.fetch_user(self.admin_user_id)

            # Build alert message
            stale_list = '\n'.join([
                f"• {s['symbol']}: {self._format_duration(s['time_since'])} ago"
                for s in stale_symbols[:10]  # Limit to 10 symbols
            ])

            if len(stale_symbols) > 10:
                stale_list += f"\n• ... and {len(stale_symbols) - 10} more"

            message = (
                f"⚠️ **{feed_name.upper()} Feed Down**\n\n"
                f"**Affected Symbols:** {len(stale_symbols)}\n"
                f"{stale_list}\n\n"
                f"**Reconnection Attempts:** {self.reconnect_attempts[feed_name]}/{self.config.get('max_reconnect_attempts', 3)}\n"
                f"**Status:** {'Failed' if self.reconnect_attempts[feed_name] >= self.config.get('max_reconnect_attempts', 3) else 'Retrying'}\n\n"
                f"{'⚠️ Manual intervention may be required' if self.reconnect_attempts[feed_name] >= self.config.get('max_reconnect_attempts', 3) else '🔄 Automatic reconnection in progress'}"
            )

            await admin_user.send(message)

            self.last_alert_time[feed_name] = datetime.now()
            self.stats['alerts_sent'] += 1

            logger.info(f"Sent failure alert to admin for {feed_name}")

        except Exception as e:
            logger.error(f"Failed to send admin alert: {e}")

    async def _send_feed_recovery_alert(self, feed_name: str):
        """Send admin DM alert for feed recovery"""
        if not self.admin_user_id:
            return

        try:
            admin_user = await self.bot.fetch_user(self.admin_user_id)

            # Calculate downtime
            last_alert = self.last_alert_time.get(feed_name)
            downtime = ""

            if last_alert:
                duration = datetime.now() - last_alert
                downtime = f"\n**Downtime:** {self._format_duration(duration)}"

            message = (
                f"✅ **{feed_name.upper()} Feed Recovered**\n"
                f"{downtime}\n"
                f"**Current Status:** Healthy\n"
                f"All symbols receiving updates normally"
            )

            await admin_user.send(message)

            logger.info(f"Sent recovery alert to admin for {feed_name}")

        except Exception as e:
            logger.error(f"Failed to send recovery alert: {e}")

    async def send_admin_alert(self, message: str):
        """
        Send a custom alert to admin

        Args:
            message: Alert message
        """
        if not self.admin_user_id:
            logger.warning("No admin user ID set, cannot send alert")
            return

        try:
            admin_user = await self.bot.fetch_user(self.admin_user_id)
            await admin_user.send(message)
            logger.info("Sent custom admin alert")
        except Exception as e:
            logger.error(f"Failed to send custom alert: {e}")

    def is_market_open(self, asset_class: str) -> bool:
        """
        Check if market is open for a given asset class

        Args:
            asset_class: Asset class (forex, stocks, crypto, metals, indices)

        Returns:
            True if market should be open, False otherwise
        """
        now = datetime.now(self.est)

        # Normalize asset class
        if asset_class == 'forex_jpy':
            asset_class = 'forex'

        market_config = self.config['market_hours'].get(asset_class)

        if not market_config:
            # Unknown asset class, assume open to avoid false alerts
            logger.warning(f"Unknown asset class: {asset_class}, assuming market open")
            return True

        # Crypto is always open
        if market_config.get('always_open'):
            return True

        # Check day of week (0 = Monday, 6 = Sunday)
        if now.weekday() not in market_config.get('days', []):
            return False

        # Check if it's a holiday (for stocks)
        if asset_class == 'stocks':
            today_str = now.strftime('%Y-%m-%d')
            if today_str in self.config.get('us_market_holidays_2025', []):
                return False

        # Check spread hour (for forex/metals/indices)
        if 'spread_hour_start' in market_config:
            spread_start = datetime.strptime(market_config['spread_hour_start'], '%H:%M').time()
            spread_end = datetime.strptime(market_config['spread_hour_end'], '%H:%M').time()

            if spread_start <= now.time() < spread_end:
                # During spread hour - expect less frequent updates but not a failure
                return True  # Don't alert during spread hour

        # Check market hours
        open_time = datetime.strptime(market_config['open_time'], '%H:%M').time()
        close_time = datetime.strptime(market_config['close_time'], '%H:%M').time()

        # Handle markets that close next day (forex: Sun 6PM - Fri 5PM)
        if close_time < open_time:
            # Market is open from open_time to midnight, and midnight to close_time
            return now.time() >= open_time or now.time() < close_time
        else:
            # Normal market hours (stocks: 9:30 AM - 5:00 PM)
            return open_time <= now.time() < close_time

    def _format_duration(self, duration: timedelta) -> str:
        """Format duration in human-readable form"""
        total_seconds = int(duration.total_seconds())

        if total_seconds < 60:
            return f"{total_seconds} seconds"

        minutes = total_seconds // 60
        if minutes < 60:
            return f"{minutes} minutes"

        hours = minutes // 60
        remaining_minutes = minutes % 60

        if hours < 24:
            return f"{hours} hours, {remaining_minutes} minutes"

        days = hours // 24
        remaining_hours = hours % 24
        return f"{days} days, {remaining_hours} hours"

    def get_health_stats(self) -> Dict:
        """
        Get health monitoring statistics

        Returns:
            Dictionary with health stats
        """
        now = datetime.now()

        feed_details = {}
        for feed_name in ['icmarkets', 'oanda', 'binance']:
            feed_symbols = self.last_seen.get(feed_name, {})

            if feed_symbols:
                oldest_update = min(feed_symbols.values())
                newest_update = max(feed_symbols.values())

                feed_details[feed_name] = {
                    'status': self.feed_status.get(feed_name, 'unknown'),
                    'symbols_monitored': len(feed_symbols),
                    'oldest_update': self._format_duration(now - oldest_update),
                    'newest_update': self._format_duration(now - newest_update),
                    'reconnect_attempts': self.reconnect_attempts.get(feed_name, 0)
                }
            else:
                feed_details[feed_name] = {
                    'status': 'idle',
                    'symbols_monitored': 0
                }

        return {
            'overall_stats': self.stats,
            'feed_details': feed_details,
            'monitoring_running': self.running,
            'uptime': self._format_duration(now - self.startup_time),
            'admin_configured': self.admin_user_id is not None
        }

    def get_feed_status_summary(self) -> str:
        """Get a formatted summary of feed status"""
        stats = self.get_health_stats()

        lines = [
            "**Feed Health Status**",
            ""
        ]

        for feed_name, details in stats['feed_details'].items():
            status_emoji = {
                'healthy': '✅',
                'degraded': '⚠️',
                'down': '❌',
                'idle': '⏸️',
                'unknown': '❓'
            }.get(details['status'], '❓')

            lines.append(f"{status_emoji} **{feed_name.upper()}**: {details['status']}")

            if details['symbols_monitored'] > 0:
                lines.append(f"   • Symbols: {details['symbols_monitored']}")
                lines.append(f"   • Last update: {details['newest_update']} ago")

                if details['reconnect_attempts'] > 0:
                    lines.append(f"   • Reconnect attempts: {details['reconnect_attempts']}")

            lines.append("")

        lines.append(f"**Monitoring Status:** {'Running' if stats['monitoring_running'] else 'Stopped'}")
        lines.append(f"**Uptime:** {stats['uptime']}")

        return '\n'.join(lines)