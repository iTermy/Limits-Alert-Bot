"""
Feed Health Monitor - Tracks price feed health and handles failures
Monitors all feeds (ICMarkets, OANDA, Binance) for stale data and connection issues
"""

import asyncio
import faulthandler
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from datetime import time as dtime
from typing import Dict

import pytz

from price_feeds.symbol_mapper import SymbolMapper
from utils.logger import get_logger

logger = get_logger("feed_health")

# Monitoring knobs — not user-configurable
CHECK_INTERVAL_SECONDS = 60
STALE_THRESHOLD_SECONDS = 300
MAX_RECONNECT_ATTEMPTS = 3
RECONNECT_DELAY_SECONDS = 10
ALERT_COOLDOWN_MINUTES = 15
STARTUP_GRACE_PERIOD_SECONDS = 120

# Loop-liveness watchdog: an async heartbeat stamps a monotonic timestamp while
# the event loop is healthy; a daemon thread (which survives a frozen loop)
# hard-exits the process if that stamp stops advancing. This is the only backstop
# that works when a synchronous call wedges the loop itself (e.g. a blocking
# subprocess spawn) — at that point graceful shutdown is impossible, so we exit
# and let the main.py supervisor relaunch a fresh instance.
LOOP_HEARTBEAT_INTERVAL_SECONDS = 10
LOOP_WATCHDOG_POLL_SECONDS = 15
LOOP_FREEZE_RESTART_SECONDS = 120

# Where the watchdog writes all-thread stack dumps when it detects a freeze.
FREEZE_DUMP_DIR = "data/logs"

# Price-flow watchdog: when at least one subscribed symbol's market should be
# open but ZERO ticks have arrived across EVERY feed for this long, the bot is
# effectively dead (feeds frozen). Force a supervised restart. The open-market
# gate (is_market_open) means weekends, holidays, and spread hour never trip it.
WATCHDOG_SILENCE_SECONDS = 180
WATCHDOG_GRACE_SECONDS = 180

# Spread hour: 5–6 PM EST weekdays (matches streaming_monitor._is_spread_hour)
_SPREAD_START = dtime(17, 0)
_SPREAD_END = dtime(18, 0)

MARKET_HOURS = {
    "crypto": {"always_open": True},
    "stocks": {
        "days": [1, 2, 3, 4, 5],
        "open_time": "09:30",
        "close_time": "17:00",
        "timezone": "America/New_York",
    },
    "forex": {
        "days": [0, 1, 2, 3, 4, 6],
        "open_time": "18:00",
        "close_time": "17:00",
        "timezone": "America/New_York",
    },
    "metals": {
        "days": [0, 1, 2, 3, 4, 6],
        "open_time": "18:00",
        "close_time": "17:00",
        "timezone": "America/New_York",
    },
    "indices": {
        "days": [0, 1, 2, 3, 4, 6],
        "open_time": "18:00",
        "close_time": "17:00",
        "timezone": "America/New_York",
    },
    "oil": {
        "days": [0, 1, 2, 3, 4, 6],
        "open_time": "18:00",
        "close_time": "17:00",
        "timezone": "America/New_York",
    },
}


class FeedHealthMonitor:
    """
    Monitors price feed health and handles failures.

    Detects stale feeds (>5 min during market hours), attempts reconnection,
    and DMs the configured admin on persistent failures.
    """

    def __init__(
        self,
        stream_manager,
        bot,
        admin_user_id: int = None,
        us_market_holidays: list = None,
        db=None,
    ):
        self.stream_manager = stream_manager
        self.bot = bot
        self.admin_user_id = admin_user_id
        self.us_market_holidays = us_market_holidays or []
        self.db = db
        self.symbol_mapper = SymbolMapper()

        # Monitoring state
        self.running = False
        self.monitor_task = None
        self.startup_time = datetime.now()

        # Set once the watchdog has initiated a restart, so it never fires twice.
        self._watchdog_fired = False

        # Reconnects run off the monitoring loop as tasks, keyed by feed, so a
        # slow or wedged reconnect can never block health checks or the watchdog.
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}

        # Loop-liveness watchdog state (see module constants).
        self._loop_heartbeat = time.monotonic()
        self._heartbeat_task = None
        self._loop_watchdog_thread = None
        self._loop_watchdog_stop = threading.Event()

        # Track last update times: feed -> symbol -> timestamp
        self.last_seen: Dict[str, Dict[str, datetime]] = defaultdict(dict)

        # Track feed status
        self.feed_status: Dict[str, str] = {}  # 'healthy', 'down', 'idle'
        self.last_alert_time: Dict[str, datetime] = {}
        self.reconnect_attempts: Dict[str, int] = defaultdict(int)
        # Earliest stale-symbol last_update timestamp captured when the feed first
        # crossed the down threshold. Used to report accurate downtime on recovery.
        self.first_stale_time: Dict[str, datetime] = {}

        # Track alert history to prevent spam
        self.alert_history: Dict[str, datetime] = {}

        # Statistics
        self.stats = {
            "checks_performed": 0,
            "stale_detections": 0,
            "reconnections_attempted": 0,
            "reconnections_successful": 0,
            "alerts_sent": 0,
        }

        # Timezone for market hours
        self.est = pytz.timezone("America/New_York")

        logger.info(f"FeedHealthMonitor initialized (admin: {admin_user_id})")

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

        # Start the loop-liveness heartbeat + the out-of-loop watchdog thread.
        self._loop_heartbeat = time.monotonic()
        self._heartbeat_task = asyncio.create_task(self._loop_heartbeat_beat())
        self._loop_watchdog_stop.clear()
        self._loop_watchdog_thread = threading.Thread(
            target=self._loop_watchdog_run, name="loop-watchdog", daemon=True
        )
        self._loop_watchdog_thread.start()

        logger.info("Feed health monitoring started")

    async def _loop_heartbeat_beat(self):
        """Stamp a monotonic heartbeat while the event loop is servicing tasks."""
        while self.running:
            self._loop_heartbeat = time.monotonic()
            await asyncio.sleep(LOOP_HEARTBEAT_INTERVAL_SECONDS)

    def _loop_watchdog_run(self):
        """Daemon thread: hard-restart the process if the event loop freezes.

        Runs off the event loop, so it keeps ticking even when the loop is
        blocked by a synchronous call. If the heartbeat stops advancing for
        LOOP_FREEZE_RESTART_SECONDS the loop is wedged and no in-loop recovery
        (timeouts, tasks, bot.close()) can fire — so we flush logs and exit hard
        for the supervisor to relaunch.
        """
        while not self._loop_watchdog_stop.wait(LOOP_WATCHDOG_POLL_SECONDS):
            stalled = time.monotonic() - self._loop_heartbeat
            if stalled > LOOP_FREEZE_RESTART_SECONDS:
                logger.critical(
                    "Event loop frozen for %.0fs (heartbeat stalled) — forcing hard restart",
                    stalled,
                )
                self._dump_all_thread_stacks()
                logging.shutdown()
                os._exit(1)

    def _dump_all_thread_stacks(self):
        """Write every thread's stack to a file before the hard exit.

        A frozen loop means a synchronous call is blocking the loop thread; the
        stack of that thread names the exact culprit. faulthandler runs from this
        daemon thread even though the loop is wedged, so it always captures it.
        """
        try:
            path = os.path.join(FREEZE_DUMP_DIR, "freeze_traceback.log")
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(f"\n===== Event loop freeze at {datetime.now().isoformat()} =====\n")
                faulthandler.dump_traceback(file=fh, all_threads=True)
        except Exception as e:
            logger.error("Failed to dump freeze traceback: %s", e)

    async def stop_monitoring(self):
        """Stop the health monitoring loop"""
        self.running = False

        self._loop_watchdog_stop.set()

        for task in [self.monitor_task, self._heartbeat_task, *self._reconnect_tasks.values()]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._reconnect_tasks.clear()

        logger.info("Feed health monitoring stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                await self.check_feed_health()
                self.stats["checks_performed"] += 1
            except Exception as e:
                logger.error(f"Error in health check: {e}", exc_info=True)

            try:
                await self._check_price_flow_watchdog(datetime.now())
            except Exception as e:
                logger.error(f"Error in price-flow watchdog: {e}", exc_info=True)

            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

    def _global_newest_tick(self):
        """Most recent tick timestamp across every feed and symbol, or None."""
        newest = None
        for feed_data in self.last_seen.values():
            for ts in feed_data.values():
                if newest is None or ts > newest:
                    newest = ts
        return newest

    def _any_open_market_subscribed(self) -> bool:
        """True if any currently-subscribed symbol's market should be open now."""
        subscribed = getattr(self.stream_manager, "subscribed_symbols", set())
        for symbol in subscribed:
            asset_class = self.symbol_mapper.determine_asset_class(symbol)
            if self.is_market_open(asset_class):
                return True
        return False

    async def _check_price_flow_watchdog(self, now: datetime):
        """Force a supervised restart if the bot is producing no ticks at all
        while a subscribed market should be open.

        Conservative by construction — all of these must hold:
        - past the watchdog grace period (avoids startup churn)
        - at least one subscribed symbol whose market is open right now
        - no tick on ANY feed for WATCHDOG_SILENCE_SECONDS
        The open-market gate reuses is_market_open, which already excludes
        weekends, holidays, and spread hour, so legitimately-quiet periods do
        not trip the watchdog.
        """
        if self._watchdog_fired:
            return

        if (now - self.startup_time).total_seconds() < WATCHDOG_GRACE_SECONDS:
            return

        if not self._any_open_market_subscribed():
            return

        newest = self._global_newest_tick() or self.startup_time
        silence = (now - newest).total_seconds()
        if silence <= WATCHDOG_SILENCE_SECONDS:
            return

        self._watchdog_fired = True
        logger.critical(
            "Price-flow watchdog: no ticks on any feed for %.0fs while a market "
            "is open — forcing restart",
            silence,
        )
        # Run the shutdown in its own task: bot.close() cancels this monitor
        # task, so awaiting it here would make the task await itself.
        asyncio.create_task(self._watchdog_restart(int(silence)))

    async def _watchdog_restart(self, silence: int):
        """Alert the admin, then gracefully close the bot so the supervisor
        relaunches a fresh instance."""
        try:
            await self.send_admin_alert(
                f"🚨 **Price-flow watchdog tripped**\n"
                f"No ticks on any feed for {silence}s while a market is open.\n"
                f"Restarting the bot."
            )
        except Exception as e:
            logger.error(f"Watchdog admin alert failed: {e}")

        try:
            await self.bot.close()
        except Exception as e:
            logger.error(f"Watchdog bot.close() failed: {e}")

    def update_last_seen(self, symbol: str, feed: str):
        """
        Update last seen timestamp for a symbol on a feed
        Called by PriceStreamManager on each price update

        Args:
            symbol: Internal format symbol
            feed: Feed name
        """
        self.last_seen[feed][symbol] = datetime.now()

    def clear_symbol(self, symbol: str):
        """
        Remove a symbol from all last_seen tracking.
        Should be called when a symbol is unsubscribed (e.g. DB cleared).
        This prevents stale entries from triggering false feed-down alerts.

        Args:
            symbol: Internal format symbol to remove
        """
        for feed_data in self.last_seen.values():
            feed_data.pop(symbol, None)
        logger.debug(f"Cleared health tracking for symbol: {symbol}")

    async def check_feed_health(self):
        """
        Check health of all feeds
        Main health check logic
        """
        now = datetime.now()

        # Skip checks during startup grace period
        if (now - self.startup_time).total_seconds() < STARTUP_GRACE_PERIOD_SECONDS:
            logger.debug("Within startup grace period, skipping health checks")
            return

        # Stale-feed alerts during closed-market windows (weekends, the Friday
        # 5 PM–Sunday 6 PM EST forex break, holidays, spread hour) are suppressed
        # per-symbol by is_market_open() in _check_feed, so no blanket weekend
        # skip is needed here — and skipping the whole check would also stop the
        # feed_health table from being written while markets are open.
        stale_threshold = timedelta(seconds=STALE_THRESHOLD_SECONDS)

        # Check each feed
        for feed_name in ["icmarkets", "oanda", "binance", "exness"]:
            await self._check_feed(feed_name, stale_threshold, now)

    async def _check_feed(self, feed_name: str, stale_threshold: timedelta, now: datetime):
        """Check health of a specific feed"""
        # Only check symbols that are currently actively subscribed.
        # self.last_seen retains entries forever, so filtering by active subscriptions
        # prevents ghost alerts for symbols whose signals have been cleared from the DB.
        active_symbols = getattr(self.stream_manager, "subscribed_symbols", set())

        feed_last_seen = self.last_seen.get(feed_name, {})
        feed_symbols = {sym: ts for sym, ts in feed_last_seen.items() if sym in active_symbols}

        if not feed_symbols:
            # No active subscriptions for this feed
            self.feed_status[feed_name] = "idle"
            await self._write_feed_health(feed_name, "idle", None, None)
            return

        # Check each actively subscribed symbol
        stale_symbols = []

        for symbol, last_update in feed_symbols.items():
            time_since_update = now - last_update

            if time_since_update > stale_threshold:
                # Check if market should be open for this symbol
                asset_class = self.symbol_mapper.determine_asset_class(symbol)

                if self.is_market_open(asset_class):
                    stale_symbols.append(
                        {
                            "symbol": symbol,
                            "last_update": last_update,
                            "time_since": time_since_update,
                        }
                    )

        # Determine feed health
        newest_seen = max(feed_symbols.values()) if feed_symbols else None

        # The EX bot blocks placement on any feed marked "down", so we only mark a
        # feed down when every subscribed symbol has stalled. One unrelated quiet
        # symbol must not poison every other signal on the feed.
        if len(stale_symbols) < len(feed_symbols):
            if self.feed_status.get(feed_name) == "down":
                await self._handle_feed_recovery(feed_name)

            self.feed_status[feed_name] = "healthy"
            self.reconnect_attempts[feed_name] = 0
            self.first_stale_time.pop(feed_name, None)
            await self._write_feed_health(feed_name, "healthy", 0, newest_seen)

        else:
            if self.feed_status.get(feed_name) != "down":
                self.stats["stale_detections"] += 1
                # Capture the earliest last_update among stale symbols as the
                # real "stall began" timestamp for downtime reporting.
                self.first_stale_time[feed_name] = min(
                    s["last_update"] for s in stale_symbols
                )

            self.feed_status[feed_name] = "down"
            max_stale_secs = int(max(s["time_since"].total_seconds() for s in stale_symbols))
            await self._write_feed_health(feed_name, "down", max_stale_secs, newest_seen)
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

        # Run reconnect + alert off the monitoring loop so a slow or wedged
        # reconnect can't block health checks or the price-flow watchdog. Never
        # stack a second attempt for the same feed while one is in flight.
        existing = self._reconnect_tasks.get(feed_name)
        if existing and not existing.done():
            logger.debug(f"Reconnection already in progress for {feed_name}")
            return

        self._reconnect_tasks[feed_name] = asyncio.create_task(
            self._reconnect_and_alert(feed_name, stale_symbols)
        )

    async def _reconnect_and_alert(self, feed_name: str, stale_symbols: list):
        """Attempt reconnection and, if it fails, DM the admin. Runs as a task."""
        try:
            if self.reconnect_attempts[feed_name] < MAX_RECONNECT_ATTEMPTS:
                if await self.attempt_reconnection(feed_name):
                    return  # Don't alert if reconnection worked
            else:
                logger.error(
                    f"{feed_name} max reconnection attempts reached ({MAX_RECONNECT_ATTEMPTS})"
                )

            await self._send_feed_failure_alert(feed_name, stale_symbols)
        finally:
            self._reconnect_tasks.pop(feed_name, None)

    async def _handle_feed_recovery(self, feed_name: str):
        """Handle feed recovery"""
        logger.info(f"{feed_name} feed recovered")

        # Only send recovery DM if we previously sent a failure alert for this feed.
        # This prevents spurious "everything is healthy" messages when feeds transition
        # back to healthy after spread hour (or any other normal market-hours gap)
        # without having fired a failure alert in the first place.
        if feed_name in self.last_alert_time:
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
        self.stats["reconnections_attempted"] += 1

        logger.info(
            f"Attempting reconnection for {feed_name} (attempt {self.reconnect_attempts[feed_name]})"
        )

        try:
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

            # Reconnect only the stale feed — never tear down healthy feeds.
            success = await self.stream_manager.reconnect_feed(feed_name)

            if success:
                self.stats["reconnections_successful"] += 1
                logger.info(f"{feed_name} reconnection successful")
                return True
            logger.warning(f"{feed_name} reconnection failed")
            return False

        except Exception as e:
            logger.error(f"Error reconnecting {feed_name}: {e}")
            return False

    def _should_send_alert(self, feed_name: str) -> bool:
        """Check if we should send an alert (respects cooldown)"""
        cooldown = timedelta(minutes=ALERT_COOLDOWN_MINUTES)

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
            stale_list = "\n".join(
                [
                    f"• {s['symbol']}: {self._format_duration(s['time_since'])} ago"
                    for s in stale_symbols[:10]  # Limit to 10 symbols
                ]
            )

            if len(stale_symbols) > 10:
                stale_list += f"\n• ... and {len(stale_symbols) - 10} more"

            message = (
                f"⚠️ **{feed_name.upper()} Feed Down**\n\n"
                f"**Affected Symbols:** {len(stale_symbols)}\n"
                f"{stale_list}\n\n"
                f"**Reconnection Attempts:** {self.reconnect_attempts[feed_name]}/{MAX_RECONNECT_ATTEMPTS}\n"
                f"**Status:** {'Failed' if self.reconnect_attempts[feed_name] >= MAX_RECONNECT_ATTEMPTS else 'Retrying'}\n\n"
                f"{'⚠️ Manual intervention may be required' if self.reconnect_attempts[feed_name] >= MAX_RECONNECT_ATTEMPTS else '🔄 Automatic reconnection in progress'}"
            )

            await admin_user.send(message)

            self.last_alert_time[feed_name] = datetime.now()
            self.stats["alerts_sent"] += 1

            logger.info(f"Sent failure alert to admin for {feed_name}")

        except Exception as e:
            logger.error(f"Failed to send admin alert: {e}")

    async def _send_feed_recovery_alert(self, feed_name: str):
        """Send admin DM alert for feed recovery"""
        if not self.admin_user_id:
            return

        try:
            admin_user = await self.bot.fetch_user(self.admin_user_id)

            # Downtime is measured from when the feed actually went stale (the
            # oldest stale-symbol last_update captured at down-detection time),
            # not from when the alert was sent.
            downtime = ""
            stall_started = self.first_stale_time.get(feed_name)
            if stall_started:
                duration = datetime.now() - stall_started
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
        """Return True if the market is expected to be open (used to avoid false stale alerts)."""
        now = datetime.now(self.est)

        if asset_class == "forex_jpy":
            asset_class = "forex"

        market_config = MARKET_HOURS.get(asset_class)

        if not market_config:
            logger.warning(f"Unknown asset class: {asset_class}, assuming market open")
            return True

        if market_config.get("always_open"):
            return True

        if now.weekday() not in market_config.get("days", []):
            return False

        if asset_class == "stocks":
            today_str = now.strftime("%Y-%m-%d")
            if today_str in self.us_market_holidays:
                return False

        # Spread hour (5–6 PM EST) is an expected-quiet window for forex, metals,
        # and indices. Liquidity drops and price ticks slow or stop — treating it
        # as "open" would generate false-positive stale-feed alerts. Mirror the
        # weekend skip above and treat it as closed.
        if asset_class in ("forex", "metals", "indices", "oil"):
            if _SPREAD_START <= now.time() < _SPREAD_END:
                return False

        open_time = datetime.strptime(market_config["open_time"], "%H:%M").time()
        close_time = datetime.strptime(market_config["close_time"], "%H:%M").time()

        if close_time < open_time:
            return now.time() >= open_time or now.time() < close_time
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

    async def _write_feed_health(
        self, feed_name: str, status: str, stale_seconds, last_seen_ts
    ) -> None:
        if self.db is None:
            return
        try:
            await self.db.execute(
                """
                INSERT INTO feed_health (feed, status, stale_seconds, last_seen, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (feed) DO UPDATE SET
                    status        = EXCLUDED.status,
                    stale_seconds = EXCLUDED.stale_seconds,
                    last_seen     = EXCLUDED.last_seen,
                    updated_at    = EXCLUDED.updated_at
                """,
                (feed_name, status, stale_seconds, last_seen_ts),
            )
        except Exception as e:
            logger.error("Failed to write feed_health for %s: %s", feed_name, e)

    def get_health_stats(self) -> Dict:
        """
        Get health monitoring statistics

        Returns:
            Dictionary with health stats
        """
        now = datetime.now()

        feed_details = {}
        for feed_name in ["icmarkets", "oanda", "binance", "exness"]:
            feed_symbols = self.last_seen.get(feed_name, {})

            if feed_symbols:
                oldest_update = min(feed_symbols.values())
                newest_update = max(feed_symbols.values())

                feed_details[feed_name] = {
                    "status": self.feed_status.get(feed_name, "unknown"),
                    "symbols_monitored": len(feed_symbols),
                    "oldest_update": self._format_duration(now - oldest_update),
                    "newest_update": self._format_duration(now - newest_update),
                    "reconnect_attempts": self.reconnect_attempts.get(feed_name, 0),
                }
            else:
                feed_details[feed_name] = {"status": "idle", "symbols_monitored": 0}

        return {
            "overall_stats": self.stats,
            "feed_details": feed_details,
            "monitoring_running": self.running,
            "uptime": self._format_duration(now - self.startup_time),
            "admin_configured": self.admin_user_id is not None,
        }

    def get_feed_status_summary(self) -> str:
        """Get a formatted summary of feed status"""
        stats = self.get_health_stats()

        lines = ["**Feed Health Status**", ""]

        for feed_name, details in stats["feed_details"].items():
            status_emoji = {
                "healthy": "✅",
                "down": "❌",
                "idle": "⏸️",
                "unknown": "❓",
            }.get(details["status"], "❓")

            lines.append(f"{status_emoji} **{feed_name.upper()}**: {details['status']}")

            if details["symbols_monitored"] > 0:
                lines.append(f"   • Symbols: {details['symbols_monitored']}")
                lines.append(f"   • Last update: {details['newest_update']} ago")

                if details["reconnect_attempts"] > 0:
                    lines.append(f"   • Reconnect attempts: {details['reconnect_attempts']}")

            lines.append("")

        lines.append(
            f"**Monitoring Status:** {'Running' if stats['monitoring_running'] else 'Stopped'}"
        )
        lines.append(f"**Uptime:** {stats['uptime']}")

        return "\n".join(lines)
