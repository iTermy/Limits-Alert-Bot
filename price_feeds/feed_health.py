"""
Lightweight feed health monitoring system
Tracks feed performance and connection health in memory
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import deque
import asyncio

logger = logging.getLogger(__name__)


class FeedHealthMonitor:
    """
    Lightweight in-memory health tracking for price feeds

    Features:
    - Connection status monitoring
    - Performance metrics (latency, success rate)
    - Automatic health checks
    - Feed failure detection
    """

    def __init__(self, check_interval: int = 60):
        """
        Initialize health monitor

        Args:
            check_interval: Seconds between health checks
        """
        self.check_interval = check_interval
        self.feeds = {}  # feed_name -> feed_instance
        self.health_history = {}  # feed_name -> deque of health records
        self.max_history_size = 100
        self.monitoring = False
        self._monitor_task = None

        # Alert thresholds
        self.latency_threshold_ms = 1000  # 1 second
        self.min_success_rate = 90  # 90% success rate required

        logger.info(f"FeedHealthMonitor initialized (check every {check_interval}s)")

    def register_feed(self, feed_name: str, feed_instance):
        """
        Register a feed for monitoring

        Args:
            feed_name: Name of the feed
            feed_instance: Feed instance implementing BaseFeed
        """
        self.feeds[feed_name] = feed_instance
        self.health_history[feed_name] = deque(maxlen=self.max_history_size)
        logger.info(f"Registered feed for monitoring: {feed_name}")

    async def check_feed_health(self, feed_name: str) -> Dict:
        """
        Perform health check on a specific feed

        Args:
            feed_name: Name of the feed to check

        Returns:
            Dict with health check results
        """
        if feed_name not in self.feeds:
            return {'error': f'Feed {feed_name} not registered'}

        feed = self.feeds[feed_name]
        start_time = datetime.now()

        try:
            # Test connection and basic functionality
            success, message = await feed.test_connection()
            latency = (datetime.now() - start_time).total_seconds() * 1000

            # Get feed metrics
            metrics = feed.get_health_status()

            health_record = {
                'timestamp': datetime.now(),
                'connected': success,
                'latency_ms': latency,
                'message': message,
                'metrics': metrics,
                'status': self._determine_status(success, latency, metrics)
            }

            # Store in history
            self.health_history[feed_name].append(health_record)

            return health_record

        except Exception as e:
            logger.error(f"Health check failed for {feed_name}: {e}")
            health_record = {
                'timestamp': datetime.now(),
                'connected': False,
                'latency_ms': None,
                'message': str(e),
                'metrics': feed.get_health_status() if feed else {},
                'status': 'error'
            }
            self.health_history[feed_name].append(health_record)
            return health_record

    def _determine_status(self, connected: bool, latency: float, metrics: Dict) -> str:
        """
        Determine overall health status

        Returns:
            'healthy', 'degraded', or 'unhealthy'
        """
        if not connected:
            return 'unhealthy'

        # Check latency
        if latency > self.latency_threshold_ms:
            return 'degraded'

        # Check success rate
        success_rate_str = metrics.get('fetch_success_rate', '0%')
        try:
            success_rate = float(success_rate_str.rstrip('%'))
            if success_rate < self.min_success_rate:
                return 'degraded'
        except:
            pass

        return 'healthy'

    async def check_all_feeds(self) -> Dict[str, Dict]:
        """
        Check health of all registered feeds

        Returns:
            Dict mapping feed_name to health status
        """
        results = {}

        for feed_name in self.feeds:
            results[feed_name] = await self.check_feed_health(feed_name)

        return results

    async def start_monitoring(self):
        """Start background health monitoring"""
        if self.monitoring:
            logger.warning("Health monitoring already running")
            return

        self.monitoring = True
        self._monitor_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Started health monitoring")

    async def stop_monitoring(self):
        """Stop background health monitoring"""
        self.monitoring = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped health monitoring")

    async def _monitoring_loop(self):
        """Background loop for periodic health checks"""
        while self.monitoring:
            try:
                # Perform health checks
                results = await self.check_all_feeds()

                # Log any unhealthy feeds
                for feed_name, health in results.items():
                    if health.get('status') == 'unhealthy':
                        logger.warning(f"Feed {feed_name} is unhealthy: {health.get('message')}")
                    elif health.get('status') == 'degraded':
                        logger.info(f"Feed {feed_name} is degraded: {health.get('message')}")

                # Wait for next check
                await asyncio.sleep(self.check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(self.check_interval)

    def get_feed_summary(self, feed_name: str) -> Dict:
        """
        Get summary statistics for a feed

        Args:
            feed_name: Name of the feed

        Returns:
            Dict with summary statistics
        """
        if feed_name not in self.health_history:
            return {'error': f'No history for feed {feed_name}'}

        history = list(self.health_history[feed_name])
        if not history:
            return {'error': 'No health records available'}

        # Calculate statistics
        total_checks = len(history)
        successful_checks = sum(1 for h in history if h.get('connected', False))
        recent_history = history[-10:] if len(history) >= 10 else history
        recent_success = sum(1 for h in recent_history if h.get('connected', False))

        # Average latency (excluding failures)
        latencies = [h['latency_ms'] for h in history
                     if h.get('latency_ms') is not None]
        avg_latency = sum(latencies) / len(latencies) if latencies else None

        # Current status
        current_status = history[-1] if history else None

        return {
            'feed_name': feed_name,
            'current_status': current_status.get('status') if current_status else 'unknown',
            'last_check': current_status.get('timestamp') if current_status else None,
            'total_checks': total_checks,
            'success_rate': f"{(successful_checks / total_checks * 100):.1f}%" if total_checks > 0 else "0%",
            'recent_success_rate': f"{(recent_success / len(recent_history) * 100):.1f}%" if recent_history else "0%",
            'avg_latency_ms': f"{avg_latency:.1f}" if avg_latency else "N/A",
            'consecutive_failures': self._count_consecutive_failures(history)
        }

    def _count_consecutive_failures(self, history: List[Dict]) -> int:
        """Count consecutive failures from most recent"""
        count = 0
        for record in reversed(history):
            if not record.get('connected', False):
                count += 1
            else:
                break
        return count

    def get_all_summaries(self) -> Dict[str, Dict]:
        """Get summaries for all feeds"""
        return {
            feed_name: self.get_feed_summary(feed_name)
            for feed_name in self.feeds
        }

    def get_critical_alerts(self) -> List[Dict]:
        """
        Get list of critical issues requiring attention

        Returns:
            List of alert dictionaries
        """
        alerts = []

        for feed_name, feed in self.feeds.items():
            summary = self.get_feed_summary(feed_name)

            # Check for disconnected feeds
            if summary.get('current_status') == 'unhealthy':
                alerts.append({
                    'feed': feed_name,
                    'type': 'disconnected',
                    'message': f"{feed_name} is disconnected",
                    'severity': 'critical'
                })

            # Check for high failure rate
            try:
                recent_rate = float(summary.get('recent_success_rate', '0%').rstrip('%'))
                if recent_rate < 50:
                    alerts.append({
                        'feed': feed_name,
                        'type': 'high_failure_rate',
                        'message': f"{feed_name} success rate: {recent_rate:.1f}%",
                        'severity': 'warning'
                    })
            except:
                pass

            # Check for consecutive failures
            if summary.get('consecutive_failures', 0) >= 5:
                alerts.append({
                    'feed': feed_name,
                    'type': 'consecutive_failures',
                    'message': f"{feed_name} has {summary['consecutive_failures']} consecutive failures",
                    'severity': 'warning'
                })

        return alerts