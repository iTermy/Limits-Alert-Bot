"""
Test script for the price monitoring system
Run this to verify monitor functionality before full bot integration
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import db, signal_db
from price_feeds.monitor import PriceMonitor
from price_feeds.feeds.icmarkets import ICMarketsFeed
from price_feeds.smart_cache import SmartPriceCache, Priority

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockBot:
    """Mock bot for testing without Discord"""
    def get_channel(self, channel_id):
        return MockChannel(channel_id)


class MockChannel:
    """Mock channel for testing"""
    def __init__(self, channel_id):
        self.id = channel_id
        self.name = f"mock-channel-{channel_id}"

    async def send(self, content=None, embed=None):
        """Mock send that just logs"""
        if embed:
            logger.info(f"ALERT: {embed.title} - {embed.description}")
            for field in embed.fields:
                logger.info(f"  {field.name}: {field.value}")
        else:
            logger.info(f"MESSAGE: {content}")


async def test_monitor_setup():
    """Test basic monitor setup and connections"""
    print("\n" + "="*50)
    print("TEST 1: Monitor Setup and Feed Connection")
    print("="*50)

    # Initialize database
    await db.initialize()
    print("✅ Database initialized")

    # Create mock bot
    mock_bot = MockBot()

    # Create monitor
    monitor = PriceMonitor(mock_bot, signal_db, db)
    print("✅ Monitor created")

    # Initialize monitor (connects to feed)
    await monitor.initialize()
    print("✅ Monitor initialized and connected to ICMarkets")

    # Test feed connection
    feed_connected = monitor.feed.connected
    print(f"✅ Feed connected: {feed_connected}")

    return monitor


async def test_signal_monitoring(monitor):
    """Test monitoring for active signals"""
    print("\n" + "="*50)
    print("TEST 2: Active Signal Monitoring")
    print("="*50)

    # Get active signals
    signals = await db.get_active_signals_for_tracking()
    print(f"Found {len(signals)} active/hit signals")

    if not signals:
        print("⚠️ No active signals to monitor")
        return

    # Show first few signals
    for signal in signals[:3]:
        print(f"\nSignal #{signal['signal_id']}:")
        print(f"  Instrument: {signal['instrument']}")
        print(f"  Direction: {signal['direction']}")
        print(f"  Status: {signal['status']}")
        print(f"  Pending limits: {len(signal['pending_limits'])}")

        # Calculate priority
        priority = await monitor.calculate_signal_priority(signal)
        print(f"  Priority: {priority.name}")

        # Get price
        price = await monitor.feed.get_price(signal['instrument'], priority)
        if price:
            print(f"  Current price: Bid={price['bid']:.5f}, Ask={price['ask']:.5f}")

            # Check signal
            await monitor.check_signal(signal, price)
            print(f"  ✅ Signal checked")
        else:
            print(f"  ❌ Failed to get price")


async def test_monitoring_loop(monitor):
    """Test the monitoring loop for a few seconds"""
    print("\n" + "="*50)
    print("TEST 3: Monitoring Loop (5 seconds)")
    print("="*50)

    # Start monitoring
    await monitor.start()
    print("Monitor started...")

    # Let it run for 5 seconds
    for i in range(5):
        await asyncio.sleep(1)
        stats = monitor.get_stats()
        print(f"Second {i+1}: Checks={stats['checks_performed']}, "
              f"Alerts={stats['alerts_sent']}, "
              f"Errors={stats['errors']}, "
              f"Loop time={stats['last_loop_time']:.3f}s")

    # Stop monitoring
    await monitor.stop()
    print("Monitor stopped")

    # Final stats
    stats = monitor.get_stats()
    print("\nFinal Statistics:")
    print(f"  Total checks: {stats['checks_performed']}")
    print(f"  Alerts sent: {stats['alerts_sent']}")
    print(f"  Limits hit: {stats['limits_hit']}")
    print(f"  Errors: {stats['errors']}")

    # Cache stats
    cache_stats = stats.get('cache_stats', {})
    if cache_stats:
        print("\nCache Performance:")
        print(f"  Hit rate: {cache_stats.get('overall_hit_rate', 0):.1%}")
        print(f"  Total entries: {cache_stats.get('total_entries', 0)}")


async def test_specific_signal(monitor, signal_id: int):
    """Test monitoring for a specific signal"""
    print("\n" + "="*50)
    print(f"TEST 4: Specific Signal Test (ID: {signal_id})")
    print("="*50)

    try:
        await monitor.test_signal_monitoring(signal_id)
        print(f"✅ Test completed for signal #{signal_id}")
    except Exception as e:
        print(f"❌ Test failed: {e}")


async def test_priority_calculation(monitor):
    """Test priority calculation for different distances"""
    print("\n" + "="*50)
    print("TEST 5: Priority Calculation")
    print("="*50)

    # Create test signals with different distances
    test_cases = [
        {'distance_pips': 5, 'expected': 'CRITICAL'},
        {'distance_pips': 25, 'expected': 'MEDIUM'},
        {'distance_pips': 100, 'expected': 'LOW'},
    ]

    for test in test_cases:
        # Create mock signal
        mock_signal = {
            'signal_id': 999,
            'instrument': 'EURUSD',
            'direction': 'long',
            'status': 'active',
            'stop_loss': 1.0500,
            'limits_hit': 0,
            'total_limits': 3,
            'pending_limits': [
                {
                    'limit_id': 1,
                    'price_level': 1.0800,  # Will be adjusted based on distance
                    'sequence_number': 1,
                    'approaching_alert_sent': False,
                    'hit_alert_sent': False
                }
            ]
        }

        # Set up cache with a fake price
        current_price = 1.0850
        await monitor.cache.update_price(
            'EURUSD',
            bid=current_price,
            ask=current_price + 0.0001,
            timestamp=asyncio.get_event_loop().time(),
            priority=Priority.LOW
        )

        # Adjust limit price based on test distance
        pip_size = 0.0001
        mock_signal['pending_limits'][0]['price_level'] = current_price - (test['distance_pips'] * pip_size)

        # Calculate priority
        priority = await monitor.calculate_signal_priority(mock_signal)

        result = "✅" if priority.name == test['expected'] else "❌"
        print(f"{result} Distance: {test['distance_pips']} pips → Priority: {priority.name} (expected: {test['expected']})")


async def test_alert_deduplication(monitor):
    """Test that alerts aren't sent twice"""
    print("\n" + "="*50)
    print("TEST 6: Alert Deduplication")
    print("="*50)

    # Get an active signal
    signals = await db.get_active_signals_for_tracking()
    if not signals:
        print("⚠️ No active signals to test")
        return

    signal = signals[0]
    print(f"Testing with signal #{signal['signal_id']} ({signal['instrument']})")

    # Get initial alert counts
    initial_stats = monitor.get_stats()
    initial_alerts = initial_stats['alerts_sent']

    # Process the signal twice
    price = await monitor.feed.get_price(signal['instrument'], Priority.CRITICAL)
    if price:
        # First check
        await monitor.check_signal(signal, price)
        first_check_alerts = monitor.get_stats()['alerts_sent']

        # Second check (should not send duplicate alerts)
        await monitor.check_signal(signal, price)
        second_check_alerts = monitor.get_stats()['alerts_sent']

        if first_check_alerts == second_check_alerts:
            print(f"✅ No duplicate alerts sent")
        else:
            print(f"❌ Duplicate alert detected!")

        print(f"  Alerts after first check: {first_check_alerts - initial_alerts}")
        print(f"  Alerts after second check: {second_check_alerts - initial_alerts}")
    else:
        print("❌ Failed to get price for testing")


async def test_stop_loss_monitoring(monitor):
    """Test stop loss detection"""
    print("\n" + "="*50)
    print("TEST 7: Stop Loss Monitoring")
    print("="*50)

    # Find a signal with stop loss
    signals = await db.get_active_signals_for_tracking()
    test_signal = None

    for signal in signals:
        if signal.get('stop_loss'):
            test_signal = signal
            break

    if not test_signal:
        print("⚠️ No signals with stop loss to test")
        return

    print(f"Testing signal #{test_signal['signal_id']} ({test_signal['instrument']})")
    print(f"  Direction: {test_signal['direction']}")
    print(f"  Stop Loss: {test_signal['stop_loss']:.5f}")

    # Get current price
    price = await monitor.feed.get_price(test_signal['instrument'], Priority.CRITICAL)
    if price:
        current = price['ask'] if test_signal['direction'] == 'long' else price['bid']
        print(f"  Current Price: {current:.5f}")

        # Calculate distance to stop loss
        if test_signal['direction'] == 'long':
            distance = current - test_signal['stop_loss']
            would_hit = current <= test_signal['stop_loss']
        else:
            distance = test_signal['stop_loss'] - current
            would_hit = current >= test_signal['stop_loss']

        print(f"  Distance to SL: {distance:.5f}")
        print(f"  Would trigger: {'Yes' if would_hit else 'No'}")

        # Check the signal
        await monitor.check_signal(test_signal, price)
        print("✅ Stop loss check completed")
    else:
        print("❌ Failed to get price")


async def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("DISCORD TRADING BOT - PRICE MONITOR INTEGRATION TEST")
    print("="*60)

    try:
        # Test 1: Setup
        monitor = await test_monitor_setup()

        # Test 2: Signal monitoring
        await test_signal_monitoring(monitor)

        # Test 3: Monitoring loop
        await test_monitoring_loop(monitor)

        # Test 4: Specific signal (if you have a signal ID to test)
        # Uncomment and set a real signal ID to test:
        # await test_specific_signal(monitor, 123)

        # Test 5: Priority calculation
        await test_priority_calculation(monitor)

        # Test 6: Alert deduplication
        await test_alert_deduplication(monitor)

        # Test 7: Stop loss monitoring
        await test_stop_loss_monitoring(monitor)

        print("\n" + "="*60)
        print("ALL TESTS COMPLETED")
        print("="*60)

        # Summary
        final_stats = monitor.get_stats()
        print("\nFinal Summary:")
        print(f"  Total checks performed: {final_stats['checks_performed']}")
        print(f"  Total alerts sent: {final_stats['alerts_sent']}")
        print(f"  Total limits hit: {final_stats['limits_hit']}")
        print(f"  Total errors: {final_stats['errors']}")

        if final_stats['errors'] > 0:
            print("\n⚠️ Some errors occurred during testing. Check logs for details.")
        else:
            print("\n✅ No errors detected during testing!")

    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        logger.error("Test suite error", exc_info=True)
    finally:
        # Cleanup
        if 'monitor' in locals():
            if monitor.running:
                await monitor.stop()
            if monitor.feed and monitor.feed.connected:
                await monitor.feed.disconnect()
        print("\nCleanup completed")


if __name__ == "__main__":
    asyncio.run(main())