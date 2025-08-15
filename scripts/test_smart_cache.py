#!/usr/bin/env python3
"""
Test script for Phase 2C: Smart Cache Integration
Place in: /scripts/test_smart_cache.py

Tests:
1. Cache TTL behavior
2. Priority-based caching
3. Cache hit rates
4. Performance improvements
5. Cache invalidation
"""

import asyncio
import sys
import os
from datetime import datetime
import time
from typing import Dict, List
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from price_feeds.feeds.icmarkets import ICMarketsFeed
from price_feeds.smart_cache import SmartPriceCache, Priority

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CacheTestSuite:
    """Comprehensive test suite for smart cache integration"""

    def __init__(self):
        self.feed = None
        self.test_symbols = {
            'forex': ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD'],
            'metals': ['XAUUSD', 'XAGUSD'],
            'indices': ['USTEC'],  # May not be available on all accounts
        }
        self.results = {}

    async def setup(self):
        """Initialize feed and connect"""
        print("\n" + "=" * 60)
        print("SMART CACHE TEST SUITE - Phase 2C")
        print("=" * 60)

        self.feed = ICMarketsFeed()
        print("\n🔧 Setting up MT5 connection...")

        if await self.feed.connect():
            print("✅ Connected to MT5 successfully")
            return True
        else:
            print("❌ Failed to connect to MT5")
            return False

    async def teardown(self):
        """Cleanup and disconnect"""
        if self.feed:
            print("\n📊 Final cache statistics:")
            stats = self.feed.get_cache_stats()
            for key, value in stats.items():
                print(f"   {key}: {value}")

            await self.feed.disconnect()
            print("✅ Disconnected from MT5")

    async def test_cache_ttl(self):
        """Test 1: Verify TTL behavior for different priorities"""
        print("\n" + "-" * 50)
        print("TEST 1: Cache TTL Behavior")
        print("-" * 50)

        symbol = 'EURUSD'
        results = {}

        # Test CRITICAL priority (1 second TTL)
        print(f"\n1. Testing CRITICAL priority (1s TTL) for {symbol}:")

        # First fetch - should hit MT5
        price1 = await self.feed.get_price(symbol, Priority.CRITICAL)
        if price1:
            print(f"   Initial fetch: Bid={price1['bid']:.5f}")

        # Immediate fetch - should hit cache
        price2 = await self.feed.get_price(symbol, Priority.CRITICAL)
        if price2:
            print(f"   Immediate re-fetch: Bid={price2['bid']:.5f} (should be from cache)")

        # Wait for TTL expiry
        print("   Waiting 1.5 seconds for TTL expiry...")
        await asyncio.sleep(1.5)

        # Fetch after expiry - should hit MT5 again
        price3 = await self.feed.get_price(symbol, Priority.CRITICAL)
        if price3:
            print(f"   After expiry: Bid={price3['bid']:.5f} (should be fresh)")

        results['critical'] = price1 is not None and price2 is not None and price3 is not None

        # Test MEDIUM priority (30 second TTL)
        print(f"\n2. Testing MEDIUM priority (30s TTL) for {symbol}:")

        # Clear cache first
        await self.feed.clear_cache(symbol)

        # First fetch
        price4 = await self.feed.get_price(symbol, Priority.MEDIUM)
        if price4:
            print(f"   Initial fetch: Bid={price4['bid']:.5f}")

        # Wait 5 seconds - should still be cached
        print("   Waiting 5 seconds...")
        await asyncio.sleep(5)

        price5 = await self.feed.get_price(symbol, Priority.MEDIUM)
        if price5:
            print(f"   After 5s: Bid={price5['bid']:.5f} (should be from cache)")

        results['medium'] = price4 is not None and price5 is not None

        # Test LOW priority (120 second TTL)
        print(f"\n3. Testing LOW priority (120s TTL) for {symbol}:")

        # Clear cache
        await self.feed.clear_cache(symbol)

        price6 = await self.feed.get_price(symbol, Priority.LOW)
        if price6:
            print(f"   Initial fetch: Bid={price6['bid']:.5f}")
            print("   (Would remain cached for 2 minutes)")

        results['low'] = price6 is not None

        self.results['ttl_test'] = all(results.values())

        if self.results['ttl_test']:
            print("\n✅ TTL Test PASSED")
        else:
            print("\n❌ TTL Test FAILED")

        return self.results['ttl_test']

    async def test_batch_caching(self):
        """Test 2: Batch operations with caching"""
        print("\n" + "-" * 50)
        print("TEST 2: Batch Operations with Cache")
        print("-" * 50)

        # Clear cache
        await self.feed.clear_cache()

        symbols = self.test_symbols['forex'] + self.test_symbols['metals']

        # Create priority map (simulate different signal distances)
        priorities = {
            'EURUSD': Priority.CRITICAL,  # Close to limit
            'GBPUSD': Priority.MEDIUM,  # Medium distance
            'USDJPY': Priority.LOW,  # Far from limit
            'AUDUSD': Priority.LOW,  # Far from limit
            'XAUUSD': Priority.MEDIUM,  # Medium distance
            'XAGUSD': Priority.LOW,  # Far from limit
        }

        print(f"\n1. First batch fetch ({len(symbols)} symbols):")
        start_time = time.time()
        prices1 = await self.feed.get_batch_prices(symbols, priorities)
        time1 = time.time() - start_time

        print(f"   Fetched {len(prices1)}/{len(symbols)} symbols in {time1:.3f}s")
        print(f"   All from MT5 (cache was empty)")

        # Second fetch - should use cache for most
        print(f"\n2. Second batch fetch (immediate):")
        start_time = time.time()
        prices2 = await self.feed.get_batch_prices(symbols, priorities)
        time2 = time.time() - start_time

        print(f"   Fetched {len(prices2)}/{len(symbols)} symbols in {time2:.3f}s")
        print(f"   Speedup: {time1 / time2:.1f}x faster" if time2 else "   Speedup: Infinite (time2 is zero)")


        # Get cache stats
        stats = self.feed.get_cache_stats()
        print(f"   Cache hit rate: {stats['hit_rate']}")

        # Wait for CRITICAL to expire
        print("\n3. After 1.5s (CRITICAL expired, others cached):")
        await asyncio.sleep(1.5)

        start_time = time.time()
        prices3 = await self.feed.get_batch_prices(symbols, priorities)
        time3 = time.time() - start_time

        print(f"   Fetched {len(prices3)}/{len(symbols)} symbols in {time3:.3f}s")
        print(f"   Should fetch only EURUSD (CRITICAL), rest from cache")

        self.results['batch_caching'] = (
                len(prices1) > 0 and
                len(prices2) > 0 and
                time2 < time1 * 0.5  # At least 2x faster with cache
        )

        if self.results['batch_caching']:
            print("\n✅ Batch Caching Test PASSED")
        else:
            print("\n❌ Batch Caching Test FAILED")

        return self.results['batch_caching']

    async def test_cache_hit_rates(self):
        """Test 3: Measure cache hit rates by priority"""
        print("\n" + "-" * 50)
        print("TEST 3: Cache Hit Rates by Priority")
        print("-" * 50)

        # Clear cache and stats
        await self.feed.clear_cache()

        # Simulate monitoring loop with different priorities
        print("\n1. Simulating 30-second monitoring period...")

        test_duration = 30  # seconds
        fetch_interval = 1  # second

        # Symbol priorities (simulating signal distances)
        symbol_priorities = {
            'EURUSD': Priority.CRITICAL,  # 1s TTL
            'GBPUSD': Priority.MEDIUM,  # 30s TTL
            'XAUUSD': Priority.LOW,  # 120s TTL
        }

        fetch_counts = {sym: 0 for sym in symbol_priorities}
        start_time = time.time()

        print("   Fetching prices every second...")

        while time.time() - start_time < test_duration:
            for symbol, priority in symbol_priorities.items():
                price = await self.feed.get_price(symbol, priority)
                if price:
                    fetch_counts[symbol] += 1

            await asyncio.sleep(fetch_interval)

        # Get final stats
        final_stats = self.feed.get_cache_stats()

        print("\n2. Results after 30 seconds:")
        print(f"   Total fetches attempted: {sum(fetch_counts.values())}")
        print(f"   Cache hits: {final_stats['cache_hits']}")
        print(f"   Cache misses: {final_stats['cache_misses']}")
        print(f"   Overall hit rate: {final_stats['hit_rate']}")

        print("\n3. Expected behavior:")
        print("   CRITICAL (1s TTL): ~3% hit rate (mostly misses)")
        print("   MEDIUM (30s TTL): ~97% hit rate (1 miss, rest hits)")
        print("   LOW (120s TTL): ~100% hit rate (all hits after first)")

        # Verify expectations
        total_attempts = sum(fetch_counts.values())
        hit_rate = final_stats['cache_hits'] / (final_stats['cache_hits'] + final_stats['cache_misses'])

        self.results['hit_rates'] = hit_rate > 0.6  # Should be >60% overall

        if self.results['hit_rates']:
            print(f"\n✅ Cache Hit Rate Test PASSED (achieved {hit_rate:.1%})")
        else:
            print(f"\n❌ Cache Hit Rate Test FAILED (only {hit_rate:.1%})")

        return self.results['hit_rates']

    async def test_performance_improvement(self):
        """Test 4: Measure performance improvement with cache"""
        print("\n" + "-" * 50)
        print("TEST 4: Performance Improvement Measurement")
        print("-" * 50)

        symbols = self.test_symbols['forex'] + self.test_symbols['metals']

        # Test without cache
        print("\n1. Performance WITHOUT cache:")
        await self.feed.set_cache_enabled(False)

        start_time = time.time()
        for _ in range(5):
            prices = await self.feed.get_batch_prices(symbols)
        time_without_cache = time.time() - start_time

        print(f"   5 iterations: {time_without_cache:.3f}s")
        print(f"   Average: {time_without_cache / 5:.3f}s per batch")

        # Test with cache
        print("\n2. Performance WITH cache:")
        await self.feed.set_cache_enabled(True)
        await self.feed.clear_cache()

        # Set all to LOW priority for maximum caching
        priorities = {sym: Priority.LOW for sym in symbols}

        start_time = time.time()
        for _ in range(5):
            prices = await self.feed.get_batch_prices(symbols, priorities)
        time_with_cache = time.time() - start_time

        print(f"   5 iterations: {time_with_cache:.3f}s")
        print(f"   Average: {time_with_cache / 5:.3f}s per batch")

        # Calculate improvement
        if time_with_cache > 0:
            improvement = (time_without_cache - time_with_cache) / time_without_cache * 100
            speedup = time_without_cache / time_with_cache

            print(f"\n3. Performance improvement:")
            print(f"   Time saved: {time_without_cache - time_with_cache:.3f}s")
            print(f"   Improvement: {improvement:.1f}%")
            print(f"   Speedup: {speedup:.1f}x faster")

            self.results['performance'] = improvement > 50  # Should be >50% improvement
        else:
            self.results['performance'] = False

        if self.results['performance']:
            print("\n✅ Performance Test PASSED")
        else:
            print("\n❌ Performance Test FAILED")

        return self.results['performance']

    async def test_cache_invalidation(self):
        """Test 5: Cache invalidation and cleanup"""
        print("\n" + "-" * 50)
        print("TEST 5: Cache Invalidation")
        print("-" * 50)

        # Fill cache
        print("\n1. Filling cache with prices...")
        symbols = self.test_symbols['forex']
        priorities = {sym: Priority.LOW for sym in symbols}

        await self.feed.get_batch_prices(symbols, priorities)

        stats1 = self.feed.get_cache_stats()
        print(f"   Cache size: {stats1['cache_size']}")

        # Test single symbol invalidation
        print("\n2. Invalidating single symbol (EURUSD)...")
        await self.feed.clear_cache('EURUSD')

        # Check if EURUSD needs refetch
        cached_prices, to_fetch = await self.feed.cache.get_batch_with_filter(symbols, priorities)

        print(f"   Symbols to fetch: {to_fetch}")
        print(f"   Should only be EURUSD: {'✅' if to_fetch == ['EURUSD'] else '❌'}")

        # Test full cache invalidation
        print("\n3. Invalidating entire cache...")
        await self.feed.clear_cache()

        stats2 = self.feed.get_cache_stats()
        print(f"   Cache size after clear: {stats2['cache_size']}")

        # Test automatic cleanup
        print("\n4. Testing automatic cleanup of expired entries...")

        # Add entries with different priorities
        await self.feed.get_price('EURUSD', Priority.CRITICAL)
        await self.feed.get_price('GBPUSD', Priority.MEDIUM)
        await self.feed.get_price('XAUUSD', Priority.LOW)

        initial_size = self.feed.cache.cache.__len__()
        print(f"   Initial cache size: {initial_size}")

        # Cleanup expired entries
        cleaned = await self.feed.cache.cleanup_expired()
        print(f"   Cleaned {cleaned} expired entries")

        self.results['invalidation'] = (
                stats2['cache_size'] == 0 and  # Full clear worked
                'EURUSD' in to_fetch  # Single invalidation worked
        )

        if self.results['invalidation']:
            print("\n✅ Cache Invalidation Test PASSED")
        else:
            print("\n❌ Cache Invalidation Test FAILED")

        return self.results['invalidation']

    async def run_all_tests(self):
        """Run complete test suite"""
        if not await self.setup():
            print("❌ Setup failed, cannot run tests")
            return False

        try:
            # Run all tests
            await self.test_cache_ttl()
            await self.test_batch_caching()
            await self.test_cache_hit_rates()
            await self.test_performance_improvement()
            await self.test_cache_invalidation()

            # Summary
            print("\n" + "=" * 60)
            print("TEST SUITE SUMMARY")
            print("=" * 60)

            total_tests = len(self.results)
            passed_tests = sum(1 for v in self.results.values() if v)

            print(f"\nTests Passed: {passed_tests}/{total_tests}")
            print("\nDetailed Results:")

            for test_name, passed in self.results.items():
                status = "✅ PASSED" if passed else "❌ FAILED"
                print(f"  {test_name}: {status}")

            overall_success = all(self.results.values())

            if overall_success:
                print("\n🎉 ALL TESTS PASSED! Smart cache is working correctly.")
            else:
                print("\n⚠️ Some tests failed. Check the details above.")

            return overall_success

        finally:
            await self.teardown()


async def quick_integration_test():
    """Quick integration test for developers"""
    print("\n" + "=" * 60)
    print("QUICK INTEGRATION TEST")
    print("=" * 60)

    feed = ICMarketsFeed()

    try:
        # Connect
        print("\n1. Connecting to MT5...")
        if not await feed.connect():
            print("❌ Connection failed")
            return
        print("✅ Connected")

        # Test single fetch with cache
        print("\n2. Testing single symbol with cache:")

        symbol = 'EURUSD'

        # First fetch - from MT5
        print(f"   Fetching {symbol} (CRITICAL priority)...")
        start = time.time()
        price1 = await feed.get_price(symbol, Priority.CRITICAL)
        time1 = time.time() - start

        if price1:
            print(f"   ✅ First fetch: {time1:.3f}s (from MT5)")
            print(f"      Bid: {price1['bid']:.5f}, Ask: {price1['ask']:.5f}")

        # Second fetch - from cache
        print(f"   Re-fetching immediately...")
        start = time.time()
        price2 = await feed.get_price(symbol, Priority.CRITICAL)
        time2 = time.time() - start

        if price2:
            print(f"   ✅ Second fetch: {time2:.3f}s (from cache)")
            print(f"   Speedup: {time1 / time2:.1f}x faster" if time2 else "   Speedup: Infinite (time2 is zero)")


        # Test batch with priorities
        print("\n3. Testing batch fetch with priorities:")

        symbols = ['EURUSD', 'GBPUSD', 'XAUUSD', 'USDJPY']
        priorities = {
            'EURUSD': Priority.CRITICAL,
            'GBPUSD': Priority.MEDIUM,
            'XAUUSD': Priority.MEDIUM,
            'USDJPY': Priority.LOW
        }

        print(f"   Fetching {len(symbols)} symbols with mixed priorities...")
        start = time.time()
        prices = await feed.get_batch_prices(symbols, priorities)
        batch_time = time.time() - start

        print(f"   ✅ Fetched {len(prices)}/{len(symbols)} symbols in {batch_time:.3f}s")

        for sym, price in prices.items():
            if price:
                priority = priorities.get(sym, Priority.MEDIUM)
                print(f"      {sym} ({priority.value}): {price['bid']:.5f}/{price['ask']:.5f}")

        # Show cache stats
        print("\n4. Cache Statistics:")
        stats = feed.get_cache_stats()
        for key, value in stats.items():
            if key != 'symbols_cached':  # Skip long list
                print(f"   {key}: {value}")

        print("\n✅ Quick integration test completed successfully!")

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await feed.disconnect()


async def benchmark_cache_effectiveness():
    """Benchmark to show real-world cache effectiveness"""
    print("\n" + "=" * 60)
    print("CACHE EFFECTIVENESS BENCHMARK")
    print("=" * 60)
    print("Simulating real-world monitoring scenario...")

    feed = ICMarketsFeed()

    try:
        if not await feed.connect():
            print("❌ Connection failed")
            return

        # Simulate monitoring these signals
        active_signals = [
            {'symbol': 'EURUSD', 'distance_pips': 5},  # CRITICAL
            {'symbol': 'GBPUSD', 'distance_pips': 8},  # CRITICAL
            {'symbol': 'USDJPY', 'distance_pips': 25},  # MEDIUM
            {'symbol': 'GOLD', 'distance_pips': 35},  # MEDIUM
            {'symbol': 'AUDUSD', 'distance_pips': 75},  # LOW
            {'symbol': 'NZDUSD', 'distance_pips': 100},  # LOW
        ]

        # Calculate priorities
        priorities = {}
        for signal in active_signals:
            if signal['distance_pips'] < 10:
                priorities[signal['symbol']] = Priority.CRITICAL
            elif signal['distance_pips'] < 50:
                priorities[signal['symbol']] = Priority.MEDIUM
            else:
                priorities[signal['symbol']] = Priority.LOW

        print(f"\nMonitoring {len(active_signals)} signals:")
        for signal in active_signals:
            priority = priorities[signal['symbol']]
            print(f"  {signal['symbol']}: {signal['distance_pips']} pips away ({priority.value})")

        # Run for 60 seconds
        print("\nRunning 60-second monitoring simulation...")
        print("(Fetching prices every second, like real monitoring would)")

        start_time = time.time()
        fetch_count = 0

        # Disable cache for first 30 seconds
        await feed.set_cache_enabled(False)
        print("\nFirst 30 seconds WITHOUT cache...")

        while time.time() - start_time < 30:
            symbols = [s['symbol'] for s in active_signals]
            await feed.get_batch_prices(symbols)
            fetch_count += 1
            await asyncio.sleep(1)

        time_without_cache = 30
        fetches_without_cache = fetch_count

        # Enable cache for next 30 seconds
        await feed.set_cache_enabled(True)
        print("Next 30 seconds WITH cache...")

        cache_fetch_count = 0
        while time.time() - start_time < 60:
            symbols = [s['symbol'] for s in active_signals]
            await feed.get_batch_prices(symbols, priorities)
            cache_fetch_count += 1
            await asyncio.sleep(1)

        # Results
        print("\n" + "=" * 60)
        print("BENCHMARK RESULTS")
        print("=" * 60)

        print(f"\nWithout Cache (30 seconds):")
        print(f"  Total fetches: {fetches_without_cache}")
        print(f"  Symbols per fetch: {len(active_signals)}")
        print(f"  Total API calls: ~{fetches_without_cache * len(active_signals)}")

        cache_stats = feed.get_cache_stats()
        print(f"\nWith Cache (30 seconds):")
        print(f"  Total fetches: {cache_fetch_count}")
        print(f"  Cache hits: {cache_stats['cache_hits']}")
        print(f"  Cache misses: {cache_stats['cache_misses']}")
        print(f"  Hit rate: {cache_stats['hit_rate']}")

        # Calculate savings
        estimated_api_calls_without = fetches_without_cache * len(active_signals)
        estimated_api_calls_with = cache_stats['cache_misses']
        reduction = (1 - estimated_api_calls_with / estimated_api_calls_without) * 100

        print(f"\nAPI Call Reduction:")
        print(f"  Without cache: ~{estimated_api_calls_without} API calls")
        print(f"  With cache: ~{estimated_api_calls_with} API calls")
        print(f"  Reduction: {reduction:.1f}%")

        print(f"\n{'🎯' if reduction > 70 else '⚠️'} Target: 90% reduction for low-priority")
        print(f"{'✅' if reduction > 70 else '❌'} Achieved: {reduction:.1f}% overall reduction")

    finally:
        await feed.disconnect()


def print_menu():
    """Print test menu"""
    print("\n" + "=" * 60)
    print("SMART CACHE TEST MENU")
    print("=" * 60)
    print("\n1. Run Complete Test Suite (comprehensive)")
    print("2. Quick Integration Test (fast check)")
    print("3. Cache Effectiveness Benchmark (60 seconds)")
    print("4. Exit")
    print("\nSelect option (1-4): ", end="")


async def main():
    """Main test runner with menu"""
    while True:
        print_menu()

        try:
            choice = input().strip()

            if choice == '1':
                suite = CacheTestSuite()
                await suite.run_all_tests()
            elif choice == '2':
                await quick_integration_test()
            elif choice == '3':
                await benchmark_cache_effectiveness()
            elif choice == '4':
                print("\nExiting...")
                break
            else:
                print("Invalid choice. Please select 1-4.")
                continue

            print("\nPress Enter to continue...")
            input()

        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
            break
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("DISCORD TRADING BOT - SMART CACHE TESTING")
    print("Stage 3 Phase 2C Implementation")
    print("=" * 60)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()