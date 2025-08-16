"""
Direct MT5 test to verify prices and compare with cache
Run this to see what MT5 is actually returning
"""

import asyncio
import MetaTrader5 as mt5
from datetime import datetime
from price_feeds.smart_cache import SmartPriceCache, Priority
from price_feeds.feeds.icmarkets import ICMarketsFeed


async def test_mt5_direct():
    """Test MT5 directly and compare with feed/cache"""

    print("\n" + "=" * 50)
    print("DIRECT MT5 TEST")
    print("=" * 50)

    # Initialize MT5
    if not mt5.initialize():
        print("Failed to initialize MT5")
        return

    symbol = "EURUSD"

    # Get direct from MT5
    print(f"\n1. Direct MT5 fetch for {symbol}:")
    tick = mt5.symbol_info_tick(symbol)
    if tick:
        print(f"   Bid: {tick.bid:.5f}")
        print(f"   Ask: {tick.ask:.5f}")
        print(f"   Spread: {(tick.ask - tick.bid) * 10000:.1f} pips")
        print(f"   Time: {datetime.fromtimestamp(tick.time)}")
    else:
        print(f"   Failed to get tick for {symbol}")

    # Now test with our feed
    print(f"\n2. Using ICMarketsFeed:")
    feed = ICMarketsFeed()
    await feed.connect()

    # Disable cache first
    await feed.set_cache_enabled(False)
    price = await feed.get_price(symbol)
    if price:
        print(f"   Without cache:")
        print(f"   Bid: {price['bid']:.5f}")
        print(f"   Ask: {price['ask']:.5f}")
        print(f"   Match MT5: {price['bid'] == tick.bid and price['ask'] == tick.ask}")

    # Enable cache
    await feed.set_cache_enabled(True)

    # First fetch (should cache)
    price1 = await feed.get_price(symbol, Priority.CRITICAL)
    print(f"\n3. First cached fetch:")
    print(f"   Bid: {price1['bid']:.5f}")
    print(f"   Ask: {price1['ask']:.5f}")

    # Wait a moment
    await asyncio.sleep(0.5)

    # Second fetch (should be from cache)
    price2 = await feed.get_price(symbol, Priority.CRITICAL)
    print(f"\n4. Second fetch (from cache):")
    print(f"   Bid: {price2['bid']:.5f}")
    print(f"   Ask: {price2['ask']:.5f}")
    print(f"   Same as first: {price1['bid'] == price2['bid'] and price1['ask'] == price2['ask']}")

    # Get cache stats
    stats = feed.get_cache_stats()
    print(f"\n5. Cache stats:")
    print(f"   {stats}")

    # Check the actual cache entry
    cached = await feed.cache.get_price(symbol, Priority.CRITICAL)
    if cached:
        print(f"\n6. Direct cache check:")
        print(f"   Bid: {cached['bid']:.5f}")
        print(f"   Ask: {cached['ask']:.5f}")

    await feed.disconnect()
    mt5.shutdown()

    print("\n" + "=" * 50)
    print("TEST COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(test_mt5_direct())