"""
Debug script to identify GBPJPY symbol issue
Tests different symbol variations and direct MT5 access
"""

import asyncio
import MetaTrader5 as mt5
from datetime import datetime
from price_feeds.feeds.icmarkets import ICMarketsFeed
from price_feeds.smart_cache import SmartPriceCache, Priority


async def debug_gbpjpy():
    """Debug why GBPJPY is returning wrong price"""

    print("\n" + "=" * 60)
    print("GBPJPY SYMBOL DEBUG")
    print("=" * 60)

    # Initialize MT5
    if not mt5.initialize():
        print("Failed to initialize MT5")
        return

    # Test different symbol variations
    test_symbols = [
        "GBPJPY",
        "GBPJPY.",
        "GBPJPY.a",
        "GBPJPYm",
        "GBPJPY_m",
        "GBPJPY.r"
    ]

    print("\n1. Testing symbol variations directly in MT5:")
    print("-" * 50)

    for symbol in test_symbols:
        tick = mt5.symbol_info_tick(symbol)
        if tick:
            print(f"✓ {symbol:12} -> Bid: {tick.bid:.5f}, Ask: {tick.ask:.5f}")
        else:
            print(f"✗ {symbol:12} -> Not found")

    # Check what symbols are available
    print("\n2. Searching for GBP/JPY symbols in MT5:")
    print("-" * 50)

    all_symbols = mt5.symbols_get()
    gbpjpy_symbols = [s.name for s in all_symbols if 'GBPJPY' in s.name.upper()]

    if gbpjpy_symbols:
        print(f"Found {len(gbpjpy_symbols)} GBPJPY symbols:")
        for sym in gbpjpy_symbols[:5]:  # Show first 5
            tick = mt5.symbol_info_tick(sym)
            if tick:
                print(f"  {sym}: Bid={tick.bid:.5f}, Ask={tick.ask:.5f}")
    else:
        print("No GBPJPY symbols found!")

        # Try to find any JPY pairs
        jpy_symbols = [s.name for s in all_symbols if 'JPY' in s.name.upper()]
        print(f"\nFound {len(jpy_symbols)} JPY symbols total:")
        for sym in jpy_symbols[:10]:  # Show first 10
            print(f"  {sym}")

    # Test with our feed
    print("\n3. Testing with ICMarketsFeed:")
    print("-" * 50)

    feed = ICMarketsFeed()
    await feed.connect()

    # Disable cache for fresh data
    await feed.set_cache_enabled(False)

    # Test GBPJPY
    price = await feed.get_price("GBPJPY")
    if price:
        print(f"Feed returned for GBPJPY:")
        print(f"  Bid: {price['bid']:.5f}")
        print(f"  Ask: {price['ask']:.5f}")

        # Check if this looks like GBPUSD instead
        if price['bid'] < 10:  # JPY pairs should be > 100
            print(f"  ⚠️ WARNING: Price looks like GBPUSD not GBPJPY!")
    else:
        print("Feed returned None for GBPJPY")

    # Also test GBPUSD for comparison
    print("\n4. Comparing with GBPUSD:")
    print("-" * 50)

    price_gbpusd = await feed.get_price("GBPUSD")
    if price_gbpusd:
        print(f"GBPUSD: Bid={price_gbpusd['bid']:.5f}, Ask={price_gbpusd['ask']:.5f}")

        if price and abs(price['bid'] - price_gbpusd['bid']) < 0.01:
            print("❌ ERROR: GBPJPY price matches GBPUSD! Symbol confusion detected!")

    # Check the symbol mapping
    print("\n5. Checking symbol suffix mapping:")
    print("-" * 50)

    # Check what the feed thinks is the correct symbol
    actual_symbol = feed._find_symbol_with_suffix("GBPJPY")
    print(f"Feed maps 'GBPJPY' to: '{actual_symbol}'")

    if actual_symbol:
        tick = mt5.symbol_info_tick(actual_symbol)
        if tick:
            print(f"Direct MT5 for '{actual_symbol}': Bid={tick.bid:.5f}, Ask={tick.ask:.5f}")

    await feed.disconnect()
    mt5.shutdown()

    print("\n" + "=" * 60)
    print("DEBUG COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(debug_gbpjpy())