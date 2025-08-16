"""
Test script to identify MT5 symbol confusion
Run this to see if MT5 is returning wrong prices
"""

import MetaTrader5 as mt5
import time


def test_mt5_symbols():
    """Test if MT5 is returning correct prices for symbols"""

    if not mt5.initialize():
        print("Failed to initialize MT5")
        return

    print("\n" + "=" * 60)
    print("MT5 SYMBOL CONFUSION TEST")
    print("=" * 60)

    # Test pairs that have been problematic
    test_pairs = [
        "CHFJPY",  # Should be ~180
        "EURCHF",  # Should be ~0.9-1.0
        "GBPCHF",  # Should be ~1.1-1.3
        "USDCHF",  # Should be ~0.8-1.0
        "GBPJPY",  # Should be ~190-200
        "EURJPY",  # Should be ~160-170
        "USDJPY",  # Should be ~150-160
        "EURNZD",  # Should be ~1.7-1.9
    ]

    print("\nDirect MT5 symbol_info_tick() calls:")
    print("-" * 50)

    for symbol in test_pairs:
        tick = mt5.symbol_info_tick(symbol)
        if tick:
            is_jpy = 'JPY' in symbol
            expected_range = (50, 300) if is_jpy else (0.5, 3.0)
            in_range = expected_range[0] <= tick.bid <= expected_range[1]

            status = "✓" if in_range else "❌"
            print(f"{status} {symbol:8} -> Bid: {tick.bid:8.5f}, Ask: {tick.ask:8.5f}")

            if not in_range:
                print(f"    WARNING: Expected {expected_range[0]}-{expected_range[1]}")
        else:
            print(f"✗ {symbol:8} -> Not found")

        # Small delay to avoid overwhelming MT5
        time.sleep(0.1)

    print("\n" + "=" * 60)

    # Test fetching multiple times to see if results change
    print("\nConsistency test for CHFJPY (5 fetches):")
    print("-" * 50)

    for i in range(5):
        tick = mt5.symbol_info_tick("CHFJPY")
        if tick:
            print(f"  Attempt {i + 1}: Bid={tick.bid:.5f}, Ask={tick.ask:.5f}")
        time.sleep(0.5)

    mt5.shutdown()
    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    test_mt5_symbols()