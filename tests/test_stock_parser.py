#!/usr/bin/env python
"""
Test suite for stock symbol parsing in the enhanced signal parser
"""
import sys
from pathlib import Path
import MetaTrader5 as mt5

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.parser import parse_signal, EnhancedSignalParser

# Stock signal test cases
STOCK_TEST_SIGNALS = [
    # Test 1: Direct ticker mention
    {
        "text": "224.85---225.38---227.73 AAPL short Stops 229.42",
        "channel": "stock-trades",
        "expected": {
            "instrument": "AAPL.NAS",  # or AAPL.NYSE depending on MT5
            "direction": "short",
            "limits": [227.73, 225.38, 224.85],
            "stop_loss": 229.42,
            "expiry_type": "day_end"
        },
        "description": "Direct ticker mention (AAPL)"
    },

    # Test 2: Company name in description
    {
        "text": "70.77---71.75---71.23---71.42 Cisco short Stops 71.90",
        "channel": "stock-trades",
        "expected": {
            "instrument": "CSCO.NAS",
            "direction": "short",
            "limits": [71.75, 71.42, 71.23, 70.77],
            "stop_loss": 71.90,
            "expiry_type": "day_end"
        },
        "description": "Company name match (Cisco -> CSCO)"
    },

    # Test 3: Microsoft
    {
        "text": "Microsoft buy 420.50 419.75 419.00 sl 417.50",
        "channel": "stock-trades",
        "expected": {
            "instrument": "MSFT.NAS",
            "direction": "long",
            "limits": [420.50, 419.75, 419.00],
            "stop_loss": 417.50,
            "expiry_type": "day_end"
        },
        "description": "Company name (Microsoft -> MSFT)"
    },

    # Test 4: Tesla with ticker
    {
        "text": "TSLA long 240.00 238.50 237.00 stop 235.00 hot",
        "channel": "stock-trades",
        "expected": {
            "instrument": "TSLA.NAS",
            "direction": "long",
            "limits": [240.00, 238.50, 237.00],
            "stop_loss": 235.00,
            "expiry_type": "day_end",
            "keywords": ["hot"]
        },
        "description": "Direct ticker (TSLA)"
    },

    # Test 5: Amazon description match
    {
        "text": "175.50 174.25 173.00 Amazon short stops 177.00",
        "channel": "stock-trades",
        "expected": {
            "instrument": "AMZN.NAS",
            "direction": "short",
            "limits": [175.50, 174.25, 173.00],
            "stop_loss": 177.00,
            "expiry_type": "day_end"
        },
        "description": "Company name (Amazon -> AMZN)"
    },

    # Test 6: With exchange suffix
    {
        "text": "NVDA.NAS buy 880 875 870 sl 865",
        "channel": "stock-trades",
        "expected": {
            "instrument": "NVDA.NAS",
            "direction": "long",
            "limits": [880, 875, 870],
            "stop_loss": 865,
            "expiry_type": "day_end"
        },
        "description": "Ticker with exchange suffix"
    },

    # Test 7: Meta/Facebook
    {
        "text": "Meta long 485.50 483.00 480.50 stop 478.00 swing",
        "channel": "stock-trades",
        "expected": {
            "instrument": "META.NAS",
            "direction": "long",
            "limits": [485.50, 483.00, 480.50],
            "stop_loss": 478.00,
            "expiry_type": "week_end",
            "keywords": ["swing"]
        },
        "description": "Company name (Meta -> META)"
    },

    # Test 8: Google/Alphabet
    {
        "text": "158.75 158.00 157.25 Google short sl 160.00",
        "channel": "stock-trades",
        "expected": {
            "instrument": "GOOGL.NAS",  # or GOOG.NAS
            "direction": "short",
            "limits": [158.75, 158.00, 157.25],
            "stop_loss": 160.00,
            "expiry_type": "day_end"
        },
        "description": "Company name (Google -> GOOGL/GOOG)"
    }
]


def test_mt5_connection():
    """Test MT5 connection and available symbols"""
    print("\n" + "=" * 60)
    print("Testing MT5 Connection")
    print("=" * 60)

    if not mt5.initialize():
        print("❌ MT5 initialization failed")
        print("   Make sure MetaTrader5 terminal is installed and running")
        return False

    # Get all symbols
    symbols = mt5.symbols_get()
    if not symbols:
        print("❌ No symbols retrieved from MT5")
        mt5.shutdown()
        return False

    # Count stock symbols
    stock_symbols = [s.name for s in symbols if s.name.endswith(('.NYSE', '.NAS', '.NASDAQ'))]

    print(f"✅ MT5 connected successfully")
    print(f"   Total symbols: {len(symbols)}")
    print(f"   Stock symbols: {len(stock_symbols)}")

    # Show some example stocks
    if stock_symbols:
        print(f"\n   Sample stocks available:")
        for symbol in stock_symbols[:10]:
            info = mt5.symbol_info(symbol)
            if info:
                print(f"     • {symbol}: {info.description}")

    return True


def test_stock_parsing():
    """Test stock signal parsing"""
    print("\n" + "=" * 60)
    print("Stock Signal Parsing Tests")
    print("=" * 60)

    # Initialize parser
    parser = EnhancedSignalParser()

    if not parser.mt5_initialized:
        print("⚠️  MT5 not initialized - stock tests will fail")
        print("   Please ensure MetaTrader5 is running")
        return 0, len(STOCK_TEST_SIGNALS)

    passed = 0
    failed = 0

    for i, test_case in enumerate(STOCK_TEST_SIGNALS, 1):
        print(f"\n📈 Test {i}: {test_case['description']}")
        print(f"   Signal: {test_case['text'][:60]}...")
        print(f"   Channel: {test_case['channel']}")

        # Parse the signal
        result = parse_signal(test_case["text"], test_case["channel"])

        if not result:
            print(f"❌ FAILED - Parsing returned None")
            failed += 1
            continue

        # Check instrument (flexible for .NAS/.NYSE)
        expected_base = test_case["expected"]["instrument"].split('.')[0]
        result_base = result.instrument.split('.')[0] if result.instrument else ""

        if result_base != expected_base:
            print(f"❌ FAILED - Instrument: got {result.instrument}, expected {expected_base}.XXX")
            failed += 1
            continue

        # Check that it has proper exchange suffix
        if not (result.instrument.endswith('.NAS') or result.instrument.endswith('.NYSE')):
            print(f"❌ FAILED - Invalid exchange suffix: {result.instrument}")
            failed += 1
            continue

        # Check other fields
        errors = []

        if result.direction != test_case["expected"]["direction"]:
            errors.append(f"Direction: got {result.direction}, expected {test_case['expected']['direction']}")

        if abs(result.stop_loss - test_case["expected"]["stop_loss"]) > 0.01:
            errors.append(f"Stop loss: got {result.stop_loss}, expected {test_case['expected']['stop_loss']}")

        if len(result.limits) != len(test_case["expected"]["limits"]):
            errors.append(f"Limits count: got {len(result.limits)}, expected {len(test_case['expected']['limits'])}")

        if errors:
            print(f"❌ FAILED - {'; '.join(errors)}")
            failed += 1
        else:
            print(f"✅ PASSED - {result.instrument} {result.direction}")
            print(f"   Parse method: {result.parse_method}")
            passed += 1

    # Cleanup
    parser.cleanup()

    return passed, failed


def test_ambiguous_cases():
    """Test cases that might match multiple stocks"""
    print("\n" + "=" * 60)
    print("Ambiguous Stock Name Tests")
    print("=" * 60)

    parser = EnhancedSignalParser()

    if not parser.mt5_initialized:
        print("⚠️  MT5 not initialized - skipping ambiguous tests")
        return

    ambiguous_tests = [
        ("Bank long 45.50 45.00 44.50 sl 44.00", "stock-trades", "Bank (multiple matches expected)"),
        ("Energy buy 85.00 84.50 84.00 stop 83.00", "stock-trades", "Energy (sector, multiple matches)"),
        ("Tech short 125.00 124.50 124.00 sl 126.00", "stock-trades", "Tech (generic term)"),
    ]

    for text, channel, description in ambiguous_tests:
        print(f"\n🔍 Testing: {description}")
        print(f"   Signal: {text}")

        result = parse_signal(text, channel)

        if result:
            print(f"   Result: {result.instrument} via {result.parse_method}")
            if result.parse_method == "moderate_confidence":
                print(f"   ✅ Correctly used AI for ambiguous case")
            else:
                print(f"   ℹ️  Found match via {result.parse_method}")
        else:
            print(f"   ℹ️  No match found (may need AI)")

    parser.cleanup()


def main():
    """Run all stock parsing tests"""
    print("\n🏦 Stock Signal Parser Test Suite")
    print("=" * 60)

    # Test MT5 connection first
    if not test_mt5_connection():
        print("\n⛔ Cannot proceed without MT5 connection")
        print("Please ensure:")
        print("1. MetaTrader5 terminal is installed")
        print("2. Terminal is running and logged in")
        print("3. ICMarkets account is connected")
        return 1

    # Run stock parsing tests
    passed, failed = test_stock_parsing()

    # Test ambiguous cases
    test_ambiguous_cases()

    # Summary
    print("\n" + "=" * 60)
    print("STOCK TEST SUMMARY")
    print("=" * 60)
    print(f"✅ Passed: {passed}/{len(STOCK_TEST_SIGNALS)}")
    print(f"❌ Failed: {failed}/{len(STOCK_TEST_SIGNALS)}")

    if failed == 0:
        print("\n🎉 All stock parsing tests passed!")
    else:
        print(f"\n⚠️  {failed} tests failed - review output above")

    print("\n💡 Notes:")
    print("• Stock symbols must end with .NYSE or .NAS")
    print("• Parser matches both ticker and company description")
    print("• Ambiguous matches fall back to AI")
    print("• Stock channel auto-detection prioritizes stock symbols")

    # Cleanup MT5
    mt5.shutdown()

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        mt5.shutdown()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test error: {e}")
        import traceback

        traceback.print_exc()
        mt5.shutdown()
        sys.exit(1)