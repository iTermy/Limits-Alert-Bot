#!/usr/bin/env python
"""
Test suite for enhanced signal parser
"""
import sys
from pathlib import Path
import asyncio

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.parser import parse_signal, EnhancedSignalParser

# Enhanced test signals covering all formats
TEST_SIGNALS = [
    # Standard formats
    {
        "text": "1.36917-----1.36869----1.36846----1.36819-----1.36803----1.36726 usdcad long vth---hot Stops 1.36636",
        "channel": "forex-exotics",
        "expected": {
            "instrument": "USDCAD",
            "direction": "long",
            "limits": [1.36917, 1.36869, 1.36846, 1.36819, 1.36803, 1.36726],
            "stop_loss": 1.36636,
            "expiry_type": "week_end",  # VTH now means week
            "keywords": ["hot"]
        }
    },
    # Abbreviations
    {
        "text": "au short 64570 64520 64480 stops 64650",
        "channel": "forex-exotics",
        "expected": {
            "instrument": "AUDUSD",
            "direction": "short",
            "limits": [0.64570, 0.64520, 0.64480],  # Should be scaled down
            "stop_loss": 0.64650,
            "expiry_type": "day_end"
        }
    },
    # Gold channel auto-detection
    {
        "text": "buy 2650 2645 2640 sl 2635 vtai",
        "channel": "gold-trades",
        "expected": {
            "instrument": "XAUUSD",
            "direction": "long",
            "limits": [2650, 2645, 2640],
            "stop_loss": 2635,
            "expiry_type": "no_expiry"  # VTAI = no expiry
        }
    },
    # Oil with IC specification
    {
        "text": "oil ic short 75.50 75.25 75.00 stop 74.50",
        "channel": "oil-trades",
        "expected": {
            "instrument": "XTIUSD",  # IC oil
            "direction": "short",
            "limits": [75.50, 75.25, 75.00],
            "stop_loss": 74.50,
            "expiry_type": "day_end"
        }
    },
    # Regular oil (no IC)
    {
        "text": "oil long 72.00 71.75 71.50 sl 71.00",
        "channel": "oil-trades",
        "expected": {
            "instrument": "USOILSPOT",
            "direction": "long",
            "limits": [72.00, 71.75, 71.50],
            "stop_loss": 71.00,
            "expiry_type": "day_end"
        }
    },
    # Indices
    {
        "text": "nas buy 15250 15200 15150 sl 15050 alien",
        "channel": "indices-trades",
        "expected": {
            "instrument": "NAS100USD",
            "direction": "long",
            "limits": [15250, 15200, 15150],
            "stop_loss": 15050,
            "expiry_type": "no_expiry"  # alien = no expiry
        }
    },
    # Crypto
    {
        "text": "btc long 95000 94500 94000 stop 93000 semi-swing",
        "channel": "crypto-trades",
        "expected": {
            "instrument": "BTCUSDT",
            "direction": "long",
            "limits": [95000, 94500, 94000],
            "stop_loss": 93000,
            "expiry_type": "week_end",
            "keywords": ["semi-swing"]
        }
    },
    # OT trade calls format
    {
        "text": "EURUSD LONG 1.0850-1.0840-1.0830 Sl 1.0820",
        "channel": "ot-trade-calls",
        "expected": {
            "instrument": "EURUSD",
            "direction": "long",
            "limits": [1.0850, 1.0840, 1.0830],
            "stop_loss": 1.0820,
            "expiry_type": "day_end"
        }
    },
    # Complex with emojis and extra text
    {
        "text": "🔥 HOT SIGNAL: eu sell NOW @ 1.0950, 1.0960, 1.0970 | Stop: 1.1000 | hot",
        "channel": "scalps",
        "expected": {
            "instrument": "EURUSD",
            "direction": "short",
            "limits": [1.0950, 1.0960, 1.0970],
            "stop_loss": 1.1000,
            "expiry_type": "day_end",
            "keywords": ["hot"]
        }
    },
    # SPX with full name
    {
        "text": "spx short 4500 4490 4480 stops 4520",
        "channel": "indices-trades",
        "expected": {
            "instrument": "SPX500USD",
            "direction": "short",
            "limits": [4500, 4490, 4480],
            "stop_loss": 4520,
            "expiry_type": "day_end"
        }
    }
]

# Exclusion test cases (should fail)
EXCLUSION_TESTS = [
    ("DXY long 105.50 105.00 stop 104.50", "DXY should be excluded"),
    ("Gold futures buy 2650 sl 2640", "Futures should be excluded"),
    ("NQ long 15000 stop 14900", "NQ futures should be excluded"),
    ("ES short 4500 sl 4520", "ES futures should be excluded"),
    ("Buy some coffee", "Non-signal message"),
    ("The market is looking good", "No numbers in message"),
]


def compare_results(parsed, expected):
    """Compare parsed results with expected values"""
    if not parsed:
        return False, "Parsing failed - returned None"

    errors = []

    # Check instrument
    if parsed.instrument != expected["instrument"]:
        errors.append(f"Instrument: got {parsed.instrument}, expected {expected['instrument']}")

    # Check direction
    if parsed.direction != expected["direction"]:
        errors.append(f"Direction: got {parsed.direction}, expected {expected['direction']}")

    # Check limits count and values
    if len(parsed.limits) != len(expected["limits"]):
        errors.append(f"Limits count: got {len(parsed.limits)}, expected {len(expected['limits'])}")
    else:
        for i, (got, exp) in enumerate(zip(sorted(parsed.limits, reverse=True),
                                           sorted(expected["limits"], reverse=True))):
            if abs(got - exp) > 0.00001:
                errors.append(f"Limit {i}: got {got}, expected {exp}")

    # Check stop loss
    if abs(parsed.stop_loss - expected["stop_loss"]) > 0.00001:
        errors.append(f"Stop loss: got {parsed.stop_loss}, expected {expected['stop_loss']}")

    # Check expiry type
    if parsed.expiry_type != expected["expiry_type"]:
        errors.append(f"Expiry: got {parsed.expiry_type}, expected {expected['expiry_type']}")

    # Check keywords if specified
    if "keywords" in expected:
        expected_keywords = set(expected["keywords"])
        parsed_keywords = set(parsed.keywords)
        if expected_keywords != parsed_keywords:
            errors.append(f"Keywords: got {parsed_keywords}, expected {expected_keywords}")

    if errors:
        return False, "; ".join(errors)
    return True, "All checks passed"


def run_parser_tests():
    """Run all parser tests"""
    print("=" * 60)
    print("Enhanced Signal Parser Test Suite")
    print("=" * 60)

    passed = 0
    failed = 0

    for i, test_case in enumerate(TEST_SIGNALS, 1):
        print(f"\n📝 Test {i}: {test_case['text'][:50]}...")
        print(f"   Channel: {test_case.get('channel', 'none')}")

        # Parse the signal
        result = parse_signal(test_case["text"], test_case.get("channel"))

        # Compare with expected
        success, message = compare_results(result, test_case["expected"])

        if success:
            print(f"✅ PASSED - {message}")
            if result:
                print(f"   Parse method: {result.parse_method}")
            passed += 1
        else:
            print(f"❌ FAILED - {message}")
            failed += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"Main Tests: {passed}/{len(TEST_SIGNALS)} passed, {failed} failed")
    print(f"Success rate: {(passed / len(TEST_SIGNALS)) * 100:.1f}%")

    return passed, failed


def test_exclusions():
    """Test that certain messages are properly excluded"""
    print("\n" + "=" * 60)
    print("Exclusion Tests (should all fail to parse)")
    print("=" * 60)

    correct_exclusions = 0

    for text, reason in EXCLUSION_TESTS:
        print(f"\n🚫 Testing: '{text[:50]}...'")
        print(f"   Reason: {reason}")

        result = parse_signal(text)

        if result is None:
            print(f"   ✅ Correctly excluded")
            correct_exclusions += 1
        else:
            print(f"   ❌ ERROR: Should have been excluded but parsed as {result.instrument}")

    print(f"\n✅ Correctly excluded: {correct_exclusions}/{len(EXCLUSION_TESTS)}")
    return correct_exclusions


def test_special_cases():
    """Test special parsing cases"""
    print("\n" + "=" * 60)
    print("Special Case Tests")
    print("=" * 60)

    # Test VTH = week (not hit)
    vth_signal = "eurusd long 1.0850 stop 1.0820 vth"
    result = parse_signal(vth_signal)
    if result and result.expiry_type == "week_end":
        print("✅ VTH correctly parsed as week_end")
    else:
        print("❌ VTH parsing failed")

    # Test VTAI/alien = no expiry
    vtai_signal = "gbpusd short 1.2500 sl 1.2550 vtai"
    result = parse_signal(vtai_signal)
    if result and result.expiry_type == "no_expiry":
        print("✅ VTAI correctly parsed as no_expiry")
    else:
        print("❌ VTAI parsing failed")

    alien_signal = "gold buy 2650 stop 2640 alien"
    result = parse_signal(alien_signal)
    if result and result.expiry_type == "no_expiry":
        print("✅ 'alien' correctly parsed as no_expiry")
    else:
        print("❌ 'alien' parsing failed")

    # Test large forex numbers
    large_forex = "audusd buy 65450 65400 65350 stop 65250"
    result = parse_signal(large_forex)
    if result and abs(result.limits[0] - 0.65450) < 0.00001:
        print("✅ Large forex numbers correctly scaled down")
    else:
        print("❌ Large forex number scaling failed")

    # Test keyword extraction
    hot_signal = "eurusd long 1.0850 sl 1.0820 hot semi-swing"
    result = parse_signal(hot_signal)
    if result and "hot" in result.keywords and "semi-swing" in result.keywords:
        print("✅ Keywords correctly extracted")
    else:
        print("❌ Keyword extraction failed")


def main():
    """Run all tests"""
    print("\n🚀 Starting Enhanced Parser Tests\n")

    # Initialize parser (will connect to MT5 if available)
    parser = EnhancedSignalParser()

    # Run main parser tests
    passed, failed = run_parser_tests()

    # Test exclusions
    exclusions = test_exclusions()

    # Test special cases
    test_special_cases()

    # Cleanup
    parser.cleanup()

    # Final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"✅ Parser tests: {passed}/{len(TEST_SIGNALS)} passed")
    print(f"✅ Exclusion tests: {exclusions}/{len(EXCLUSION_TESTS)} correctly excluded")

    if failed == 0 and exclusions == len(EXCLUSION_TESTS):
        print("\n🎉 All tests passed! Enhanced parser is working correctly.")
    else:
        print(f"\n⚠️  Some tests failed. Review the output above.")

    print("\n💡 Notes:")
    print("- VTH now correctly means 'Valid Till Week'")
    print("- VTAI/alien means 'Valid Till Hit' (no expiry)")
    print("- Large forex numbers are automatically scaled")
    print("- Channel-specific defaults are applied")
    print("- Futures and DXY signals are excluded")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
    except Exception as e:
        print(f"\n❌ Test error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)