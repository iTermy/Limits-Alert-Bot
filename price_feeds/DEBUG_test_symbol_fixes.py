"""
Test Symbol Mapping Fixes
Verifies NAS100USD and JP225 mapping work correctly
"""

import sys

sys.path.insert(0, '.')

from price_feeds.symbol_mapper import SymbolMapper
import logging

logging.basicConfig(level=logging.DEBUG)

mapper = SymbolMapper()

print("=" * 80)
print("SYMBOL MAPPING FIX TEST")
print("=" * 80)

# Test cases
test_cases = [
    ('NAS100USD', 'oanda', 'NAS100_USD', 'NAS100USD'),
    ('SPX500USD', 'oanda', 'SPX500_USD', 'SPX500USD'),
    ('JP225', 'oanda', 'JP225_USD', 'JP225'),
    ('US30USD', 'oanda', 'US30_USD', 'US30USD'),
]

print("\n1. Forward Mapping (Database → OANDA)")
print("-" * 80)

all_passed = True

for internal, feed, expected_feed, expected_reverse in test_cases:
    # Test forward
    result = mapper.get_feed_symbol(internal, feed)

    if result == expected_feed:
        print(f"✓ {internal:15} → {result:20} (expected {expected_feed})")
    else:
        print(f"✗ {internal:15} → {result:20} (expected {expected_feed})")
        all_passed = False

print("\n2. Reverse Mapping (OANDA → Database)")
print("-" * 80)

for internal, feed, expected_feed, expected_reverse in test_cases:
    # Test reverse
    feed_symbol = mapper.get_feed_symbol(internal, feed)
    if feed_symbol:
        reverse = mapper.get_internal_symbol(feed_symbol, feed)

        if reverse == expected_reverse:
            print(f"✓ {feed_symbol:20} → {reverse:15} (expected {expected_reverse})")
        else:
            print(f"✗ {feed_symbol:20} → {reverse:15} (expected {expected_reverse})")
            all_passed = False
    else:
        print(f"✗ Could not map {internal} to {feed}")
        all_passed = False

print("\n3. Round-Trip Test")
print("-" * 80)

for internal, feed, expected_feed, expected_reverse in test_cases:
    # Test full round trip
    feed_symbol = mapper.get_feed_symbol(internal, feed)
    reverse = mapper.get_internal_symbol(feed_symbol, feed) if feed_symbol else None

    if reverse == expected_reverse:
        print(f"✓ {internal:15} → {feed_symbol:20} → {reverse:15}")
    else:
        print(f"✗ {internal:15} → {feed_symbol:20} → {reverse:15}")
        print(f"  Expected final: {expected_reverse}")
        all_passed = False

print("\n" + "=" * 80)
if all_passed:
    print("✓ ALL TESTS PASSED")
    print("\nThe symbol mapper is now fixed!")
    print("Deploy symbol_mapper.py to your bot and restart.")
else:
    print("✗ SOME TESTS FAILED")
    print("\nCheck the output above for issues.")

print("=" * 80)