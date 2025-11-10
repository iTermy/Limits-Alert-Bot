"""
Detailed OANDA Connection Tester
Tests OANDA connection step-by-step to find the exact issue
"""

import os
import asyncio
from dotenv import load_dotenv

# Load environment
load_dotenv()

print("="*80)
print("OANDA CONNECTION DETAILED TEST")
print("="*80)

# Step 1: Check environment variables
print("\n1. Environment Variables:")
print("-" * 80)
api_key = os.getenv('OANDA_API_KEY')
account_id = os.getenv('OANDA_ACCOUNT_ID')
practice = os.getenv('OANDA_PRACTICE', 'false').lower() == 'true'

if api_key:
    print(f"✓ OANDA_API_KEY: {api_key[:10]}...{api_key[-4:]}")
else:
    print("✗ OANDA_API_KEY: NOT FOUND")
    exit(1)

if account_id:
    print(f"✓ OANDA_ACCOUNT_ID: {account_id}")
else:
    print("✗ OANDA_ACCOUNT_ID: NOT FOUND")
    exit(1)

print(f"✓ OANDA_PRACTICE: {practice}")

# Step 2: Test OANDA REST API first (simpler than streaming)
print("\n2. Testing OANDA REST API:")
print("-" * 80)

import aiohttp

async def test_rest_api():
    """Test OANDA REST API to verify credentials"""

    # Choose server
    if practice:
        base_url = "https://api-fxpractice.oanda.com"
        print(f"Connecting to: {base_url} (PRACTICE)")
    else:
        base_url = "https://api-fxtrade.oanda.com"
        print(f"Connecting to: {base_url} (LIVE)")

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    # Test 1: Get account details
    print("\nTest 1: Fetching account details...")
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            url = f"{base_url}/v3/accounts/{account_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                print(f"Status: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    print("✓ Account verified successfully!")
                    print(f"  Currency: {data['account'].get('currency', 'N/A')}")
                    print(f"  Balance: {data['account'].get('balance', 'N/A')}")
                    return True
                elif response.status == 401:
                    print("✗ 401 Unauthorized - Invalid API key or account ID")
                    text = await response.text()
                    print(f"  Response: {text[:200]}")
                    return False
                elif response.status == 403:
                    print("✗ 403 Forbidden - API key doesn't have access to this account")
                    return False
                elif response.status == 404:
                    print("✗ 404 Not Found - Account ID doesn't exist")
                    return False
                else:
                    text = await response.text()
                    print(f"✗ Error {response.status}: {text[:200]}")
                    return False

        except asyncio.TimeoutError:
            print("✗ Connection timeout - Network issue or OANDA server down")
            return False
        except Exception as e:
            print(f"✗ Connection error: {e}")
            return False

    # Test 2: Get available instruments
    print("\nTest 2: Checking available instruments...")
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            url = f"{base_url}/v3/accounts/{account_id}/instruments"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    instruments = data.get('instruments', [])
                    print(f"✓ Account has access to {len(instruments)} instruments")

                    # Check for common indices
                    instrument_names = [i['name'] for i in instruments]

                    indices_to_check = ['NAS100_USD', 'SPX500_USD', 'US30_USD', 'EUR_USD']
                    print("\nChecking specific instruments:")
                    for inst in indices_to_check:
                        if inst in instrument_names:
                            print(f"  ✓ {inst} - Available")
                        else:
                            print(f"  ✗ {inst} - Not available")

                    return True
                else:
                    print(f"✗ Failed to get instruments: {response.status}")
                    return False
        except Exception as e:
            print(f"✗ Error checking instruments: {e}")
            return False

# Step 3: Test streaming connection
async def test_streaming():
    """Test OANDA streaming connection"""
    print("\n3. Testing OANDA Streaming:")
    print("-" * 80)

    try:
        from price_feeds.feeds.oanda_stream import OANDAStream

        print(f"Creating OANDAStream with practice={practice}...")
        stream = OANDAStream(api_key=api_key, account_id=account_id, practice=practice)

        print("Connecting to OANDA stream...")
        connected = await stream.connect()

        if connected:
            print("✓ Stream connected successfully!")

            # Try subscribing
            print("\nSubscribing to EUR_USD...")
            await stream.subscribe('EUR_USD')

            print("Waiting 5 seconds for price data...")
            import time
            start = time.time()
            price_received = False

            async for symbol, price_data in stream.stream_prices():
                print(f"✓ Price received: {symbol} @ {price_data['bid']}/{price_data['ask']}")
                price_received = True
                break

            if price_received:
                print("✓ Streaming is working correctly!")
                return True
            else:
                print("✗ No price data received")
                return False
        else:
            print("✗ Failed to connect to stream")
            return False

    except ImportError as e:
        print(f"✗ Import error: {e}")
        print("  Make sure oanda_stream.py is in price_feeds/feeds/")
        return False
    except Exception as e:
        print(f"✗ Streaming error: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    # Test REST API first
    rest_ok = await test_rest_api()

    if not rest_ok:
        print("\n" + "="*80)
        print("DIAGNOSIS:")
        print("="*80)
        print("REST API test failed. This means:")
        print("1. Your API key or account ID is incorrect, OR")
        print("2. You're using live credentials with practice=true, OR")
        print("3. You're using practice credentials with practice=false")
        print("\nFix suggestions:")
        print("- Verify account ID format: 001-001-XXXXXXX-001 (practice) or 101-001-XXXXXXX-001 (live)")
        print("- Check if OANDA_PRACTICE matches your account type")
        print("- Regenerate API key from OANDA dashboard")
        return

    # Test streaming
    print("\n" + "="*80)
    stream_ok = await test_streaming()

    print("\n" + "="*80)
    print("FINAL RESULT:")
    print("="*80)

    if rest_ok and stream_ok:
        print("✓ ALL TESTS PASSED - OANDA is configured correctly!")
        print("\nYour bot should work now. If it doesn't:")
        print("1. Verify price_stream_manager.py has the practice flag code")
        print("2. Restart your bot completely")
        print("3. Check bot logs for any initialization errors")
    elif rest_ok and not stream_ok:
        print("✓ REST API works")
        print("✗ Streaming failed")
        print("\nThis could mean:")
        print("1. Streaming API has different permissions")
        print("2. Network/firewall blocking streaming connection")
        print("3. Issue with oanda_stream.py code")
    else:
        print("✗ Connection failed")

if __name__ == "__main__":
    asyncio.run(main())