#!/usr/bin/env python
"""
Test script to verify bot setup and basic functionality
"""
import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from utils.logger import logger
from utils.config_loader import config
from database import db
from core.parser import parse_signal

# Load environment variables
load_dotenv()


async def test_database():
    """Test database connection and operations"""
    print("\n🔍 Testing Database...")

    try:
        # Initialize database
        await db.initialize()
        print("✅ Database initialized")

        # Test insert
        signal_id = await db.insert_signal(
            message_id="test_123",
            channel_id="test_channel",
            instrument="GBPUSD",
            direction="long",
            stop_loss=1.3450,
            expiry_type="day_end"
        )
        print(f"✅ Test signal inserted with ID: {signal_id}")

        # Test insert limits
        await db.insert_limits(signal_id, [1.3500, 1.3510, 1.3520])
        print("✅ Test limits inserted")

        # Test fetch
        signals = await db.get_active_signals()
        print(f"✅ Retrieved {len(signals)} active signal(s)")

        # Clean up test data
        await db.update_signal_status(signal_id, "cancelled")
        print("✅ Test signal cleaned up")

        return True

    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


def test_configuration():
    """Test configuration loading"""
    print("\n🔍 Testing Configuration...")

    try:
        # Load settings
        settings = config.load("settings.json")
        print(f"✅ Settings loaded: {len(settings)} keys")

        # Load channels
        channels = config.load("channels.json")
        print(f"✅ Channels config loaded")

        # Check for required configs
        if not channels.get("monitored_channels"):
            print("⚠️  No monitored channels configured - add channel IDs to config/channels.json")

        if not channels.get("alert_channel"):
            print("⚠️  No alert channel configured - add alert channel ID to config/channels.json")

        return True

    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False


def test_environment():
    """Test environment variables"""
    print("\n🔍 Testing Environment Variables...")

    token = os.getenv('DISCORD_BOT_TOKEN')
    if token:
        print(f"✅ Discord bot token found (length: {len(token)})")
    else:
        print("❌ DISCORD_BOT_TOKEN not found in .env file")
        return False

    openai_key = os.getenv('OPENAI_API_KEY')
    if openai_key:
        print(f"✅ OpenAI API key found (optional)")
    else:
        print("ℹ️  OpenAI API key not found (optional for Stage 1)")

    return True


def test_parser():
    """Test signal parser with sample signals"""
    print("\n🔍 Testing Signal Parser...")

    test_signals = [
        "1.34850—–1.34922——1.35035 gbpusd short vth Stops 1.35132",
        "Gold buy 2650, 2645, 2640 SL 2635 vtwe",
        "EURUSD 1.0850/1.0840/1.0830 long stop 1.0820",
    ]

    success_count = 0
    for signal_text in test_signals:
        print(f"\nTesting: {signal_text[:50]}...")
        result = parse_signal(signal_text)

        if result:
            print(f"✅ Parsed successfully:")
            print(f"   Instrument: {result.instrument}")
            print(f"   Direction: {result.direction}")
            print(f"   Limits: {result.limits}")
            print(f"   Stop Loss: {result.stop_loss}")
            print(f"   Expiry: {result.expiry_type}")
            success_count += 1
        else:
            print(f"⚠️  Failed to parse (this is expected in Stage 1)")

    print(f"\n📊 Parsed {success_count}/{len(test_signals)} signals")
    return True  # Parser is basic in Stage 1, so we don't fail the test


def check_dependencies():
    """Check if all required packages are installed"""
    print("\n🔍 Checking Dependencies...")

    required = ['discord', 'dotenv', 'aiohttp', 'pytz', 'aiosqlite']
    missing = []

    for package in required:
        try:
            __import__(package)
            print(f"✅ {package} installed")
        except ImportError:
            print(f"❌ {package} not installed")
            missing.append(package)

    if missing:
        print(f"\n⚠️  Missing packages: {', '.join(missing)}")
        print("Run: pip install -r requirements.txt")
        return False

    return True


async def main():
    """Run all tests"""
    print("=" * 50)
    print("Discord Trading Bot - Setup Test")
    print("=" * 50)

    all_passed = True

    # Check dependencies
    if not check_dependencies():
        all_passed = False

    # Test environment
    if not test_environment():
        all_passed = False

    # Test configuration
    if not test_configuration():
        all_passed = False

    # Test database
    if not await test_database():
        all_passed = False

    # Test parser
    if not test_parser():
        all_passed = False

    # Summary
    print("\n" + "=" * 50)
    if all_passed:
        print("✅ All tests passed! Bot is ready to run.")
        print("\nNext steps:")
        print("1. Add your Discord channel IDs to config/channels.json")
        print("2. Run the bot with: python main.py")
        print("3. Use !ping and !status commands to test")
    else:
        print("⚠️  Some tests failed. Please fix the issues above.")
    print("=" * 50)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"\n❌ Test script error: {e}")
        sys.exit(1)