#!/usr/bin/env python
"""
Script to update existing channel configuration with new settings
"""
import json
import os
from pathlib import Path


def update_channel_config():
    """Update the channels.json file with new channel_settings structure"""

    config_path = Path("config/channels.json")

    # Read existing configuration
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
        print(f"✅ Loaded existing configuration")
    else:
        print("❌ No existing configuration found")
        return

    # Add channel_settings if not present
    if "channel_settings" not in config:
        config["channel_settings"] = {}
        print("➕ Added channel_settings section")

    # Update settings for each monitored channel
    for channel_name, channel_id in config.get("monitored_channels", {}).items():
        if channel_name not in config["channel_settings"]:
            # Set defaults based on channel name
            if "gold" in channel_name.lower():
                config["channel_settings"][channel_name] = {
                    "default_instrument": "XAUUSD",
                    "default_expiry": "day_end"
                }
                print(f"➕ Added gold-specific settings for {channel_name}")
            elif "oil" in channel_name.lower():
                config["channel_settings"][channel_name] = {
                    "default_instrument": "USOIL",
                    "default_expiry": "week_end"
                }
                print(f"➕ Added oil-specific settings for {channel_name}")
            elif "indices" in channel_name.lower() or "index" in channel_name.lower():
                config["channel_settings"][channel_name] = {
                    "default_expiry": "day_end"
                }
                print(f"➕ Added indices-specific settings for {channel_name}")
            else:
                config["channel_settings"][channel_name] = {
                    "default_expiry": "day_end"
                }
                print(f"➕ Added default settings for {channel_name}")

    # Special handling for gold_trades channel
    if "gold_trades" in config["channel_settings"]:
        config["channel_settings"]["gold_trades"]["default_instrument"] = "XAUUSD"
        print("✅ Ensured gold_trades has XAUUSD as default instrument")

    # Save updated configuration
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"\n✅ Configuration updated successfully!")
    print("\nChannel settings added:")
    for channel, settings in config["channel_settings"].items():
        print(f"  - {channel}:")
        for key, value in settings.items():
            print(f"      {key}: {value}")


def main():
    """Main entry point"""
    print("=" * 50)
    print("Channel Configuration Updater")
    print("=" * 50)

    # Change to project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    os.chdir(project_root)

    update_channel_config()

    print("\n💡 Tips:")
    print("1. Messages from 'gold_trades' channel will now auto-detect as XAUUSD")
    print("2. You can add more channel-specific defaults in channel_settings")
    print("3. Reload the bot configuration with !reload command")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")