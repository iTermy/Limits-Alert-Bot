"""
Configuration loader for the Trading Alert Bot
Enhanced with spread buffer settings helpers
"""
import json
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Manages loading and accessing configuration files"""

    def __init__(self, config_dir: str = "config"):
        """
        Initialize configuration loader

        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = Path(config_dir)
        self._configs = {}
        self._ensure_config_files()

    def _ensure_config_files(self):
        """Create default configuration files if they don't exist"""
        # Default settings
        default_settings = {
            "bot_prefix": "!",
            "enable_openai_fallback": False,
            "max_retries": 3,
            "connection_timeout": 30,
            "alert_cooldown_minutes": 5,
            "cleanup_days": 30,
            "debug_mode": True,
            "spread_buffer_enabled": True,
            "spread_buffer_config": {
                "apply_to_approaching": True,
                "apply_to_hit": True,
                "apply_to_stop_loss": False,
                "fallback_spread": 0.0,
                "log_buffer_usage": True
            }
        }

        # Default channels (will need to be updated by user)
        default_channels = {
            "monitored_channels": {},
            "alert_channel": None,
            "command_channel": None,
            "channel_settings": {}
        }

        # Default tracking config
        default_tracking = {
            "update_intervals": {
                "critical": 1,
                "near": 5,
                "medium": 30,
                "far": 60
            },
            "distance_thresholds": {
                "critical": 5,
                "near": 20,
                "medium": 50
            },
            "approaching_alert_distance": 3,
            "spread_multiplier": 1.0
        }

        # Default expiry config
        default_expiry = {
            "session_times": {
                "forex": {
                    "daily_close": "17:00",
                    "timezone": "America/New_York"
                },
                "commodity": {
                    "daily_close": "14:30",
                    "timezone": "America/New_York"
                }
            },
            "expiry_check_interval": 300,
            "default_expiry_type": "day_end"
        }

        # Create config directory if it doesn't exist
        self.config_dir.mkdir(exist_ok=True)

        # Create default files if they don't exist
        defaults = {
            "settings.json": default_settings,
            "channels.json": default_channels,
            "tracking_config.json": default_tracking,
            "expiry_config.json": default_expiry
        }

        for filename, default_content in defaults.items():
            filepath = self.config_dir / filename
            if not filepath.exists():
                with open(filepath, 'w') as f:
                    json.dump(default_content, f, indent=2)
                logger.info(f"Created default {filename}")

    def load(self, filename: str, reload: bool = False) -> Dict[str, Any]:
        """
        Load a configuration file

        Args:
            filename: Name of the configuration file
            reload: Force reload from disk

        Returns:
            Configuration dictionary
        """
        if not reload and filename in self._configs:
            return self._configs[filename]

        filepath = self.config_dir / filename

        if not filepath.exists():
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r') as f:
                config = json.load(f)
                self._configs[filename] = config
                return config
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {filename}: {e}")

    def get(self, filename: str, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration value

        Args:
            filename: Configuration file name
            key: Configuration key (supports dot notation)
            default: Default value if key not found

        Returns:
            Configuration value
        """
        config = self.load(filename)

        # Support dot notation for nested keys
        keys = key.split('.')
        value = config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def save(self, filename: str, config: Dict[str, Any]):
        """
        Save configuration to file

        Args:
            filename: Configuration file name
            config: Configuration dictionary to save
        """
        filepath = self.config_dir / filename

        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)

        self._configs[filename] = config

    def reload_all(self):
        """Reload all configuration files from disk"""
        self._configs.clear()
        for filepath in self.config_dir.glob("*.json"):
            self.load(filepath.name, reload=True)


# Global configuration instance
config = ConfigLoader()


def get_config(filename: str = "settings.json") -> Dict[str, Any]:
    """
    Quick access to configuration

    Args:
        filename: Configuration file to load

    Returns:
        Configuration dictionary
    """
    return config.load(filename)


def load_settings() -> dict:
    """
    Load settings from settings.json

    Returns:
        Settings dictionary with spread buffer defaults if file not found
    """
    config_path = Path(__file__).parent.parent / 'config' / 'settings.json'
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("settings.json not found, using defaults")
        return {
            "bot_prefix": "!",
            "spread_buffer_enabled": True,
            "spread_buffer_config": {
                "apply_to_approaching": True,
                "apply_to_hit": True,
                "apply_to_stop_loss": False,
                "fallback_spread": 0.0,
                "log_buffer_usage": True
            }
        }
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
        return {"spread_buffer_enabled": True}


def save_settings(settings: dict):
    """
    Save settings to settings.json

    Args:
        settings: Settings dictionary to save
    """
    config_path = Path(__file__).parent.parent / 'config' / 'settings.json'
    try:
        with open(config_path, 'w') as f:
            json.dump(settings, f, indent=2)
        logger.info("Settings saved successfully")
    except Exception as e:
        logger.error(f"Error saving settings: {e}")
        raise