"""
Alert Distance Configuration for Trading Alert Bot

Manages alert distances with support for multiple distance types (pips, dollars, percentage).

Supported types:
  - pips        (forex)
  - dollars     (metals, oil)
  - percentage  (indices, stocks, crypto)
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Literal

from ._base_config import BaseThresholdConfig

logger = logging.getLogger(__name__)

DistanceType = Literal["pips", "dollars", "percentage"]


class AlertDistanceConfig(BaseThresholdConfig):
    """
    Manages alert distance configuration with support for multiple distance types.

    Features:
    - Asset-specific defaults
    - Persistent manual overrides
    - Automatic JPY pair detection
    - Config validation and migration from old formats
    """

    CONFIG_FILENAME = "alert_distances.json"

    def __init__(self, config_path: str = None):
        super().__init__(config_path)
        logger.info("AlertDistanceConfig initialized")

    # === Config defaults, migration & validation ===

    def _post_load(self, raw: Dict) -> Dict:
        if "defaults" not in raw or not self._is_new_format(raw):
            logger.warning("Old config format detected, migrating...")
            migrated = self._migrate_old_config(raw)
            self._save_config(migrated)
            return migrated
        return raw

    def _is_new_format(self, config: Dict) -> bool:
        defaults = config.get("defaults")
        if not isinstance(defaults, dict):
            return False
        for settings in defaults.values():
            if isinstance(settings, dict):
                if "type" in settings and "value" in settings:
                    return True
                if "approaching_pips" in settings or "approaching_distance" in settings:
                    return False
        return False

    def _create_default_config(self) -> Dict:
        return {
            "defaults": {
                "forex": {"type": "pips", "value": 10.0, "description": "Standard forex pairs"},
                "forex_jpy": {
                    "type": "pips",
                    "value": 20.0,
                    "description": "JPY pairs (auto-detected)",
                },
                "metals": {"type": "dollars", "value": 10.0, "description": "Gold, Silver, etc."},
                "indices": {"type": "percentage", "value": 1.0, "description": "Stock indices"},
                "stocks": {"type": "percentage", "value": 1.0, "description": "Individual stocks"},
                "crypto": {"type": "percentage", "value": 0.5, "description": "Cryptocurrencies"},
                "oil": {"type": "dollars", "value": 0.5, "description": "Oil commodities"},
            },
            "overrides": {},
        }

    def _migrate_old_config(self, old_config: Dict) -> Dict:
        """
        Migrate old config to new nested structure.

        Handles multiple old formats:
        1. Flat format: {"forex": 10.0, "metals": 10.0}
        2. Nested with approaching_pips: {"defaults": {"forex": {"approaching_pips": 10, ...}}}
        3. Nested with approaching_distance: {"defaults": {"metals": {"approaching_distance": 10, ...}}}
        """
        logger.info("Starting config migration...")
        new_config = self._create_default_config()

        if "defaults" in old_config:
            old_defaults = old_config["defaults"]

            for asset_class, settings in old_defaults.items():
                if not isinstance(settings, dict):
                    continue

                if asset_class in ["forex", "forex_jpy"]:
                    distance_type = "pips"
                elif asset_class in ["metals", "oil"]:
                    distance_type = "dollars"
                elif asset_class in ["indices", "stocks", "crypto"]:
                    distance_type = "percentage"
                else:
                    distance_type = "pips"

                value = None
                if "approaching_pips" in settings:
                    value = settings["approaching_pips"]
                    distance_type = "pips"
                elif "approaching_distance" in settings:
                    value = settings["approaching_distance"]

                if value is not None:
                    if distance_type == "percentage" and value > 10:
                        if asset_class == "indices":
                            value = 1.0
                        elif asset_class == "crypto":
                            value = 0.5
                        else:
                            value = 1.0

                    if asset_class in new_config["defaults"]:
                        new_config["defaults"][asset_class]["value"] = value
                        new_config["defaults"][asset_class]["type"] = distance_type
        else:
            for asset_class, value in old_config.items():
                if asset_class in ["overrides", "dynamic_overrides"]:
                    continue

                if isinstance(value, (int, float)):
                    if asset_class in ["forex", "forex_jpy"]:
                        distance_type = "pips"
                    elif asset_class in ["metals", "oil"]:
                        distance_type = "dollars"
                    elif asset_class in ["indices", "stocks", "crypto"]:
                        distance_type = "percentage"
                        if value > 10:
                            value = 1.0 if asset_class != "crypto" else 0.5
                    else:
                        distance_type = "pips"

                    if asset_class in new_config["defaults"]:
                        new_config["defaults"][asset_class]["value"] = value
                        new_config["defaults"][asset_class]["type"] = distance_type

        for section_key in ("overrides", "dynamic_overrides"):
            if section_key in old_config and isinstance(old_config[section_key], dict):
                for symbol, settings in old_config[section_key].items():
                    if not isinstance(settings, dict):
                        continue
                    if symbol in new_config["overrides"]:
                        continue
                    if "approaching_pips" in settings:
                        new_config["overrides"][symbol] = {
                            "type": "pips",
                            "value": settings["approaching_pips"],
                            "set_by": "Migration",
                            "set_at": datetime.now(timezone.utc).isoformat(),
                        }
                    elif "approaching_distance" in settings:
                        new_config["overrides"][symbol] = {
                            "type": "dollars",
                            "value": settings["approaching_distance"],
                            "set_by": "Migration",
                            "set_at": datetime.now(timezone.utc).isoformat(),
                        }

        logger.info("Configuration migrated successfully")
        return new_config

    def _validate_config(self):
        super()._validate_config()

        for asset_class, settings in self.config.get("defaults", {}).items():
            if not isinstance(settings, dict):
                self.config["defaults"][asset_class] = {
                    "type": "pips",
                    "value": 10.0,
                    "description": "Default",
                }
                continue

            if "type" not in settings:
                settings["type"] = "pips"
            if "value" not in settings:
                settings["value"] = 10.0
            if "description" not in settings:
                settings["description"] = f"Default for {asset_class}"

    # === Public API ===

    def get_approaching_distance(self, symbol: str, current_price: float = None) -> float:
        """
        Get approaching alert distance for a symbol in absolute price units.

        Args:
            symbol: Trading symbol (e.g., EURUSD, XAUUSD, NAS100USD)
            current_price: Current price (required for percentage calculations)

        Returns:
            Distance in absolute price units (e.g., 0.0010 for 10 pips in EURUSD)
        """
        config = self._get_config_for_symbol(symbol)
        distance_type = config["type"]
        value = config["value"]

        if distance_type == "pips":
            pip_size = self.get_pip_size(symbol)
            return value * pip_size

        if distance_type == "dollars":
            return value

        if distance_type == "percentage":
            if current_price is None:
                logger.error(f"Current price required for percentage calculation: {symbol}")
                return self._get_fallback_distance(symbol)
            return (value / 100.0) * current_price

        logger.error(f"Unknown distance type: {distance_type}")
        return self._get_fallback_distance(symbol)

    def _get_config_for_symbol(self, symbol: str) -> Dict:
        symbol_upper = symbol.upper()

        if symbol_upper in self.config["overrides"]:
            override = self.config["overrides"][symbol_upper]
            return {"type": override["type"], "value": override["value"]}

        asset_class = self.determine_asset_class(symbol)

        if asset_class in self.config["defaults"]:
            default = self.config["defaults"][asset_class]
            return {"type": default["type"], "value": default["value"]}

        logger.warning(f"No config found for {symbol}, using forex default")
        return {"type": "pips", "value": 10.0}

    def _get_fallback_distance(self, symbol: str) -> float:
        asset_class = self.determine_asset_class(symbol)
        fallbacks = {
            "forex": 0.0010,
            "forex_jpy": 0.20,
            "metals": 10.0,
            "indices": 50.0,
            "stocks": 1.0,
            "crypto": 100.0,
            "oil": 0.5,
        }
        return fallbacks.get(asset_class, 0.0010)

    def set_override(
        self, symbol: str, value: float, distance_type: DistanceType, set_by: str = "User"
    ) -> bool:
        """Set a manual override for a symbol."""
        if distance_type not in ["pips", "dollars", "percentage"]:
            logger.error(f"Invalid distance type: {distance_type}")
            return False

        if value <= 0:
            logger.error(f"Invalid value: {value}")
            return False

        symbol_upper = symbol.upper()

        self.config["overrides"][symbol_upper] = {
            "type": distance_type,
            "value": value,
            "set_by": set_by,
            "set_at": datetime.now(timezone.utc).isoformat(),
        }

        self._save_config()

        logger.info(f"Set alert distance override: {symbol_upper} = {value} {distance_type}")
        return True

    def remove_override(self, symbol: str) -> bool:
        """Remove a manual override for a symbol."""
        symbol_upper = symbol.upper()

        if symbol_upper in self.config["overrides"]:
            del self.config["overrides"][symbol_upper]
            self._save_config()
            logger.info(f"Removed alert distance override: {symbol_upper}")
            return True
        logger.warning(f"No override found for: {symbol_upper}")
        return False

    def get_config_display(self, symbol: str = None) -> Dict:
        """Get configuration for display purposes."""
        if symbol:
            symbol_upper = symbol.upper()
            config = self._get_config_for_symbol(symbol_upper)
            asset_class = self.determine_asset_class(symbol_upper)

            is_override = symbol_upper in self.config["overrides"]

            result = {
                "symbol": symbol_upper,
                "type": config["type"],
                "value": config["value"],
                "asset_class": asset_class,
                "is_override": is_override,
            }

            if is_override:
                override = self.config["overrides"][symbol_upper]
                result["set_by"] = override.get("set_by", "Unknown")
                result["set_at"] = override.get("set_at", "Unknown")

            return result

        return {
            "defaults": self.config["defaults"],
            "overrides": self.config["overrides"],
            "total_overrides": len(self.config["overrides"]),
        }

    def format_distance_for_display(
        self, symbol: str, distance: float, current_price: float = None
    ) -> str:
        """Format distance for user-friendly display."""
        config = self._get_config_for_symbol(symbol)
        distance_type = config["type"]

        if distance_type == "pips":
            pip_size = self.get_pip_size(symbol)
            pips = distance / pip_size
            return f"{pips:.1f} pips"

        if distance_type == "dollars":
            return f"${distance:.2f}"

        if distance_type == "percentage":
            if current_price and current_price > 0:
                percentage = (distance / current_price) * 100
                return f"{percentage:.2f}%"
            return f"${distance:.2f}"

        return f"{distance:.5f}"

    # Backward compatibility
    def get_alert_config(self, symbol: str) -> Dict:
        """BACKWARD COMPATIBILITY: Get alert config in old format."""
        config = self._get_config_for_symbol(symbol)
        distance_type = config["type"]
        value = config["value"]
        pip_size = self.get_pip_size(symbol)

        if distance_type == "pips":
            return {"approaching_pips": value, "pip_size": pip_size}
        return {"approaching_distance": value, "pip_size": pip_size}


def get_alert_config() -> AlertDistanceConfig:
    """Get global alert config instance."""
    return AlertDistanceConfig()
