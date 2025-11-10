"""
Alert Distance Configuration for Trading Alert Bot - REFACTORED FOR PHASE 2 PART 1
FIXED VERSION - Properly handles migration from old config format

Manages alert distances with support for multiple distance types (pips, dollars, percentage)

FEATURES:
- Pips-based distances (forex)
- Dollar-based distances (metals, can be used for any asset)
- Percentage-based distances (indices, stocks, crypto, can be used for any asset)
- Asset-specific defaults
- Persistent manual overrides
- Automatic JPY pair detection
- Config validation and migration
"""

import json
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional, Literal
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DistanceType = Literal["pips", "dollars", "percentage"]


class AlertDistanceConfig:
    """
    Manages alert distance configuration with support for multiple distance types

    Supports:
    - Pips (forex)
    - Dollars (metals, can be used for indices/stocks/crypto)
    - Percentage (indices, stocks, crypto, can be used for anything)

    Features:
    - Asset-specific defaults
    - Persistent manual overrides
    - Automatic JPY pair detection
    - Config validation and migration
    """

    def __init__(self, config_path: str = None):
        """Initialize alert distance configuration"""
        if config_path is None:
            self.config_path = Path(__file__).resolve().parent.parent / 'config' / 'alert_distances.json'
        else:
            self.config_path = Path(config_path)

        self.config = self._load_config()
        self._validate_config()

        # Import SymbolMapper for asset class detection
        try:
            from price_feeds.symbol_mapper import SymbolMapper
            mapper_config = self.config_path.parent / 'symbol_mappings.json'
            self.mapper = SymbolMapper(str(mapper_config))
        except Exception as e:
            logger.warning(f"Could not initialize SymbolMapper: {e}, using fallback detection")
            self.mapper = None

        logger.info("AlertDistanceConfig initialized (Phase 2 - Fixed Migration)")

    def _load_config(self) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            # Check if migration needed (old format)
            if 'defaults' not in config or not self._is_new_format(config):
                logger.warning("Old config format detected, migrating...")
                config = self._migrate_old_config(config)
                self._save_config(config)

            return config

        except FileNotFoundError:
            logger.warning(f"Config file not found, creating default: {self.config_path}")
            return self._create_default_config()

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e}")
            logger.warning("Using default configuration")
            return self._create_default_config()

    def _is_new_format(self, config: Dict) -> bool:
        """Check if config is in new format with type/value/description"""
        if 'defaults' not in config:
            return False

        defaults = config['defaults']
        if not isinstance(defaults, dict):
            return False

        # Check if at least one asset class has the new format
        for asset_class, settings in defaults.items():
            if isinstance(settings, dict):
                # New format has 'type' and 'value' keys
                if 'type' in settings and 'value' in settings:
                    return True
                # Old format has 'approaching_pips' or 'approaching_distance'
                if 'approaching_pips' in settings or 'approaching_distance' in settings:
                    return False

        return False

    def _create_default_config(self) -> Dict:
        """Create default configuration"""
        config = {
            "defaults": {
                "forex": {
                    "type": "pips",
                    "value": 10.0,
                    "description": "Standard forex pairs"
                },
                "forex_jpy": {
                    "type": "pips",
                    "value": 20.0,
                    "description": "JPY pairs (auto-detected)"
                },
                "metals": {
                    "type": "dollars",
                    "value": 10.0,
                    "description": "Gold, Silver, etc."
                },
                "indices": {
                    "type": "percentage",
                    "value": 1.0,
                    "description": "Stock indices"
                },
                "stocks": {
                    "type": "percentage",
                    "value": 1.0,
                    "description": "Individual stocks"
                },
                "crypto": {
                    "type": "percentage",
                    "value": 0.5,
                    "description": "Cryptocurrencies"
                },
                "oil": {
                    "type": "dollars",
                    "value": 0.5,
                    "description": "Oil commodities"
                }
            },
            "overrides": {}
        }

        self._save_config(config)
        return config

    def _migrate_old_config(self, old_config: Dict) -> Dict:
        """
        Migrate old config to new nested structure

        Handles multiple old formats:
        1. Flat format: {"forex": 10.0, "metals": 10.0}
        2. Nested with approaching_pips: {"defaults": {"forex": {"approaching_pips": 10, "pip_size": 0.0001}}}
        3. Nested with approaching_distance: {"defaults": {"metals": {"approaching_distance": 10, "pip_size": 0.01}}}
        """
        logger.info("Starting config migration...")

        # Create new config with proper structure
        new_config = self._create_default_config()

        # Determine old format type
        if 'defaults' in old_config:
            # Format 2 or 3: Has 'defaults' key
            old_defaults = old_config['defaults']

            for asset_class, settings in old_defaults.items():
                if not isinstance(settings, dict):
                    continue

                # Determine the distance type for this asset class
                if asset_class in ['forex', 'forex_jpy']:
                    distance_type = "pips"
                elif asset_class in ['metals', 'oil']:
                    distance_type = "dollars"
                elif asset_class in ['indices', 'stocks', 'crypto']:
                    distance_type = "percentage"
                else:
                    distance_type = "pips"  # Default

                # Extract value from old format
                value = None
                if 'approaching_pips' in settings:
                    value = settings['approaching_pips']
                    distance_type = "pips"  # Override if explicitly using pips
                elif 'approaching_distance' in settings:
                    value = settings['approaching_distance']

                if value is not None:
                    # For percentage type, convert large values to reasonable percentages
                    if distance_type == "percentage" and value > 10:
                        # Old config had pip-based values for indices/stocks
                        # Convert to reasonable percentage
                        if asset_class == 'indices':
                            value = 1.0  # 1%
                        elif asset_class == 'crypto':
                            value = 0.5  # 0.5%
                        else:
                            value = 1.0  # 1%

                    # Update the new config
                    if asset_class in new_config["defaults"]:
                        new_config["defaults"][asset_class]["value"] = value
                        new_config["defaults"][asset_class]["type"] = distance_type
        else:
            # Format 1: Flat format
            for asset_class, value in old_config.items():
                if asset_class in ['overrides', 'dynamic_overrides']:
                    continue

                if isinstance(value, (int, float)):
                    # Determine type based on asset class
                    if asset_class in ['forex', 'forex_jpy']:
                        distance_type = "pips"
                    elif asset_class in ['metals', 'oil']:
                        distance_type = "dollars"
                    elif asset_class in ['indices', 'stocks', 'crypto']:
                        distance_type = "percentage"
                        # Convert large values to reasonable percentages
                        if value > 10:
                            value = 1.0 if asset_class != 'crypto' else 0.5
                    else:
                        distance_type = "pips"

                    # Update if this asset class exists in defaults
                    if asset_class in new_config["defaults"]:
                        new_config["defaults"][asset_class]["value"] = value
                        new_config["defaults"][asset_class]["type"] = distance_type

        # Migrate any existing overrides
        if 'overrides' in old_config and isinstance(old_config['overrides'], dict):
            for symbol, settings in old_config['overrides'].items():
                if isinstance(settings, dict):
                    # Extract override value and type
                    if 'approaching_pips' in settings:
                        new_config['overrides'][symbol] = {
                            "type": "pips",
                            "value": settings['approaching_pips'],
                            "set_by": "Migration",
                            "set_at": datetime.now(timezone.utc).isoformat()
                        }
                    elif 'approaching_distance' in settings:
                        new_config['overrides'][symbol] = {
                            "type": "dollars",
                            "value": settings['approaching_distance'],
                            "set_by": "Migration",
                            "set_at": datetime.now(timezone.utc).isoformat()
                        }

        # Migrate dynamic_overrides if present
        if 'dynamic_overrides' in old_config and isinstance(old_config['dynamic_overrides'], dict):
            for symbol, settings in old_config['dynamic_overrides'].items():
                if isinstance(settings, dict) and symbol not in new_config['overrides']:
                    # Extract override value and type
                    if 'approaching_pips' in settings:
                        new_config['overrides'][symbol] = {
                            "type": "pips",
                            "value": settings['approaching_pips'],
                            "set_by": "Migration",
                            "set_at": datetime.now(timezone.utc).isoformat()
                        }
                    elif 'approaching_distance' in settings:
                        new_config['overrides'][symbol] = {
                            "type": "dollars",
                            "value": settings['approaching_distance'],
                            "set_by": "Migration",
                            "set_at": datetime.now(timezone.utc).isoformat()
                        }

        logger.info("Configuration migrated successfully")
        return new_config

    def _validate_config(self):
        """Validate configuration structure"""
        required_keys = ["defaults", "overrides"]

        for key in required_keys:
            if key not in self.config:
                logger.error(f"Missing required key in config: {key}")
                self.config = self._create_default_config()
                return

        # Validate defaults - ensure they all have type and value
        for asset_class, settings in self.config["defaults"].items():
            if not isinstance(settings, dict):
                logger.error(f"Invalid settings for {asset_class}: not a dict")
                self.config["defaults"][asset_class] = {
                    "type": "pips",
                    "value": 10.0,
                    "description": "Default"
                }
                continue

            if "type" not in settings:
                logger.error(f"Invalid settings for {asset_class}: missing 'type'")
                settings["type"] = "pips"

            if "value" not in settings:
                logger.error(f"Invalid settings for {asset_class}: missing 'value'")
                settings["value"] = 10.0

            if "description" not in settings:
                settings["description"] = f"Default for {asset_class}"

        logger.debug("Configuration validated successfully")

    def _save_config(self, config: Dict = None):
        """Save configuration to JSON file"""
        if config is None:
            config = self.config

        try:
            # Ensure directory exists
            self.config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            raise

    def get_approaching_distance(self, symbol: str, current_price: float = None) -> float:
        """
        Get approaching alert distance for a symbol in absolute price units

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
            # Convert pips to price units
            pip_size = self.get_pip_size(symbol)
            return value * pip_size

        elif distance_type == "dollars":
            # Dollars are already in price units
            return value

        elif distance_type == "percentage":
            # Convert percentage to price units
            if current_price is None:
                logger.error(f"Current price required for percentage calculation: {symbol}")
                # Fallback to a reasonable default
                return self._get_fallback_distance(symbol)

            return (value / 100.0) * current_price

        else:
            logger.error(f"Unknown distance type: {distance_type}")
            return self._get_fallback_distance(symbol)

    def _get_config_for_symbol(self, symbol: str) -> Dict:
        """
        Get configuration for a specific symbol (check overrides first)

        Args:
            symbol: Trading symbol

        Returns:
            Dict with 'type' and 'value' keys
        """
        symbol_upper = symbol.upper()

        # Check for override first
        if symbol_upper in self.config["overrides"]:
            override = self.config["overrides"][symbol_upper]
            return {
                "type": override["type"],
                "value": override["value"]
            }

        # Use default based on asset class
        asset_class = self._determine_asset_class(symbol)

        if asset_class in self.config["defaults"]:
            default = self.config["defaults"][asset_class]
            return {
                "type": default["type"],
                "value": default["value"]
            }

        # Ultimate fallback
        logger.warning(f"No config found for {symbol}, using forex default")
        return {
            "type": "pips",
            "value": 10.0
        }

    def _determine_asset_class(self, symbol: str) -> str:
        """
        Determine asset class of a symbol

        Returns: 'forex', 'forex_jpy', 'metals', 'indices', 'stocks', 'crypto', 'oil'
        """
        # Use SymbolMapper if available
        if self.mapper:
            try:
                asset_class = self.mapper.determine_asset_class(symbol)
                return asset_class
            except Exception as e:
                logger.warning(f"SymbolMapper failed for {symbol}: {e}, using fallback")

        # Fallback detection
        symbol_upper = symbol.upper()

        # Check crypto
        if any(crypto in symbol_upper for crypto in ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'DOT']):
            return 'crypto'
        if 'USDT' in symbol_upper:
            return 'crypto'

        # Check metals
        if any(metal in symbol_upper for metal in ['XAU', 'XAG', 'GOLD', 'SILVER']):
            return 'metals'

        # Check oil
        if any(oil in symbol_upper for oil in ['WTI', 'BRENT', 'OIL', 'USOIL', 'USOILSPOT']):
            return 'oil'

        # Check indices
        if any(idx in symbol_upper for idx in ['SPX', 'NAS', 'DOW', 'DAX', 'CHINA50', 'US500', 'USTEC', 'US30',
                                               'US2000', 'RUSSELL', 'GER', 'DE30', 'DE40', 'JP225', 'NIKKEI']):
            return 'indices'

        # Check stocks
        if '.' in symbol or any(exchange in symbol_upper for exchange in ['.NAS', '.NYSE', '.LON']):
            return 'stocks'

        # Check forex - JPY pairs get special handling
        forex_currencies = ['EUR', 'USD', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF']

        if len(symbol_upper) == 6:
            currency1 = symbol_upper[:3]
            currency2 = symbol_upper[3:]

            if currency1 in forex_currencies and currency2 in forex_currencies:
                if 'JPY' in symbol_upper:
                    return 'forex_jpy'
                return 'forex'

        # Default
        return 'forex'

    def get_pip_size(self, symbol: str) -> float:
        """
        Get pip size for a symbol

        Args:
            symbol: Trading symbol

        Returns:
            Pip size (e.g., 0.0001 for EURUSD, 0.01 for USDJPY)
        """
        symbol_upper = symbol.upper()

        # JPY pairs use 0.01
        if 'JPY' in symbol_upper:
            return 0.01

        # Indices use 1.0 (points)
        if any(idx in symbol_upper for idx in ['SPX', 'NAS', 'DOW', 'DAX', 'US500', 'USTEC', 'US30']):
            return 1.0

        # Metals
        if 'XAU' in symbol_upper or 'GOLD' in symbol_upper:
            return 0.01
        if 'XAG' in symbol_upper or 'SILVER' in symbol_upper:
            return 0.001

        # Crypto - varies widely
        if 'BTC' in symbol_upper:
            return 1.0  # $1 per pip for BTC

        # Default forex
        return 0.0001

    def _get_fallback_distance(self, symbol: str) -> float:
        """Fallback distance when calculation fails"""
        asset_class = self._determine_asset_class(symbol)

        fallbacks = {
            'forex': 0.0010,  # 10 pips
            'forex_jpy': 0.20,  # 20 pips
            'metals': 10.0,  # $10
            'indices': 50.0,  # 50 points
            'stocks': 1.0,  # $1
            'crypto': 100.0,  # $100
            'oil': 0.5  # $0.50
        }

        return fallbacks.get(asset_class, 0.0010)

    def set_override(self, symbol: str, value: float, distance_type: DistanceType,
                    set_by: str = "User") -> bool:
        """
        Set a manual override for a symbol

        Args:
            symbol: Trading symbol
            value: Distance value
            distance_type: 'pips', 'dollars', or 'percentage'
            set_by: Who set this override

        Returns:
            Success status
        """
        # Validate inputs
        if distance_type not in ["pips", "dollars", "percentage"]:
            logger.error(f"Invalid distance type: {distance_type}")
            return False

        if value <= 0:
            logger.error(f"Invalid value: {value}")
            return False

        symbol_upper = symbol.upper()

        # Add override
        self.config["overrides"][symbol_upper] = {
            "type": distance_type,
            "value": value,
            "set_by": set_by,
            "set_at": datetime.now(timezone.utc).isoformat()
        }

        # Save to file
        self._save_config()

        logger.info(f"Set alert distance override: {symbol_upper} = {value} {distance_type}")
        return True

    def remove_override(self, symbol: str) -> bool:
        """
        Remove a manual override for a symbol

        Args:
            symbol: Trading symbol

        Returns:
            Success status
        """
        symbol_upper = symbol.upper()

        if symbol_upper in self.config["overrides"]:
            del self.config["overrides"][symbol_upper]
            self._save_config()
            logger.info(f"Removed alert distance override: {symbol_upper}")
            return True
        else:
            logger.warning(f"No override found for: {symbol_upper}")
            return False

    def get_config_display(self, symbol: str = None) -> Dict:
        """
        Get configuration for display purposes

        Args:
            symbol: Optional symbol to show specific config

        Returns:
            Dict with formatted configuration info
        """
        if symbol:
            # Show specific symbol config
            symbol_upper = symbol.upper()
            config = self._get_config_for_symbol(symbol_upper)
            asset_class = self._determine_asset_class(symbol_upper)

            is_override = symbol_upper in self.config["overrides"]

            result = {
                "symbol": symbol_upper,
                "type": config["type"],
                "value": config["value"],
                "asset_class": asset_class,
                "is_override": is_override
            }

            if is_override:
                override = self.config["overrides"][symbol_upper]
                result["set_by"] = override.get("set_by", "Unknown")
                result["set_at"] = override.get("set_at", "Unknown")

            return result

        else:
            # Show all configuration
            return {
                "defaults": self.config["defaults"],
                "overrides": self.config["overrides"],
                "total_overrides": len(self.config["overrides"])
            }

    def format_distance_for_display(self, symbol: str, distance: float, current_price: float = None) -> str:
        """
        Format distance for user-friendly display

        Args:
            symbol: Trading symbol
            distance: Distance in absolute price units
            current_price: Optional current price for percentage calculation

        Returns:
            Formatted string (e.g., "10.5 pips", "$5.30", "1.2%")
        """
        config = self._get_config_for_symbol(symbol)
        distance_type = config["type"]

        if distance_type == "pips":
            pip_size = self.get_pip_size(symbol)
            pips = distance / pip_size
            return f"{pips:.1f} pips"

        elif distance_type == "dollars":
            return f"${distance:.2f}"

        elif distance_type == "percentage":
            # Calculate percentage if we have current price
            if current_price and current_price > 0:
                percentage = (distance / current_price) * 100
                return f"{percentage:.2f}%"
            else:
                # Fallback to showing as dollars
                return f"${distance:.2f}"

        else:
            return f"{distance:.5f}"

    # BACKWARD COMPATIBILITY METHODS
    def get_alert_config(self, symbol: str) -> Dict:
        """
        BACKWARD COMPATIBILITY: Get alert config in old format

        This maintains compatibility with existing code that expects
        the old format with approaching_pips/approaching_distance and pip_size
        """
        config = self._get_config_for_symbol(symbol)
        distance_type = config["type"]
        value = config["value"]
        pip_size = self.get_pip_size(symbol)

        if distance_type == "pips":
            return {
                "approaching_pips": value,
                "pip_size": pip_size
            }
        else:
            return {
                "approaching_distance": value,
                "pip_size": pip_size
            }

    def reload_config(self):
        """Reload configuration from file"""
        self.config = self._load_config()
        self._validate_config()
        logger.info("Alert configuration reloaded")


# For backward compatibility
def get_alert_config() -> AlertDistanceConfig:
    """Get global alert config instance"""
    return AlertDistanceConfig()