"""
Alert Distance Configuration for Trading Alert Bot
Manages approaching distances and pip sizes for different asset classes
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Union
import os

logger = logging.getLogger(__name__)


class AlertDistanceConfig:
    """
    Manages alert distance configuration for different symbols and asset classes

    Features:
    - Default configurations per asset class
    - Symbol-specific overrides
    - Dynamic updates via Discord commands
    - Minimum distance thresholds for noise filtering
    """

    def __init__(self, config_path: str = None):
        """Initialize alert configuration"""
        if config_path is None:
            # Find config relative to this file
            config_path = Path(__file__).parent.parent / 'config' / 'alert_distances.json'

        self.config_path = Path(config_path)
        self.config = self._load_config()

        # Import SymbolMapper here to avoid circular imports
        from price_feeds.symbol_mapper import SymbolMapper
        mapper_config = self.config_path.parent / 'symbol_mappings.json'
        self.mapper = SymbolMapper(str(mapper_config))

        logger.info(f"AlertDistanceConfig initialized from {config_path}")

    def _load_config(self) -> Dict:
        """Load alert configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}, using defaults")
            return self._get_default_config()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config: {e}, using defaults")
            return self._get_default_config()

    def _get_default_config(self) -> Dict:
        """Return default configuration if file not found"""
        return {
            'defaults': {
                'forex': {
                    'approaching_pips': 10,
                    'pip_size': 0.0001
                },
                'forex_jpy': {
                    'approaching_pips': 15,
                    'pip_size': 0.01
                },
                'metals': {
                    'approaching_distance': 1.00,
                    'pip_size': 0.01
                },
                'crypto': {
                    'approaching_distance': 50,
                    'pip_size': 1
                },
                'indices': {
                    'approaching_distance': 5.0,
                    'pip_size': 1
                },
                'stocks': {
                    'approaching_distance': 0.50,
                    'pip_size': 0.01
                }
            },
            'overrides': {},
            'dynamic_overrides': {}
        }

    def get_alert_config(self, symbol: str) -> Dict:
        """
        Get alert configuration for a specific symbol

        Args:
            symbol: Trading symbol (e.g., 'EURUSD', 'XAUUSD', 'BTCUSDT')

        Returns:
            Dict with approaching_distance/approaching_pips, pip_size, min_distance
        """
        symbol_upper = symbol.upper()

        # Check dynamic overrides first (set via Discord commands)
        dynamic = self.config.get('dynamic_overrides', {})
        if symbol_upper in dynamic:
            return dynamic[symbol_upper]

        # Check static overrides
        overrides = self.config.get('overrides', {})
        if symbol_upper in overrides:
            return overrides[symbol_upper]
        if symbol.lower() in overrides:
            return overrides[symbol.lower()]

        # Determine asset class and get defaults
        asset_class = self.mapper.determine_asset_class(symbol)

        # Special handling for JPY pairs
        if asset_class == 'forex' and 'JPY' in symbol_upper:
            return self.config['defaults'].get('forex_jpy',
                                               self.config['defaults']['forex'])

        # Return default for asset class
        defaults = self.config.get('defaults', {})
        if asset_class in defaults:
            return defaults[asset_class]

        # Ultimate fallback
        return {
            'approaching_distance': 10,
            'pip_size': 0.0001,
            'min_distance': 2
        }

    def get_approaching_distance(self, symbol: str) -> float:
        """
        Get the approaching distance for a symbol

        Returns distance in appropriate units (pips for forex, points for others)
        """
        config = self.get_alert_config(symbol)

        # Check for pips-based config (forex)
        if 'approaching_pips' in config:
            return config['approaching_pips']

        return config.get('approaching_distance', 10)

    def get_pip_size(self, symbol: str) -> float:
        """Get the pip size for a symbol"""
        config = self.get_alert_config(symbol)
        return config.get('pip_size', 0.0001)

    def should_alert_approaching(self, symbol: str, distance: float) -> bool:
        """
        Determine if an approaching alert should be sent

        Args:
            symbol: Trading symbol
            distance: Distance to limit (in pips or points)

        Returns:
            True if distance is within approaching threshold but not noise threshold
        """
        approaching = self.get_approaching_distance(symbol)
        # Alert if within approaching distance but not within noise
        return abs(distance) <= approaching

    async def update_override(self, symbol: str, distance: float,
                              pip_size: Optional[float] = None) -> bool:
        """
        Update dynamic override for a symbol (via Discord command)

        Args:
            symbol: Trading symbol
            distance: New approaching distance
            pip_size: Optional new pip size

        Returns:
            True if successful
        """
        try:
            symbol_upper = symbol.upper()

            # Initialize dynamic overrides if not exists
            if 'dynamic_overrides' not in self.config:
                self.config['dynamic_overrides'] = {}

            # Get current config as base
            current = self.get_alert_config(symbol)

            # Determine if this is pips or points based
            asset_class = self.mapper.determine_asset_class(symbol)

            if asset_class == 'forex':
                self.config['dynamic_overrides'][symbol_upper] = {
                    'approaching_pips': distance,
                    'pip_size': pip_size or current.get('pip_size', 0.0001),
                    'min_distance_pips': current.get('min_distance_pips', 2)
                }
            else:
                self.config['dynamic_overrides'][symbol_upper] = {
                    'approaching_distance': distance,
                    'pip_size': pip_size or current.get('pip_size', 0.01),
                    'min_distance': current.get('min_distance', 1)
                }

            # Save to file
            await self._save_config()

            logger.info(f"Updated alert config for {symbol_upper}: distance={distance}")
            return True

        except Exception as e:
            logger.error(f"Failed to update override for {symbol}: {e}")
            return False

    async def remove_override(self, symbol: str) -> bool:
        """Remove dynamic override for a symbol"""
        try:
            symbol_upper = symbol.upper()

            if 'dynamic_overrides' in self.config:
                if symbol_upper in self.config['dynamic_overrides']:
                    del self.config['dynamic_overrides'][symbol_upper]
                    await self._save_config()
                    logger.info(f"Removed override for {symbol_upper}")
                    return True

            return False

        except Exception as e:
            logger.error(f"Failed to remove override for {symbol}: {e}")
            return False

    async def _save_config(self):
        """Save configuration back to file"""
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            raise

    def reload_config(self):
        """Reload configuration from file"""
        self.config = self._load_config()
        logger.info("Alert configuration reloaded")

    def get_all_overrides(self) -> Dict[str, Dict]:
        """Get all configured overrides (static and dynamic)"""
        result = {}

        # Add static overrides
        for symbol, config in self.config.get('overrides', {}).items():
            result[symbol] = {'type': 'static', 'config': config}

        # Add dynamic overrides
        for symbol, config in self.config.get('dynamic_overrides', {}).items():
            result[symbol] = {'type': 'dynamic', 'config': config}

        return result

    def format_distance_for_display(self, symbol: str, distance: float) -> str:
        """
        Format distance for user-friendly display

        Args:
            symbol: Trading symbol
            distance: Distance value

        Returns:
            Formatted string like "10 pips" or "1.50 points"
        """
        config = self.get_alert_config(symbol)

        if 'approaching_pips' in config:
            return f"{distance:.1f} pips"
        else:
            # Determine decimal places based on pip size
            pip_size = config.get('pip_size', 0.01)
            if pip_size >= 1:
                return f"{distance:.0f} points"
            elif pip_size >= 0.1:
                return f"{distance:.1f} points"
            elif pip_size >= 0.01:
                return f"{distance:.2f} points"
            else:
                return f"{distance:.4f} points"


# Test function
def test_alert_config():
    """Test alert configuration"""
    config = AlertDistanceConfig()

    test_symbols = [
        'EURUSD',
        'USDJPY',
        'XAUUSD',
        'BTCUSDT',
        'spx500usd',
        'AAPL.NAS'
    ]

    print("\nAlert Configuration Test:")
    print("-" * 50)

    for symbol in test_symbols:
        alert_cfg = config.get_alert_config(symbol)
        distance = config.get_approaching_distance(symbol)
        pip_size = config.get_pip_size(symbol)
        min_dist = config.get_min_distance(symbol)
        formatted = config.format_distance_for_display(symbol, distance)

        print(f"\n{symbol}:")
        print(f"  Approaching: {formatted}")
        print(f"  Pip size: {pip_size}")
        print(f"  Min distance: {min_dist}")
        print(f"  Config: {alert_cfg}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_alert_config()