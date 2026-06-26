"""
Trailing-stop shadow-simulation configuration.

Defines three what-if trailing distances (tight/medium/loose) per signal
type and asset class, mirroring TPConfig's resolution chain. Defaults are
seeded as multiples of the live TP configuration at first creation, so the
starting point reflects values already tuned in production rather than
arbitrary guesses.

Supported types:
  - pips     (forex)
  - dollars  (metals, indices, crypto, oil, stocks)
"""

import logging
from typing import Dict, Literal

from ._base_config import BaseThresholdConfig
from .tp_config import TPConfig

logger = logging.getLogger(__name__)

DistanceType = Literal["pips", "dollars"]

VALID_SIGNAL_TYPES = ("standard", "scalp", "swing", "toll", "pa", "1-1")
LEVELS = ("tight", "medium", "loose")

# Seed multipliers applied to the live TP value at first creation.
LEVEL_MULTIPLIERS = {"tight": 0.5, "medium": 1.0, "loose": 2.0}


class TrailingStopConfig(BaseThresholdConfig):
    """
    Manages trailing-stop shadow-simulation distances with per-signal-type
    defaults and per-symbol overrides.

    Each level's value defines how far price must retrace from the
    high-water-mark (in native pips/dollars) before that level's simulated
    trail is considered stopped out.
    """

    CONFIG_FILENAME = "trailing_configuration.json"
    ASSET_CLASS_TYPES = TPConfig.ASSET_CLASS_TYPES

    def __init__(self, tp_config: TPConfig, config_path: str = None):
        self._tp_config = tp_config
        super().__init__(config_path)
        logger.info("TrailingStopConfig initialised")

    # === Config defaults & validation ===

    @staticmethod
    def _scale(tp_entry: Dict) -> Dict:
        value = tp_entry["value"]
        return {
            "type": tp_entry["type"],
            **{level: value * mult for level, mult in LEVEL_MULTIPLIERS.items()},
        }

    def _create_default_config(self) -> Dict:
        config = {
            "type_defaults": {t: {} for t in VALID_SIGNAL_TYPES},
            "type_overrides": {t: {} for t in VALID_SIGNAL_TYPES},
        }

        tp_defaults = self._tp_config.config.get("type_defaults", {})
        tp_overrides = self._tp_config.config.get("type_overrides", {})

        for signal_type in VALID_SIGNAL_TYPES:
            for asset_class, entry in tp_defaults.get(signal_type, {}).items():
                config["type_defaults"][signal_type][asset_class] = self._scale(entry)
            for symbol, entry in tp_overrides.get(signal_type, {}).items():
                config["type_overrides"][signal_type][symbol] = self._scale(entry)

        self._save_config(config)
        logger.info("Seeded trailing-stop defaults from live TP configuration")
        return config

    def _validate_config(self):
        if "type_defaults" not in self.config or "type_overrides" not in self.config:
            logger.error("Trailing config missing type_defaults/type_overrides; resetting")
            self.config = self._create_default_config()
            return

        for t in VALID_SIGNAL_TYPES:
            self.config["type_defaults"].setdefault(t, {})
            self.config["type_overrides"].setdefault(t, {})

        for type_name, asset_classes in self.config["type_defaults"].items():
            for asset_class, settings in asset_classes.items():
                if not isinstance(settings, dict):
                    logger.error(f"Invalid trailing settings for {type_name}.{asset_class}")
                    continue
                settings.setdefault("type", self.ASSET_CLASS_TYPES.get(asset_class, "dollars"))
                for level in LEVELS:
                    settings.setdefault(level, 5.0 * LEVEL_MULTIPLIERS[level])

    # === Public API ===

    @staticmethod
    def _normalize_type(signal_type) -> str:
        if signal_type in VALID_SIGNAL_TYPES:
            return signal_type
        return "standard"

    def _get_config_for_symbol(self, symbol: str, signal_type: str = "standard") -> Dict:
        """Return {type, tight, medium, loose} for a symbol given a signal type.

        Resolution order mirrors TPConfig:
          1. type_overrides[signal_type][SYMBOL]
          2. type_overrides[standard][SYMBOL]
          3. type_defaults[signal_type][asset_class]
          4. type_defaults[standard][asset_class]
          5. hard fallback ($5 base)
        """
        signal_type = self._normalize_type(signal_type)
        s = symbol.upper()
        asset_class = self.determine_asset_class(s)

        per_type_overrides = self.config["type_overrides"].get(signal_type, {})
        if s in per_type_overrides:
            return dict(per_type_overrides[s])

        standard_overrides = self.config["type_overrides"].get("standard", {})
        if signal_type != "standard" and s in standard_overrides:
            return dict(standard_overrides[s])

        per_type_defaults = self.config["type_defaults"].get(signal_type, {})
        if asset_class in per_type_defaults:
            return dict(per_type_defaults[asset_class])

        standard_defaults = self.config["type_defaults"].get("standard", {})
        if asset_class in standard_defaults:
            return dict(standard_defaults[asset_class])

        logger.warning(f"No trailing config for {s} (type={signal_type}), using fallback")
        return {"type": "dollars", **{l: 5.0 * LEVEL_MULTIPLIERS[l] for l in LEVELS}}

    def get_levels(self, symbol: str, signal_type: str = "standard") -> Dict[str, float]:
        """Return {"tight": x, "medium": y, "loose": z} distances in native units."""
        cfg = self._get_config_for_symbol(symbol, signal_type=signal_type)
        return {level: cfg[level] for level in LEVELS}

    def get_distance_type(self, symbol: str, signal_type: str = "standard") -> DistanceType:
        return self._get_config_for_symbol(symbol, signal_type=signal_type)["type"]  # type: ignore
