"""
Take Profit Configuration for Trading Alert Bot

Manages TP thresholds per asset class and per instrument.

Supported types:
  - pips     (forex)
  - dollars  (metals, indices, crypto, oil, stocks)

P&L is always calculated in the same native unit as the TP type:
  - pips for forex/forex_jpy
  - dollars for everything else
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Literal

from ._base_config import BaseThresholdConfig

logger = logging.getLogger(__name__)

TPType = Literal["pips", "dollars"]


class TPConfig(BaseThresholdConfig):
    """
    Manages take-profit configuration with per-asset-class defaults
    and per-symbol overrides.

    The TP value defines how many pips / dollars the LAST HIT LIMIT
    must be in profit before auto-close is triggered.

    For signals with multiple limits, all non-last limits must have a
    combined P&L >= 0 (breakeven) at the same moment.
    """

    CONFIG_FILENAME = "tp_configuration.json"

    ASSET_CLASS_TYPES: Dict[str, TPType] = {
        "forex": "pips",
        "forex_jpy": "pips",
        "metals": "dollars",
        "indices": "dollars",
        "stocks": "dollars",
        "crypto": "dollars",
        "oil": "dollars",
    }

    def __init__(self, config_path: str = None):
        super().__init__(config_path)
        logger.info("TPConfig initialised")

    # === Config defaults & validation ===

    def _create_default_config(self) -> Dict:
        config = {
            "defaults": {
                "forex": {"type": "pips", "value": 10.0, "description": "Standard forex pairs"},
                "forex_jpy": {
                    "type": "pips",
                    "value": 10.0,
                    "description": "JPY pairs (auto-detected)",
                },
                "metals": {"type": "dollars", "value": 5.0, "description": "Gold, Silver, etc."},
                "indices": {"type": "dollars", "value": 20.0, "description": "Stock indices"},
                "stocks": {"type": "dollars", "value": 1.0, "description": "Individual stocks"},
                "crypto": {"type": "dollars", "value": 50.0, "description": "Cryptocurrencies"},
                "oil": {"type": "dollars", "value": 0.5, "description": "Oil commodities"},
            },
            "scalp_defaults": {
                "forex": {
                    "type": "pips",
                    "value": 3.0,
                    "description": "Scalp - Standard forex pairs",
                },
                "forex_jpy": {
                    "type": "pips",
                    "value": 5.0,
                    "description": "Scalp - JPY pairs (auto-detected)",
                },
                "metals": {
                    "type": "dollars",
                    "value": 2.0,
                    "description": "Scalp - Gold, Silver, etc.",
                },
                "indices": {
                    "type": "dollars",
                    "value": 10.0,
                    "description": "Scalp - Stock indices",
                },
                "stocks": {
                    "type": "dollars",
                    "value": 0.5,
                    "description": "Scalp - Individual stocks",
                },
                "crypto": {
                    "type": "dollars",
                    "value": 20.0,
                    "description": "Scalp - Cryptocurrencies",
                },
                "oil": {"type": "dollars", "value": 0.2, "description": "Scalp - Oil commodities"},
            },
            "overrides": {},
            "scalp_overrides": {},
        }
        self._save_config(config)
        return config

    def _validate_config(self):
        super()._validate_config()

        if "scalp_defaults" not in self.config:
            self.config["scalp_defaults"] = self._create_default_config()["scalp_defaults"]
        if "scalp_overrides" not in self.config:
            self.config["scalp_overrides"] = {}

        for section in ("defaults", "scalp_defaults"):
            for asset_class, settings in self.config.get(section, {}).items():
                if not isinstance(settings, dict):
                    logger.error(f"Invalid TP settings for {asset_class} in {section}")
                    continue
                if "type" not in settings:
                    settings["type"] = self.ASSET_CLASS_TYPES.get(asset_class, "dollars")
                if "value" not in settings:
                    settings["value"] = 5.0
                if "description" not in settings:
                    settings["description"] = f"Default for {asset_class}"

    # === Public API ===

    def _get_config_for_symbol(self, symbol: str, scalp: bool = False) -> Dict:
        """Return {type, value} for a symbol, respecting overrides and scalp mode."""
        s = symbol.upper()

        if scalp:
            if s in self.config.get("scalp_overrides", {}):
                ov = self.config["scalp_overrides"][s]
                return {"type": ov["type"], "value": ov["value"]}
            asset_class = self.determine_asset_class(s)
            scalp_defaults = self.config.get("scalp_defaults", {})
            if asset_class in scalp_defaults:
                d = scalp_defaults[asset_class]
                return {"type": d["type"], "value": d["value"]}

        if s in self.config["overrides"]:
            ov = self.config["overrides"][s]
            return {"type": ov["type"], "value": ov["value"]}

        asset_class = self.determine_asset_class(s)
        if asset_class in self.config["defaults"]:
            d = self.config["defaults"][asset_class]
            return {"type": d["type"], "value": d["value"]}

        logger.warning(f"No TP config for {s}, using fallback $5")
        return {"type": "dollars", "value": 5.0}

    def get_tp_value(self, symbol: str, scalp: bool = False) -> float:
        """Return the TP threshold in its native unit (pips or dollars)."""
        return self._get_config_for_symbol(symbol, scalp=scalp)["value"]

    def get_tp_type(self, symbol: str, scalp: bool = False) -> TPType:
        """Return 'pips' or 'dollars' for the symbol."""
        return self._get_config_for_symbol(symbol, scalp=scalp)["type"]  # type: ignore

    def calculate_pnl(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        current_price: float,
        scalp: bool = False,
    ) -> float:
        """
        Calculate P&L for a single limit position in native units.

        For 'pips' instruments: result is in pips (positive = profit).
        For 'dollars' instruments: result is in dollars per unit (positive = profit).
        """
        tp_type = self.get_tp_type(symbol, scalp=scalp)

        if direction == "long":
            raw_diff = current_price - entry_price
        else:
            raw_diff = entry_price - current_price

        if tp_type == "pips":
            pip_size = self.get_pip_size(symbol)
            return raw_diff / pip_size
        return raw_diff

    def set_override(
        self, symbol: str, value: float, tp_type: TPType, set_by: str = "User", scalp: bool = False
    ) -> bool:
        """Set a per-symbol TP override."""
        if tp_type not in ("pips", "dollars"):
            logger.error(f"Invalid TP type: {tp_type}")
            return False
        if value <= 0:
            logger.error(f"TP value must be positive, got {value}")
            return False

        section = "scalp_overrides" if scalp else "overrides"
        if section not in self.config:
            self.config[section] = {}

        self.config[section][symbol.upper()] = {
            "type": tp_type,
            "value": value,
            "set_by": set_by,
            "set_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_config()
        logger.info(
            f"Set {'scalp ' if scalp else ''}TP override: {symbol.upper()} = {value} {tp_type}"
        )
        return True

    def set_default(
        self,
        asset_class: str,
        value: float,
        tp_type: TPType,
        set_by: str = "User",
        scalp: bool = False,
    ) -> bool:
        """Update the default TP for an asset class."""
        section = "scalp_defaults" if scalp else "defaults"
        if asset_class not in self.config.get(section, {}):
            logger.error(f"Unknown asset class: {asset_class} in {section}")
            return False
        if tp_type not in ("pips", "dollars"):
            logger.error(f"Invalid TP type: {tp_type}")
            return False
        if value <= 0:
            logger.error(f"TP value must be positive, got {value}")
            return False

        self.config[section][asset_class]["value"] = value
        self.config[section][asset_class]["type"] = tp_type
        self._save_config()
        logger.info(f"Set {'scalp ' if scalp else ''}TP default: {asset_class} = {value} {tp_type}")
        return True

    def remove_override(self, symbol: str, scalp: bool = False) -> bool:
        """Remove a per-symbol override."""
        s = symbol.upper()
        section = "scalp_overrides" if scalp else "overrides"
        if s in self.config.get(section, {}):
            del self.config[section][s]
            self._save_config()
            logger.info(f"Removed {'scalp ' if scalp else ''}TP override: {s}")
            return True
        return False

    def get_display_info(self, symbol: str = None, scalp: bool = False) -> Dict:
        """Return formatted config dict for display in Discord."""
        if symbol:
            s = symbol.upper()
            cfg = self._get_config_for_symbol(s, scalp=scalp)
            asset_class = self.determine_asset_class(s)
            section = "scalp_overrides" if scalp else "overrides"
            is_override = s in self.config.get(section, {})
            result = {
                "symbol": s,
                "type": cfg["type"],
                "value": cfg["value"],
                "asset_class": asset_class,
                "is_override": is_override,
                "scalp": scalp,
            }
            if is_override:
                ov = self.config[section][s]
                result["set_by"] = ov.get("set_by", "Unknown")
                result["set_at"] = ov.get("set_at", "Unknown")
            return result

        return {
            "defaults": self.config["defaults"],
            "scalp_defaults": self.config.get("scalp_defaults", {}),
            "overrides": self.config["overrides"],
            "scalp_overrides": self.config.get("scalp_overrides", {}),
            "total_overrides": len(self.config["overrides"]),
            "total_scalp_overrides": len(self.config.get("scalp_overrides", {})),
        }

    def format_value(self, symbol: str, value: float) -> str:
        """Format a TP/P&L value with the correct unit label."""
        tp_type = self.get_tp_type(symbol)
        if tp_type == "pips":
            return f"{value:.1f} pips"
        return f"${value:.2f}"
