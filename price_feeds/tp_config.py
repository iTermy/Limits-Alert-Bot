"""
Take Profit Configuration for Trading Alert Bot

Manages TP thresholds per signal type (standard, scalp, swing, toll, pa, 1-1),
per asset class, and per symbol.

Supported types:
  - pips     (forex)
  - dollars  (metals, indices, crypto, oil, stocks)

P&L is always calculated in the same native unit as the TP type:
  - pips for forex/forex_jpy
  - dollars for everything else
"""

import copy
import logging
from datetime import datetime, timezone
from typing import Dict, Literal

from ._base_config import BaseThresholdConfig

logger = logging.getLogger(__name__)

TPType = Literal["pips", "dollars"]

VALID_SIGNAL_TYPES = ("standard", "scalp", "swing", "toll", "pa", "1-1", "risky")


class TPConfig(BaseThresholdConfig):
    """
    Manages take-profit configuration with per-signal-type defaults
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

    @staticmethod
    def _standard_defaults() -> Dict:
        return {
            "forex": {"type": "pips", "value": 5.0, "description": "Standard forex pairs"},
            "forex_jpy": {"type": "pips", "value": 10.0, "description": "JPY pairs"},
            "metals": {"type": "dollars", "value": 5.0, "description": "Gold, Silver, etc."},
            "indices": {"type": "dollars", "value": 20.0, "description": "Stock indices"},
            "stocks": {"type": "dollars", "value": 1.0, "description": "Individual stocks"},
            "crypto": {"type": "dollars", "value": 50.0, "description": "Cryptocurrencies"},
            "oil": {"type": "dollars", "value": 0.5, "description": "Oil commodities"},
        }

    @staticmethod
    def _scalp_defaults() -> Dict:
        return {
            "forex": {"type": "pips", "value": 3.0, "description": "Scalp - forex"},
            "forex_jpy": {"type": "pips", "value": 5.0, "description": "Scalp - JPY pairs"},
            "metals": {"type": "dollars", "value": 2.0, "description": "Scalp - metals"},
            "indices": {"type": "dollars", "value": 10.0, "description": "Scalp - indices"},
            "stocks": {"type": "dollars", "value": 0.5, "description": "Scalp - stocks"},
            "crypto": {"type": "dollars", "value": 20.0, "description": "Scalp - crypto"},
            "oil": {"type": "dollars", "value": 0.2, "description": "Scalp - oil"},
        }

    @classmethod
    def _swing_defaults(cls) -> Dict:
        # Swing = 3x standard.
        out = {}
        for asset_class, cfg in cls._standard_defaults().items():
            out[asset_class] = {
                "type": cfg["type"],
                "value": cfg["value"] * 3,
                "description": f"Swing - {asset_class}",
            }
        return out

    @staticmethod
    def _one_one_defaults() -> Dict:
        # $10 from the last hit limit for metals (gold-1-1-rr default).
        # Other asset classes fall back to standard via the resolution chain.
        return {
            "metals": {"type": "dollars", "value": 10.0, "description": "1-1 RR - metals"},
        }

    def _create_default_config(self) -> Dict:
        config = {
            "type_defaults": {
                "standard": self._standard_defaults(),
                "scalp": self._scalp_defaults(),
                "swing": self._swing_defaults(),
                "toll": self._scalp_defaults(),  # initialised from scalp; user-adjustable
                "pa": self._standard_defaults(),  # initialised from standard; user-adjustable
                "1-1": self._one_one_defaults(),
                "risky": self._scalp_defaults(),  # initialised from scalp; user-adjustable
            },
            "type_overrides": {t: {} for t in VALID_SIGNAL_TYPES},
        }
        self._save_config(config)
        return config

    def _validate_config(self):
        if "type_defaults" not in self.config or "type_overrides" not in self.config:
            logger.error("TP config missing type_defaults/type_overrides; resetting to defaults")
            self.config = self._create_default_config()
            return

        # Ensure every valid signal type has its own bucket.
        for t in VALID_SIGNAL_TYPES:
            self.config["type_defaults"].setdefault(t, {})
            self.config["type_overrides"].setdefault(t, {})

        # Seed the risky config from the loaded scalp config for files written
        # before the risky type existed, so risky signals keep the same TP they
        # had when they were classified as scalp (rather than falling back to
        # standard). Copies the on-disk scalp values, not the hardcoded defaults.
        if not self.config["type_defaults"].get("risky"):
            scalp_defaults = self.config["type_defaults"].get("scalp") or self._scalp_defaults()
            self.config["type_defaults"]["risky"] = copy.deepcopy(scalp_defaults)
            if not self.config["type_overrides"].get("risky"):
                scalp_overrides = self.config["type_overrides"].get("scalp") or {}
                self.config["type_overrides"]["risky"] = copy.deepcopy(scalp_overrides)

        # Fill in missing fields on each entry.
        for type_name, asset_classes in self.config["type_defaults"].items():
            for asset_class, settings in asset_classes.items():
                if not isinstance(settings, dict):
                    logger.error(f"Invalid TP settings for {type_name}.{asset_class}")
                    continue
                settings.setdefault("type", self.ASSET_CLASS_TYPES.get(asset_class, "dollars"))
                settings.setdefault("value", 5.0)
                settings.setdefault("description", f"{type_name} - {asset_class}")

    # === Public API ===

    @staticmethod
    def _normalize_type(signal_type) -> str:
        if isinstance(signal_type, bool):  # legacy callers passing scalp bool
            return "scalp" if signal_type else "standard"
        if signal_type in VALID_SIGNAL_TYPES:
            return signal_type
        return "standard"

    def _get_config_for_symbol(self, symbol: str, signal_type: str = "standard") -> Dict:
        """Return {type, value} for a symbol given a signal type.

        Resolution order:
          1. type_overrides[signal_type][SYMBOL]
          2. type_overrides[standard][SYMBOL]   (per-symbol overrides fall back)
          3. type_defaults[signal_type][asset_class]
          4. type_defaults[standard][asset_class]
          5. hard fallback ($5)
        """
        signal_type = self._normalize_type(signal_type)
        s = symbol.upper()
        asset_class = self.determine_asset_class(s)

        per_type_overrides = self.config["type_overrides"].get(signal_type, {})
        if s in per_type_overrides:
            ov = per_type_overrides[s]
            return {"type": ov["type"], "value": ov["value"]}

        standard_overrides = self.config["type_overrides"].get("standard", {})
        if signal_type != "standard" and s in standard_overrides:
            ov = standard_overrides[s]
            return {"type": ov["type"], "value": ov["value"]}

        per_type_defaults = self.config["type_defaults"].get(signal_type, {})
        if asset_class in per_type_defaults:
            d = per_type_defaults[asset_class]
            return {"type": d["type"], "value": d["value"]}

        standard_defaults = self.config["type_defaults"].get("standard", {})
        if asset_class in standard_defaults:
            d = standard_defaults[asset_class]
            return {"type": d["type"], "value": d["value"]}

        logger.warning(f"No TP config for {s} (type={signal_type}), using fallback $5")
        return {"type": "dollars", "value": 5.0}

    def get_tp_value(self, symbol: str, signal_type: str = "standard") -> float:
        return self._get_config_for_symbol(symbol, signal_type=signal_type)["value"]

    def get_tp_type(self, symbol: str, signal_type: str = "standard") -> TPType:
        return self._get_config_for_symbol(symbol, signal_type=signal_type)["type"]  # type: ignore

    def calculate_pnl(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        current_price: float,
        signal_type: str = "standard",
    ) -> float:
        """Calculate P&L for a single limit position in native units."""
        tp_type = self.get_tp_type(symbol, signal_type=signal_type)

        if direction == "long":
            raw_diff = current_price - entry_price
        else:
            raw_diff = entry_price - current_price

        if tp_type == "pips":
            pip_size = self.get_pip_size(symbol)
            return raw_diff / pip_size
        return raw_diff

    def set_override(
        self,
        symbol: str,
        value: float,
        tp_type: TPType,
        set_by: str = "User",
        signal_type: str = "standard",
    ) -> bool:
        """Set a per-symbol TP override for a given signal type."""
        signal_type = self._normalize_type(signal_type)
        if tp_type not in ("pips", "dollars"):
            logger.error(f"Invalid TP type: {tp_type}")
            return False
        if value <= 0:
            logger.error(f"TP value must be positive, got {value}")
            return False

        bucket = self.config["type_overrides"].setdefault(signal_type, {})
        bucket[symbol.upper()] = {
            "type": tp_type,
            "value": value,
            "set_by": set_by,
            "set_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_config()
        logger.info(f"Set TP override [{signal_type}]: {symbol.upper()} = {value} {tp_type}")
        return True

    def set_default(
        self,
        asset_class: str,
        value: float,
        tp_type: TPType,
        set_by: str = "User",
        signal_type: str = "standard",
    ) -> bool:
        """Update the default TP for an asset class within a signal type."""
        signal_type = self._normalize_type(signal_type)
        if tp_type not in ("pips", "dollars"):
            logger.error(f"Invalid TP type: {tp_type}")
            return False
        if value <= 0:
            logger.error(f"TP value must be positive, got {value}")
            return False

        bucket = self.config["type_defaults"].setdefault(signal_type, {})
        bucket[asset_class] = {
            "type": tp_type,
            "value": value,
            "description": f"{signal_type} - {asset_class}",
            "set_by": set_by,
            "set_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_config()
        logger.info(f"Set TP default [{signal_type}]: {asset_class} = {value} {tp_type}")
        return True

    def remove_override(self, symbol: str, signal_type: str = "standard") -> bool:
        """Remove a per-symbol override for a given signal type."""
        signal_type = self._normalize_type(signal_type)
        s = symbol.upper()
        bucket = self.config["type_overrides"].get(signal_type, {})
        if s in bucket:
            del bucket[s]
            self._save_config()
            logger.info(f"Removed TP override [{signal_type}]: {s}")
            return True
        return False

    def get_display_info(self, symbol: str = None, signal_type: str = "standard") -> Dict:
        """Return formatted config dict for display."""
        signal_type = self._normalize_type(signal_type)
        if symbol:
            s = symbol.upper()
            cfg = self._get_config_for_symbol(s, signal_type=signal_type)
            asset_class = self.determine_asset_class(s)
            bucket = self.config["type_overrides"].get(signal_type, {})
            is_override = s in bucket
            result = {
                "symbol": s,
                "type": cfg["type"],
                "value": cfg["value"],
                "asset_class": asset_class,
                "is_override": is_override,
                "signal_type": signal_type,
            }
            if is_override:
                ov = bucket[s]
                result["set_by"] = ov.get("set_by", "Unknown")
                result["set_at"] = ov.get("set_at", "Unknown")
            return result

        type_overrides = self.config.get("type_overrides", {})
        return {
            "type_defaults": self.config.get("type_defaults", {}),
            "type_overrides": type_overrides,
            "total_overrides_by_type": {t: len(type_overrides.get(t, {})) for t in VALID_SIGNAL_TYPES},
        }

    def format_value(self, symbol: str, value: float) -> str:
        """Format a TP/P&L value with the correct unit label."""
        tp_type = self.get_tp_type(symbol)
        if tp_type == "pips":
            return f"{value:.1f} pips"
        return f"${value:.2f}"
