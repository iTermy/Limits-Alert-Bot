"""
Take Profit Configuration for Trading Alert Bot

Manages TP thresholds per asset class and per instrument.
Mirrors the structure of alert_config.py / alert_distances.json.

Supported types:
  - pips     (forex)
  - dollars  (metals, indices, crypto, oil, stocks)

P&L is always calculated in the same native unit as the TP type:
  - pips for forex/forex_jpy
  - dollars for everything else
"""

import json
import logging
from pathlib import Path
from typing import Dict, Literal, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

TPType = Literal["pips", "dollars"]


class TPConfig:
    """
    Manages take-profit configuration with per-asset-class defaults
    and per-symbol overrides.

    The TP value defines how many pips / dollars the LAST HIT LIMIT
    must be in profit before auto-close is triggered.

    For signals with multiple limits, all non-last limits must have a
    combined P&L >= 0 (breakeven) at the same moment.
    """

    # Supported asset classes and their default TP type
    ASSET_CLASS_TYPES: Dict[str, TPType] = {
        "forex":     "pips",
        "forex_jpy": "pips",
        "metals":    "dollars",
        "indices":   "dollars",
        "stocks":    "dollars",
        "crypto":    "dollars",
        "oil":       "dollars",
    }

    def __init__(self, config_path: str = None):
        if config_path is None:
            self.config_path = (
                Path(__file__).resolve().parent.parent / "config" / "tp_configuration.json"
            )
        else:
            self.config_path = Path(config_path)

        self.config = self._load_config()
        self._validate_config()

        # Borrow SymbolMapper for asset-class detection (same as alert_config)
        try:
            from price_feeds.symbol_mapper import SymbolMapper
            mapper_config = self.config_path.parent / "symbol_mappings.json"
            self.mapper = SymbolMapper(str(mapper_config))
        except Exception as e:
            logger.warning(f"Could not initialise SymbolMapper: {e}, using fallback detection")
            self.mapper = None

        logger.info("TPConfig initialised")

    # ------------------------------------------------------------------
    # Config I/O
    # ------------------------------------------------------------------

    def _load_config(self) -> Dict:
        try:
            with open(self.config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"TP config not found, creating default: {self.config_path}")
            return self._create_default_config()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in TP config: {e}. Using defaults.")
            return self._create_default_config()

    def _create_default_config(self) -> Dict:
        config = {
            "defaults": {
                "forex":     {"type": "pips",    "value": 10.0,  "description": "Standard forex pairs"},
                "forex_jpy": {"type": "pips",    "value": 10.0,  "description": "JPY pairs (auto-detected)"},
                "metals":    {"type": "dollars", "value": 5.0,   "description": "Gold, Silver, etc."},
                "indices":   {"type": "dollars", "value": 20.0,  "description": "Stock indices"},
                "stocks":    {"type": "dollars", "value": 1.0,   "description": "Individual stocks"},
                "crypto":    {"type": "dollars", "value": 50.0,  "description": "Cryptocurrencies"},
                "oil":       {"type": "dollars", "value": 0.5,   "description": "Oil commodities"},
            },
            "scalp_defaults": {
                "forex":     {"type": "pips",    "value": 3.0,   "description": "Scalp - Standard forex pairs"},
                "forex_jpy": {"type": "pips",    "value": 5.0,   "description": "Scalp - JPY pairs (auto-detected)"},
                "metals":    {"type": "dollars", "value": 2.0,   "description": "Scalp - Gold, Silver, etc."},
                "indices":   {"type": "dollars", "value": 10.0,  "description": "Scalp - Stock indices"},
                "stocks":    {"type": "dollars", "value": 0.5,   "description": "Scalp - Individual stocks"},
                "crypto":    {"type": "dollars", "value": 20.0,  "description": "Scalp - Cryptocurrencies"},
                "oil":       {"type": "dollars", "value": 0.2,   "description": "Scalp - Oil commodities"},
            },
            "overrides": {},
            "scalp_overrides": {},
        }
        self._save_config(config)
        return config

    def _validate_config(self):
        for key in ("defaults", "overrides"):
            if key not in self.config:
                logger.error(f"TP config missing key '{key}', resetting to defaults")
                self.config = self._create_default_config()
                return

        # Ensure scalp sections exist (migration for older configs)
        if "scalp_defaults" not in self.config:
            self.config["scalp_defaults"] = self._create_default_config()["scalp_defaults"]
        if "scalp_overrides" not in self.config:
            self.config["scalp_overrides"] = {}

        for section in ("defaults", "scalp_defaults"):
            for asset_class, settings in self.config[section].items():
                if not isinstance(settings, dict):
                    logger.error(f"Invalid TP settings for {asset_class} in {section}")
                    continue
                if "type" not in settings:
                    settings["type"] = self.ASSET_CLASS_TYPES.get(asset_class, "dollars")
                if "value" not in settings:
                    settings["value"] = 5.0
                if "description" not in settings:
                    settings["description"] = f"Default for {asset_class}"

    def _save_config(self, config: Dict = None):
        if config is None:
            config = self.config
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, "w") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save TP config: {e}")
            raise

    # ------------------------------------------------------------------
    # Asset-class detection (delegated to SymbolMapper or fallback)
    # ------------------------------------------------------------------

    def determine_asset_class(self, symbol: str) -> str:
        if self.mapper:
            try:
                return self.mapper.determine_asset_class(symbol)
            except Exception as e:
                logger.warning(f"SymbolMapper failed for {symbol}: {e}, using fallback")

        # Fallback (mirrors alert_config._determine_asset_class)
        s = symbol.upper()

        if any(c in s for c in ["BTC", "ETH", "BNB", "XRP", "ADA", "DOGE", "SOL", "DOT"]) or "USDT" in s:
            return "crypto"
        if any(c in s for c in ["XAU", "XAG", "GOLD", "SILVER"]):
            return "metals"
        if any(c in s for c in ["WTI", "BRENT", "OIL", "USOIL"]):
            return "oil"
        if any(c in s for c in ["SPX", "NAS", "DOW", "DAX", "US500", "USTEC", "US30",
                                  "US2000", "GER", "DE30", "DE40", "JP225", "CHINA50"]):
            return "indices"
        if "." in s:
            return "stocks"

        forex_ccys = {"EUR", "USD", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"}
        if len(s) == 6 and s[:3] in forex_ccys and s[3:] in forex_ccys:
            return "forex_jpy" if "JPY" in s else "forex"

        return "forex"  # safe default

    def get_pip_size(self, symbol: str) -> float:
        """Return pip size in price units for the given symbol."""
        s = symbol.upper()
        if "JPY" in s:
            return 0.01
        if any(c in s for c in ["XAU", "GOLD"]):
            return 0.01
        if any(c in s for c in ["XAG", "SILVER"]):
            return 0.001
        if "BTC" in s:
            return 1.0
        if any(c in s for c in ["SPX", "NAS", "DOW", "US500", "USTEC", "US30", "DAX"]):
            return 1.0
        return 0.0001  # Standard forex

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _get_config_for_symbol(self, symbol: str, scalp: bool = False) -> Dict:
        """Return {type, value} for a symbol, respecting overrides and scalp mode."""
        s = symbol.upper()

        if scalp:
            # Check scalp overrides first
            if s in self.config.get("scalp_overrides", {}):
                ov = self.config["scalp_overrides"][s]
                return {"type": ov["type"], "value": ov["value"]}
            # Fall back to scalp defaults
            asset_class = self.determine_asset_class(s)
            scalp_defaults = self.config.get("scalp_defaults", {})
            if asset_class in scalp_defaults:
                d = scalp_defaults[asset_class]
                return {"type": d["type"], "value": d["value"]}
            # If no scalp config found, fall through to regular config

        # Regular (setup) path
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
        """
        Return the TP threshold in its native unit (pips or dollars).
        Use this to compare against calculate_pnl().
        """
        return self._get_config_for_symbol(symbol, scalp=scalp)["value"]

    def get_tp_type(self, symbol: str, scalp: bool = False) -> TPType:
        """Return 'pips' or 'dollars' for the symbol."""
        return self._get_config_for_symbol(symbol, scalp=scalp)["type"]  # type: ignore

    def calculate_pnl(self, symbol: str, direction: str,
                      entry_price: float, current_price: float,
                      scalp: bool = False) -> float:
        """
        Calculate P&L for a single limit position in native units.

        For 'pips' instruments: result is in pips (positive = profit).
        For 'dollars' instruments: result is in dollars per unit (positive = profit).

        Args:
            symbol: Instrument name
            direction: 'long' or 'short'
            entry_price: hit_price of the limit
            current_price: current market price (bid for long, ask for short)
            scalp: Whether to use scalp TP config

        Returns:
            P&L in native units (pips or dollars)
        """
        tp_type = self.get_tp_type(symbol, scalp=scalp)

        if direction == "long":
            raw_diff = current_price - entry_price
        else:
            raw_diff = entry_price - current_price

        if tp_type == "pips":
            pip_size = self.get_pip_size(symbol)
            return raw_diff / pip_size
        else:  # dollars
            return raw_diff

    def set_override(self, symbol: str, value: float, tp_type: TPType,
                     set_by: str = "User", scalp: bool = False) -> bool:
        """Set a per-symbol TP override. Returns True on success."""
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
        logger.info(f"Set {'scalp ' if scalp else ''}TP override: {symbol.upper()} = {value} {tp_type}")
        return True

    def set_default(self, asset_class: str, value: float, tp_type: TPType,
                    set_by: str = "User", scalp: bool = False) -> bool:
        """Update the default TP for an asset class. Returns True on success."""
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
        """Remove a per-symbol override. Returns True if one existed."""
        s = symbol.upper()
        section = "scalp_overrides" if scalp else "overrides"
        if s in self.config.get(section, {}):
            del self.config[section][s]
            self._save_config()
            logger.info(f"Removed {'scalp ' if scalp else ''}TP override: {s}")
            return True
        return False

    def reload_config(self):
        """Reload configuration from disk."""
        self.config = self._load_config()
        self._validate_config()
        logger.info("TP configuration reloaded")

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
        else:
            return f"${value:.2f}"