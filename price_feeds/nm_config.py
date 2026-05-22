"""
Near-Miss (NM) Configuration for Trading Alert Bot

Manages near-miss thresholds per asset class and per instrument using a
LINEAR bounce model — the required bounce scales with how close price got.

LINEAR MODEL
============
Two parameters per asset class / symbol:

  max_proximity  — outer cutoff (in pips or dollars).
                   If price never comes this close to the first limit,
                   the signal is NOT tracked at all.

  base_bounce    — the minimum bounce needed even at a zero-distance approach.
                   At any closest_distance d, the required bounce is:

                       required_bounce(d) = d + base_bounce

  So the threshold is a straight line with slope=1 and y-intercept=base_bounce.
  The closer price gets, the LESS additional bounce is needed — but you always
  need at least base_bounce regardless.

Supported types:
  - pips     (forex / forex_jpy)
  - dollars  (metals, indices, crypto, oil, stocks)
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Literal

from ._base_config import BaseThresholdConfig

logger = logging.getLogger(__name__)

NMType = Literal["pips", "dollars"]


class NMConfig(BaseThresholdConfig):
    """
    Near-miss configuration with per-asset-class defaults and per-symbol overrides.

    The linear bounce formula means you only need to tune two numbers:
      max_proximity  -- outer gate (no tracking beyond this distance)
      base_bounce    -- minimum bounce from closest approach to confirm NM

    required_bounce = closest_distance + base_bounce
    """

    CONFIG_FILENAME = "nm_configuration.json"

    ASSET_CLASS_TYPES: Dict[str, NMType] = {
        "forex": "pips",
        "forex_jpy": "pips",
        "metals": "dollars",
        "indices": "dollars",
        "stocks": "dollars",
        "crypto": "dollars",
        "oil": "dollars",
    }

    PIP_SIZES: Dict[str, float] = {
        "forex": 0.0001,
        "forex_jpy": 0.01,
        "metals": 1.0,
        "indices": 1.0,
        "stocks": 1.0,
        "crypto": 1.0,
        "oil": 1.0,
    }

    def __init__(self, config_path: str = None):
        super().__init__(config_path)
        logger.info("NMConfig initialised (linear bounce model)")

    # === Config defaults & migration ===

    def _post_load(self, raw: Dict) -> Dict:
        return self._migrate_if_needed(raw)

    def _migrate_if_needed(self, config: Dict) -> Dict:
        """
        Migrate from old proximity_threshold/bounce_threshold format to
        max_proximity/base_bounce format.
        """
        migrated = False
        for section in ("defaults", "overrides"):
            for key, entry in config.get(section, {}).items():
                if "proximity_threshold" in entry and "max_proximity" not in entry:
                    old_prox = entry.pop("proximity_threshold")
                    old_bounce = entry.pop("bounce_threshold", old_prox + 4.0)
                    entry["max_proximity"] = old_prox
                    entry["base_bounce"] = max(1.0, old_bounce - old_prox)
                    migrated = True
        if migrated:
            logger.info("NM config migrated from fixed-threshold to linear-bounce model")
            self._save_config(config)
        return config

    def _create_default_config(self) -> Dict:
        return {
            "defaults": {
                "forex": {
                    "type": "pips",
                    "max_proximity": 7.0,
                    "base_bounce": 4.0,
                    "description": "Track within 7 pips; bounce = closest + 4 pips",
                },
                "forex_jpy": {
                    "type": "pips",
                    "max_proximity": 10.0,
                    "base_bounce": 6.0,
                    "description": "Track within 10 pips; bounce = closest + 6 pips",
                },
                "metals": {
                    "type": "dollars",
                    "max_proximity": 6.0,
                    "base_bounce": 3.0,
                    "description": "Track within $6; bounce = closest + $3",
                },
                "indices": {
                    "type": "dollars",
                    "max_proximity": 20.0,
                    "base_bounce": 10.0,
                    "description": "Track within $20; bounce = closest + $10",
                },
                "stocks": {
                    "type": "dollars",
                    "max_proximity": 1.0,
                    "base_bounce": 0.5,
                    "description": "Track within $1; bounce = closest + $0.50",
                },
                "crypto": {
                    "type": "dollars",
                    "max_proximity": 50.0,
                    "base_bounce": 30.0,
                    "description": "Track within $50; bounce = closest + $30",
                },
                "oil": {
                    "type": "dollars",
                    "max_proximity": 0.2,
                    "base_bounce": 0.1,
                    "description": "Track within $0.20; bounce = closest + $0.10",
                },
            },
            "overrides": {},
        }

    # === Config entry lookup ===

    def _get_config_entry(self, symbol: str) -> Dict:
        overrides = self.config.get("overrides", {})
        if symbol.upper() in overrides:
            return overrides[symbol.upper()]
        asset_class = self.determine_asset_class(symbol)
        defaults = self.config.get("defaults", {})
        return defaults.get(asset_class, defaults.get("forex", {}))

    # === Core linear-model API ===

    def get_nm_type(self, symbol: str) -> NMType:
        return self._get_config_entry(symbol).get("type", "pips")

    def _to_price_units(self, symbol: str, value: float) -> float:
        """Convert a stored value (pips or dollars) to absolute price units."""
        entry = self._get_config_entry(symbol)
        if entry.get("type") == "pips":
            asset_class = self.determine_asset_class(symbol)
            pip_size = self.PIP_SIZES.get(asset_class, 0.0001)
            return value * pip_size
        return value

    def get_max_proximity(self, symbol: str) -> float:
        """
        Return max_proximity in absolute price units.
        Tracking only begins when price is closer than this to the first limit.
        """
        entry = self._get_config_entry(symbol)
        return self._to_price_units(symbol, entry.get("max_proximity", 7.0))

    def get_required_bounce(self, symbol: str, closest_distance_price_units: float) -> float:
        """
        Return the required bounce (absolute price units) given the closest approach.

        Formula:  required_bounce = closest_distance + base_bounce
        """
        entry = self._get_config_entry(symbol)
        base_bounce_price = self._to_price_units(symbol, entry.get("base_bounce", 4.0))
        return closest_distance_price_units + base_bounce_price

    def get_params_display(self, symbol: str) -> Dict:
        """Return params in stored units (pips or dollars), for display."""
        entry = self._get_config_entry(symbol)
        return {
            "max_proximity": entry.get("max_proximity", 7.0),
            "base_bounce": entry.get("base_bounce", 4.0),
            "type": entry.get("type", "pips"),
            "description": entry.get("description", ""),
        }

    def format_value(self, symbol: str, value_price_units: float) -> str:
        """Format a price-unit value for human display."""
        entry = self._get_config_entry(symbol)
        if entry.get("type") == "pips":
            asset_class = self.determine_asset_class(symbol)
            pip_size = self.PIP_SIZES.get(asset_class, 0.0001)
            pips = value_price_units / pip_size if pip_size else value_price_units
            return f"{pips:.1f} pips"
        return f"${value_price_units:.2f}"

    def describe_curve(self, symbol: str, steps: int = 5) -> str:
        """
        Return a human-readable table of the linear NM curve for a symbol.
        Used in !nmconfig show output.
        """
        entry = self._get_config_entry(symbol)
        nm_type = entry.get("type", "pips")
        max_prox = entry.get("max_proximity", 7.0)
        base_b = entry.get("base_bounce", 4.0)
        dollar = nm_type == "dollars"

        lines = []
        for i in range(1, steps + 1):
            d = round(max_prox * i / steps, 1)
            req = round(d + base_b, 1)
            if dollar:
                lines.append(f"within ${d} → need ${req} bounce")
            else:
                lines.append(f"within {d} pip → need {req} pip bounce")
        return "\n".join(lines)

    # === Override / default management ===

    def set_override(
        self,
        symbol: str,
        max_proximity: float,
        base_bounce: float,
        nm_type: str = None,
        set_by: str = "system",
    ) -> bool:
        symbol = symbol.upper()
        if nm_type is None:
            asset_class = self.determine_asset_class(symbol)
            nm_type = self.ASSET_CLASS_TYPES.get(asset_class, "pips")
        self.config.setdefault("overrides", {})[symbol] = {
            "type": nm_type,
            "max_proximity": max_proximity,
            "base_bounce": base_bounce,
            "set_by": set_by,
            "set_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_config()
        logger.info(
            f"NM override set for {symbol}: max_proximity={max_proximity}, base_bounce={base_bounce} ({nm_type})"
        )
        return True

    def set_default(
        self,
        asset_class: str,
        max_proximity: float,
        base_bounce: float,
        nm_type: str = None,
        set_by: str = "system",
    ) -> bool:
        if nm_type is None:
            nm_type = self.ASSET_CLASS_TYPES.get(asset_class, "pips")
        self.config.setdefault("defaults", {})[asset_class] = {
            "type": nm_type,
            "max_proximity": max_proximity,
            "base_bounce": base_bounce,
            "description": f"Set by {set_by}",
        }
        self._save_config()
        logger.info(
            f"NM default updated for {asset_class}: max_proximity={max_proximity}, base_bounce={base_bounce}"
        )
        return True

    def remove_override(self, symbol: str) -> bool:
        symbol = symbol.upper()
        overrides = self.config.get("overrides", {})
        if symbol in overrides:
            del overrides[symbol]
            self._save_config()
            return True
        return False

    def get_all_defaults(self) -> Dict:
        return self.config.get("defaults", {})

    def get_all_overrides(self) -> Dict:
        return self.config.get("overrides", {})
