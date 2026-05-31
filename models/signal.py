"""Pydantic models for signals and limits returned from the database."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, computed_field, model_validator

VALID_SIGNAL_TYPES = ("standard", "scalp", "swing", "toll", "pa", "1-1")


class LimitData(BaseModel):
    id: int = 0
    signal_id: int = 0
    price_level: float = 0.0
    sequence_number: int = 0
    status: str = "pending"
    hit_time: Optional[datetime] = None
    hit_price: Optional[float] = None
    approaching_alert_sent: bool = False
    hit_alert_sent: bool = False
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}

    @model_validator(mode="before")
    @classmethod
    def _normalise_limit_id(cls, values: Any) -> Any:
        """Accept 'limit_id' as an alias for 'id' (used by tracking queries)."""
        if isinstance(values, dict) and "limit_id" in values and "id" not in values:
            values["id"] = values.pop("limit_id")
        return values

    # Dict-protocol methods for seamless transition from dict-based code.
    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        object.__setattr__(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


class SignalData(BaseModel):
    signal_id: int = 0
    message_id: Optional[str] = None
    channel_id: Optional[str] = None
    instrument: str = ""
    direction: str = ""
    stop_loss: float = 0.0
    expiry_type: Optional[str] = None
    expiry_time: Optional[datetime] = None
    status: str = "active"
    type: str = "standard"
    first_limit_hit_time: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    closed_reason: Optional[str] = None
    result_pips: Optional[float] = None
    total_limits: int = 0
    limits_hit: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    limits: list[LimitData] = Field(default_factory=list)

    # Runtime-only fields set by the streaming monitor (not from DB)
    guild_id: Optional[int] = None
    current_spread: Optional[float] = None
    sl_alert_sent: bool = False

    model_config = {"from_attributes": True}

    @computed_field
    @property
    def pending_limits(self) -> list[LimitData]:
        return [lim for lim in self.limits if lim.status == "pending"]

    @computed_field
    @property
    def hit_limits(self) -> list[LimitData]:
        return [lim for lim in self.limits if lim.status == "hit"]

    @computed_field
    @property
    def hit_count(self) -> int:
        return len(self.hit_limits)

    @classmethod
    def from_db_row(
        cls, row: dict[str, Any], limits: Optional[list[dict[str, Any]]] = None
    ) -> SignalData:
        """Build a SignalData from an asyncpg Record dict.

        Handles the id/signal_id normalization: the DB column is ``id`` but
        the domain name is ``signal_id``.
        """
        data = dict(row)
        data["signal_id"] = data.pop("id", data.get("signal_id"))

        if limits is not None:
            data["limits"] = [LimitData.model_validate(dict(lim)) for lim in limits]
        elif "limits" in data and isinstance(data["limits"], list):
            data["limits"] = [
                LimitData.model_validate(dict(lim)) if isinstance(lim, dict) else lim
                for lim in data["limits"]
            ]

        return cls.model_validate(data)

    @model_validator(mode="before")
    @classmethod
    def _normalise_id(cls, values: Any) -> Any:
        if isinstance(values, dict) and "id" in values and "signal_id" not in values:
            values["signal_id"] = values.pop("id")
        return values

    @model_validator(mode="before")
    @classmethod
    def _normalise_type(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        # Drop legacy scalp payloads; if no type was supplied, infer it.
        if "type" not in values and "scalp" in values:
            values["type"] = "scalp" if values.get("scalp") else "standard"
        values.pop("scalp", None)
        type_val = values.get("type")
        if type_val is not None and type_val not in VALID_SIGNAL_TYPES:
            values["type"] = "standard"
        return values

    # Dict-protocol methods for seamless transition from dict-based code.
    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        object.__setattr__(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def pop(self, key: str, *args: Any) -> Any:
        val = getattr(self, key, *args) if args else getattr(self, key)
        return val
