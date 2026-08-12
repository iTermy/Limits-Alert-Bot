"""Pydantic models for signals and limits returned from the database."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, computed_field, model_validator

VALID_SIGNAL_TYPES = ("standard", "scalp", "swing", "toll", "pa", "1-1", "risky")


class LimitData(BaseModel):
    id: int = 0
    signal_id: int = 0
    price_level: float = 0.0
    sequence_number: int = 0
    status: str = "pending"
    hit_time: datetime | None = None
    hit_price: float | None = None
    approaching_alert_sent: bool = False
    hit_alert_sent: bool = False
    created_at: datetime | None = None

    model_config = {"from_attributes": True}

    @model_validator(mode="before")
    @classmethod
    def _normalise_limit_id(cls, values: Any) -> Any:
        """Accept 'limit_id' as an alias for 'id' (used by tracking queries)."""
        if isinstance(values, dict) and "limit_id" in values and "id" not in values:
            values["id"] = values.pop("limit_id")
        return values


class SignalData(BaseModel):
    signal_id: int = 0
    message_id: str | None = None
    channel_id: str | None = None
    instrument: str = ""
    direction: str = ""
    stop_loss: float = 0.0
    expiry_type: str | None = None
    expiry_time: datetime | None = None
    status: str = "active"
    type: str = "standard"
    first_limit_hit_time: datetime | None = None
    closed_at: datetime | None = None
    closed_reason: str | None = None
    take_profit: float | None = None
    tp_price: float | None = None
    manual_tp_price: float | None = None
    total_limits: int = 0
    limits_hit: int = 0
    alert_message_id: int | None = None
    alert_channel_id: int | None = None
    ping_message_id: int | None = None
    finished_message_id: int | None = None
    finished_channel_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    limits: list[LimitData] = Field(default_factory=list)

    # Runtime-only fields set by the streaming monitor (not from DB)
    guild_id: int | None = None
    current_spread: float | None = None
    sl_alert_sent: bool = False
    asset_class: str | None = None
    shadow_only: bool = False

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
        cls, row: dict[str, Any], limits: list[dict[str, Any]] | None = None
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
