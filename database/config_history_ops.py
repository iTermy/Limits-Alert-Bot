"""Audit log for runtime config changes (config_history table)."""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _serialize(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return str(value)


async def log_config_change(db, family: str, key: str, old_value: Any, new_value: Any, set_by: str) -> None:
    """
    Append one config_history row. Failures are logged and swallowed —
    an audit write must never break the command that triggered it.
    """
    try:
        await db.execute(
            """
            INSERT INTO config_history (config_family, key, old_value, new_value, set_by)
            VALUES ($1, $2, $3, $4, $5)
            """,
            (family, key, _serialize(old_value), _serialize(new_value), set_by),
        )
    except Exception as e:
        logger.warning(f"config_history write failed ({family}/{key}): {e}")
