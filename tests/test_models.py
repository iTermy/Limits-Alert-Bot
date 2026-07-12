"""Model surface tests for SignalData / LimitData.

These lock the behaviours the rest of the codebase depends on: id
normalization, the limit_id alias, computed limit views, and signal-type
validation.
"""

from datetime import datetime, timezone

from models import LimitData, SignalData
from models.signal import VALID_SIGNAL_TYPES


def _make_limit(**overrides) -> dict:
    base = {
        "id": 1,
        "signal_id": 10,
        "price_level": 1.1000,
        "sequence_number": 1,
        "status": "pending",
    }
    base.update(overrides)
    return base


class TestLimitData:
    def test_limit_id_alias_maps_to_id(self):
        limit = LimitData.model_validate({"limit_id": 7, "signal_id": 3, "price_level": 2.0})
        assert limit.id == 7

    def test_id_wins_when_both_present(self):
        limit = LimitData.model_validate({"id": 5, "limit_id": 7})
        assert limit.id == 5

    def test_defaults(self):
        limit = LimitData()
        assert limit.status == "pending"
        assert limit.approaching_alert_sent is False
        assert limit.hit_alert_sent is False


class TestSignalDataNormalization:
    def test_from_db_row_normalizes_id(self):
        signal = SignalData.from_db_row({"id": 42, "instrument": "EURUSD", "direction": "long"})
        assert signal.signal_id == 42

    def test_from_db_row_builds_limits(self):
        signal = SignalData.from_db_row(
            {"id": 1, "instrument": "XAUUSD", "direction": "short"},
            limits=[_make_limit(), _make_limit(id=2, sequence_number=2, status="hit")],
        )
        assert len(signal.limits) == 2
        assert all(isinstance(lim, LimitData) for lim in signal.limits)

    def test_model_validate_accepts_signal_id_directly(self):
        signal = SignalData.model_validate({"signal_id": 9, "instrument": "EURUSD"})
        assert signal.signal_id == 9

    def test_legacy_scalp_bool_infers_type(self):
        assert SignalData.model_validate({"id": 1, "scalp": True}).type == "scalp"
        assert SignalData.model_validate({"id": 1, "scalp": False}).type == "standard"

    def test_unknown_type_coerces_to_standard(self):
        signal = SignalData.model_validate({"id": 1, "type": "definitely-not-a-type"})
        assert signal.type == "standard"

    def test_all_db_types_survive_model_load(self):
        # Regression guard: 'risky' was once missing from VALID_SIGNAL_TYPES and
        # silently coerced to 'standard', disabling risky-window handling.
        for signal_type in VALID_SIGNAL_TYPES:
            signal = SignalData.model_validate({"id": 1, "type": signal_type})
            assert signal.type == signal_type


class TestSignalDataComputedFields:
    def test_pending_and_hit_limits_partition(self):
        signal = SignalData.from_db_row(
            {"id": 1, "instrument": "EURUSD", "direction": "long"},
            limits=[
                _make_limit(id=1, status="pending"),
                _make_limit(id=2, sequence_number=2, status="hit"),
                _make_limit(id=3, sequence_number=3, status="cancelled"),
            ],
        )
        assert [lim.id for lim in signal.pending_limits] == [1]
        assert [lim.id for lim in signal.hit_limits] == [2]
        assert signal.hit_count == 1

    def test_datetime_fields_pass_through(self):
        now = datetime.now(timezone.utc)
        signal = SignalData.model_validate({"id": 1, "expiry_time": now})
        assert signal.expiry_time == now
