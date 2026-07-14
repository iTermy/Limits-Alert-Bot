"""Tests for the pure limit-diff planner used by signal edits.

The planner must preserve limit_id for unchanged prices (the EX bot keys its
orders off limit_id) and flag stale alerts when an alerted limit is removed.
"""

from database.signal_ops import _plan_limit_diff


def _row(id, price, seq, status="pending", approaching=False, hit_alert=False):
    return {
        "id": id,
        "price_level": price,
        "sequence_number": seq,
        "status": status,
        "approaching_alert_sent": approaching,
        "hit_alert_sent": hit_alert,
    }


def test_unchanged_levels_keep_ids():
    existing = [_row(1, 1.10, 1), _row(2, 1.09, 2)]
    diff = _plan_limit_diff(existing, [1.10, 1.09])
    assert diff["matched_ids"] == {1, 2}
    assert diff["removed_rows"] == []
    assert not diff["alert_invalidated"]
    assert [(seq, price) for seq, price, _ in diff["plan"]] == [(1, 1.10), (2, 1.09)]


def test_new_level_inserted_without_touching_others():
    existing = [_row(1, 1.10, 1)]
    diff = _plan_limit_diff(existing, [1.10, 1.08])
    assert diff["matched_ids"] == {1}
    seq, price, matched = diff["plan"][1]
    assert (seq, price, matched) == (2, 1.08, None)


def test_removed_unalerted_level_does_not_invalidate():
    existing = [_row(1, 1.10, 1), _row(2, 1.09, 2)]
    diff = _plan_limit_diff(existing, [1.10])
    assert [r["id"] for r in diff["removed_rows"]] == [2]
    assert not diff["alert_invalidated"]


def test_removed_alerted_level_invalidates():
    existing = [_row(1, 1.10, 1, approaching=True), _row(2, 1.09, 2)]
    diff = _plan_limit_diff(existing, [1.11, 1.09])  # 1.10 corrected to 1.11
    assert 1 not in diff["matched_ids"]
    assert diff["alert_invalidated"]


def test_removed_hit_level_invalidates_and_hit_count_drops():
    existing = [_row(1, 1.10, 1, status="hit", hit_alert=True)]
    diff = _plan_limit_diff(existing, [1.05])
    assert diff["hit_count"] == 0
    assert diff["alert_invalidated"]


def test_kept_hit_level_counts():
    existing = [_row(1, 1.10, 1, status="hit", hit_alert=True), _row(2, 1.09, 2)]
    diff = _plan_limit_diff(existing, [1.10, 1.09])
    assert diff["hit_count"] == 1
    assert not diff["alert_invalidated"]


def test_cancelled_rows_are_not_preserved():
    existing = [_row(1, 1.10, 1, status="cancelled")]
    diff = _plan_limit_diff(existing, [1.10])
    # Cancelled rows are rebuilt fresh (removed + re-inserted as pending).
    assert diff["matched_ids"] == set()
    assert [r["id"] for r in diff["removed_rows"]] == [1]
    assert diff["plan"][0][2] is None


def test_duplicate_prices_match_one_row_each():
    existing = [_row(1, 1.10, 1), _row(2, 1.10, 2)]
    diff = _plan_limit_diff(existing, [1.10, 1.10])
    assert diff["matched_ids"] == {1, 2}


def test_reordered_levels_resequence_but_keep_ids():
    existing = [_row(1, 1.10, 1), _row(2, 1.09, 2)]
    diff = _plan_limit_diff(existing, [1.09, 1.10])
    plan = {price: (seq, matched["id"]) for seq, price, matched in diff["plan"]}
    assert plan[1.09] == (1, 2)
    assert plan[1.10] == (2, 1)
