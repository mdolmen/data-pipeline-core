"""deterministic_id: stable, order-sensitive, collision-resistant enough."""

from __future__ import annotations

from data_pipeline_core import deterministic_id


def test_stable_across_calls() -> None:
    assert deterministic_id("betclic", "OM", "OL", "1x2") == deterministic_id(
        "betclic", "OM", "OL", "1x2"
    )


def test_order_sensitive() -> None:
    assert deterministic_id("a", "b") != deterministic_id("b", "a")


def test_distinct_inputs_differ() -> None:
    assert deterministic_id("a", "b") != deterministic_id("a", "c")


def test_none_and_empty_are_distinct_from_values() -> None:
    assert deterministic_id("a", None) != deterministic_id("a", "None")
    assert deterministic_id("a", "") != deterministic_id("a")
