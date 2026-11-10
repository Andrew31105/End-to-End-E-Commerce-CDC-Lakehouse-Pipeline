"""Tests for the read-only SCD2 quality result layer."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from scd2.config import DIMENSIONS
from scd2.quality import evaluate_history

from .test_scd2_validation import (
    CATEGORY_HISTORY_SCHEMA,
    OPEN_END,
    history_row,
    make_history,
)


CATEGORY_SPEC = next(spec for spec in DIMENSIONS if spec.name == "categories")


def test_evaluate_history_returns_pass_for_valid_data(
    spark: SparkSession,
) -> None:
    boundary = datetime(2026, 1, 2)
    history = make_history(
        spark,
        [
            history_row(
                sk=1,
                valid_from=datetime(2026, 1, 1),
                valid_to=boundary,
                is_current=False,
                row_hash="hash-a",
                source_event_id="event-a",
            ),
            history_row(
                sk=2,
                valid_from=boundary,
                valid_to=OPEN_END,
                is_current=True,
                row_hash="hash-b",
                source_event_id="event-b",
            ),
        ],
    )

    result = evaluate_history(history, CATEGORY_SPEC)

    assert result.passed
    assert result.metrics is not None
    assert result.metrics.row_count == 2
    assert result.metrics.business_key_count == 1
    assert result.failure_details() == ""


def test_evaluate_history_returns_all_failures(
    spark: SparkSession,
) -> None:
    history = make_history(
        spark,
        [
            history_row(
                sk=1,
                valid_from=datetime(2026, 1, 1),
                valid_to=datetime(2026, 1, 10),
                is_current=False,
                row_hash="same-hash",
                source_event_id="same-event",
            ),
            history_row(
                sk=2,
                valid_from=datetime(2026, 1, 2),
                valid_to=OPEN_END,
                is_current=True,
                row_hash="same-hash",
                source_event_id="same-event",
            ),
        ],
    )

    result = evaluate_history(history, CATEGORY_SPEC)

    assert not result.passed
    assert result.metrics is not None
    assert result.metrics.overlapping_intervals == 1
    assert result.metrics.duplicate_source_event_ids == 1
    assert result.metrics.duplicate_consecutive_states == 1
    assert "overlapping_intervals=1" in result.failure_details()


def test_empty_history_is_blocking(spark: SparkSession) -> None:
    history = spark.createDataFrame([], CATEGORY_HISTORY_SCHEMA)

    result = evaluate_history(history, CATEGORY_SPEC)

    assert not result.passed
    assert result.metrics is not None
    assert result.metrics.row_count == 0
    assert result.metrics.violations() == {"empty_history_table": 1}
