"""Unit tests for invariants that make point-in-time joins trustworthy."""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from scd2.config import DIMENSIONS
from scd2.validation import (
    collect_history_quality_metrics,
    validate_history_frame,
)


CATEGORY_SPEC = next(spec for spec in DIMENSIONS if spec.name == "categories")
OPEN_END = datetime(9999, 12, 31)

CATEGORY_HISTORY_SCHEMA = StructType(
    [
        StructField("category_sk", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("source_ts", TimestampType(), True),
        StructField("valid_from", TimestampType(), True),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("row_hash", StringType(), True),
        StructField("cdc_op", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("source_event_id", StringType(), True),
        StructField("closed_by_event_id", StringType(), True),
    ]
)


def history_row(
    *,
    sk: int,
    valid_from: datetime,
    valid_to: datetime,
    is_current: bool,
    row_hash: str,
    source_event_id: str,
) -> Row:
    return Row(
        category_sk=sk,
        category_id=10,
        category_name=f"category-{sk}",
        description=None,
        source_ts=valid_from,
        valid_from=valid_from,
        valid_to=valid_to,
        is_current=is_current,
        row_hash=row_hash,
        cdc_op="u",
        ingested_at=valid_from,
        source_event_id=source_event_id,
        closed_by_event_id=None,
    )


def make_history(spark: SparkSession, rows: list[Row]):
    return spark.createDataFrame(rows, CATEGORY_HISTORY_SCHEMA)


def test_valid_adjacent_versions_pass(spark: SparkSession) -> None:
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

    metrics = collect_history_quality_metrics(history, CATEGORY_SPEC)

    assert metrics.row_count == 2
    assert metrics.business_key_count == 1
    assert metrics.current_row_count == 1
    assert metrics.violations() == {}
    validate_history_frame(history, CATEGORY_SPEC)


def test_nested_interval_is_detected_as_overlap(spark: SparkSession) -> None:
    history = make_history(
        spark,
        [
            history_row(
                sk=1,
                valid_from=datetime(2026, 1, 1),
                valid_to=datetime(2026, 1, 10),
                is_current=False,
                row_hash="hash-a",
                source_event_id="event-a",
            ),
            history_row(
                sk=2,
                valid_from=datetime(2026, 1, 2),
                valid_to=datetime(2026, 1, 3),
                is_current=False,
                row_hash="hash-b",
                source_event_id="event-b",
            ),
            history_row(
                sk=3,
                valid_from=datetime(2026, 1, 4),
                valid_to=OPEN_END,
                is_current=True,
                row_hash="hash-c",
                source_event_id="event-c",
            ),
        ],
    )

    metrics = collect_history_quality_metrics(history, CATEGORY_SPEC)

    assert metrics.overlapping_intervals == 2
    with pytest.raises(RuntimeError, match="overlapping_intervals=2"):
        validate_history_frame(history, CATEGORY_SPEC)


def test_reports_all_current_state_violations(spark: SparkSession) -> None:
    history = make_history(
        spark,
        [
            history_row(
                sk=1,
                valid_from=datetime(2026, 1, 1),
                valid_to=datetime(2026, 1, 2),
                is_current=True,
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

    metrics = collect_history_quality_metrics(history, CATEGORY_SPEC)

    assert metrics.duplicate_current_versions == 1
    assert metrics.invalid_open_ended_state == 1
    assert metrics.duplicate_source_event_ids == 1
    assert metrics.duplicate_consecutive_states == 1

    with pytest.raises(RuntimeError) as error:
        validate_history_frame(history, CATEGORY_SPEC)
    message = str(error.value)
    assert "duplicate_current_versions=1" in message
    assert "invalid_open_ended_state=1" in message
    assert "duplicate_source_event_ids=1" in message
    assert "duplicate_consecutive_states=1" in message
