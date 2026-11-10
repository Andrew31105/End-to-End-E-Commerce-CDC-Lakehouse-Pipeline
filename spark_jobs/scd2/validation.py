"""Post-merge invariants required by point-in-time joins."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import OPEN_ENDED_VALID_TO, DimensionSpec


@dataclass(frozen=True)
class HistoryQualityMetrics:
    """Volume metrics and invariant violations for one history table."""

    row_count: int
    business_key_count: int
    current_row_count: int
    null_required_fields: int
    invalid_intervals: int
    overlapping_intervals: int
    duplicate_current_versions: int
    invalid_open_ended_state: int
    duplicate_source_event_ids: int
    duplicate_consecutive_states: int

    def violations(self) -> dict[str, int]:
        """Return only failed invariants, suitable for logs and exceptions."""
        violation_names = (
            "null_required_fields",
            "invalid_intervals",
            "overlapping_intervals",
            "duplicate_current_versions",
            "invalid_open_ended_state",
            "duplicate_source_event_ids",
            "duplicate_consecutive_states",
        )
        failures = {
            name: getattr(self, name)
            for name in violation_names
            if getattr(self, name) > 0
        }
        if self.row_count == 0:
            failures = {"empty_history_table": 1, **failures}
        return failures


def collect_history_quality_metrics(
    history: DataFrame,
    spec: DimensionSpec,
) -> HistoryQualityMetrics:
    """Calculate all correctness metrics for an SCD2 history DataFrame.

    The running maximum of ``valid_to`` is used instead of a simple ``lag``.
    This also detects an interval nested inside a much longer earlier interval.
    """
    key = spec.business_key
    history_order = Window.partitionBy(key).orderBy(
        F.col("valid_from"),
        F.col("valid_to"),
        F.col(spec.surrogate_key),
    )
    prior_rows = history_order.rowsBetween(Window.unboundedPreceding, -1)

    measured = (
        history.withColumn(
            "_previous_max_valid_to",
            F.max("valid_to").over(prior_rows),
        )
        .withColumn(
            "_previous_row_hash",
            F.lag("row_hash").over(history_order),
        )
    )

    open_ended = F.lit(OPEN_ENDED_VALID_TO).cast("timestamp")
    required_is_null = (
        F.col(spec.surrogate_key).isNull()
        | F.col(key).isNull()
        | F.col("valid_from").isNull()
        | F.col("valid_to").isNull()
        | F.col("is_current").isNull()
        | F.col("row_hash").isNull()
    )
    invalid_open_ended = (
        (F.col("is_current") & (F.col("valid_to") != open_ended))
        | (~F.col("is_current") & (F.col("valid_to") == open_ended))
    )

    summary = measured.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(key).alias("business_key_count"),
        F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias(
            "current_row_count"
        ),
        F.sum(F.when(required_is_null, 1).otherwise(0)).alias(
            "null_required_fields"
        ),
        F.sum(
            F.when(F.col("valid_from") >= F.col("valid_to"), 1).otherwise(0)
        ).alias("invalid_intervals"),
        F.sum(
            F.when(
                F.col("valid_from") < F.col("_previous_max_valid_to"), 1
            ).otherwise(0)
        ).alias("overlapping_intervals"),
        (
            F.sum(F.when(F.col("is_current"), 1).otherwise(0))
            - F.countDistinct(F.when(F.col("is_current"), F.col(key)))
        ).alias("duplicate_current_versions"),
        F.sum(F.when(invalid_open_ended, 1).otherwise(0)).alias(
            "invalid_open_ended_state"
        ),
        (
            F.count("source_event_id") - F.countDistinct("source_event_id")
        ).alias("duplicate_source_event_ids"),
        F.sum(
            F.when(
                F.col("_previous_row_hash").isNotNull()
                & (F.col("row_hash") == F.col("_previous_row_hash")),
                1,
            ).otherwise(0)
        ).alias("duplicate_consecutive_states"),
    ).first()

    return HistoryQualityMetrics(
        row_count=int(summary["row_count"] or 0),
        business_key_count=int(summary["business_key_count"] or 0),
        current_row_count=int(summary["current_row_count"] or 0),
        null_required_fields=int(summary["null_required_fields"] or 0),
        invalid_intervals=int(summary["invalid_intervals"] or 0),
        overlapping_intervals=int(summary["overlapping_intervals"] or 0),
        duplicate_current_versions=int(
            summary["duplicate_current_versions"] or 0
        ),
        invalid_open_ended_state=int(
            summary["invalid_open_ended_state"] or 0
        ),
        duplicate_source_event_ids=int(
            summary["duplicate_source_event_ids"] or 0
        ),
        duplicate_consecutive_states=int(
            summary["duplicate_consecutive_states"] or 0
        ),
    )


def validate_history_frame(history: DataFrame, spec: DimensionSpec) -> None:
    """Raise one actionable error containing every failed SCD2 invariant."""
    metrics = collect_history_quality_metrics(history, spec)
    violations = metrics.violations()
    if not violations:
        return

    details = ", ".join(
        f"{name}={count}" for name, count in violations.items()
    )
    raise RuntimeError(f"[{spec.name}] invalid SCD2 history: {details}")


def validate_history(
    spark: SparkSession,
    spec: DimensionSpec,
) -> None:
    """Read an Iceberg history table and enforce all SCD2 invariants."""
    validate_history_frame(spark.table(spec.history_table), spec)
