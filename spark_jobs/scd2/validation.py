"""Post-merge invariants required by point-in-time joins."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import DimensionSpec


def validate_history(
    spark: SparkSession,
    spec: DimensionSpec,
) -> None:
    """Validate intervals, uniqueness and snapshot completeness in one scan."""
    history = spark.table(spec.history_table)
    key = spec.business_key
    summary = history.agg(
        F.sum(
            F.when(F.col("valid_from") >= F.col("valid_to"), 1).otherwise(0)
        ).alias("invalid_intervals"),
        F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_rows"),
        F.countDistinct(
            F.when(F.col("is_current"), F.col(key))
        ).alias("distinct_current_keys"),
    ).first()

    invalid_intervals = int(summary["invalid_intervals"] or 0)
    current_rows = int(summary["current_rows"] or 0)
    distinct_current_keys = int(summary["distinct_current_keys"] or 0)

    if invalid_intervals:
        raise RuntimeError(
            f"[{spec.name}] history contains {invalid_intervals} invalid intervals"
        )
    if current_rows != distinct_current_keys:
        raise RuntimeError(
            f"[{spec.name}] duplicate current versions: "
            f"rows={current_rows}, distinct_keys={distinct_current_keys}"
        )
