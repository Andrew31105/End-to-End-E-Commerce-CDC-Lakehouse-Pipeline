"""Read-only data-quality runner for real Iceberg SCD2 tables."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession

from .config import DIMENSIONS, DimensionSpec
from .validation import HistoryQualityMetrics, collect_history_quality_metrics


@dataclass(frozen=True)
class DimensionQualityResult:
    """Quality result for one configured dimension."""

    dimension_name: str
    table_name: str
    metrics: Optional[HistoryQualityMetrics] = None
    read_error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return (
            self.read_error is None
            and self.metrics is not None
            and not self.metrics.violations()
        )

    def failure_details(self) -> str:
        if self.read_error:
            return f"table_read_error={self.read_error}"
        if self.metrics is None:
            return "metrics_unavailable"
        return ", ".join(
            f"{name}={count}"
            for name, count in self.metrics.violations().items()
        )


def evaluate_history(
    history: DataFrame,
    spec: DimensionSpec,
) -> DimensionQualityResult:
    """Evaluate an already loaded history DataFrame without modifying it."""
    return DimensionQualityResult(
        dimension_name=spec.name,
        table_name=spec.history_table,
        metrics=collect_history_quality_metrics(history, spec),
    )


def evaluate_dimension(
    spark: SparkSession,
    spec: DimensionSpec,
) -> DimensionQualityResult:
    """Load and evaluate one real Iceberg table, converting errors to results."""
    try:
        return evaluate_history(spark.table(spec.history_table), spec)
    except Exception as exc:
        return DimensionQualityResult(
            dimension_name=spec.name,
            table_name=spec.history_table,
            read_error=f"{type(exc).__name__}: {exc}",
        )


def print_quality_result(result: DimensionQualityResult) -> None:
    """Print one concise, Airflow-friendly result line."""
    status = "PASS" if result.passed else "FAIL"
    if result.metrics is None:
        volume = "rows=unknown, keys=unknown, current=unknown"
    else:
        volume = (
            f"rows={result.metrics.row_count:,}, "
            f"keys={result.metrics.business_key_count:,}, "
            f"current={result.metrics.current_row_count:,}"
        )
    details = "" if result.passed else f", errors: {result.failure_details()}"
    print(
        f"[SCD2 DQ][{status}] {result.dimension_name} "
        f"({result.table_name}): {volume}{details}"
    )


def run_scd2_quality_checks(
    spark: SparkSession,
    specs: Iterable[DimensionSpec] = DIMENSIONS,
) -> list[DimensionQualityResult]:
    """Check every requested table, print all results, then fail as one unit."""
    results = [evaluate_dimension(spark, spec) for spec in specs]
    for result in results:
        print_quality_result(result)

    failed = [result for result in results if not result.passed]
    print(
        f"[SCD2 DQ] checked={len(results)}, "
        f"passed={len(results) - len(failed)}, failed={len(failed)}"
    )
    if failed:
        failed_dimensions = ", ".join(
            result.dimension_name for result in failed
        )
        raise RuntimeError(
            f"[SCD2 DQ] Blocking checks failed for: {failed_dimensions}"
        )
    return results
