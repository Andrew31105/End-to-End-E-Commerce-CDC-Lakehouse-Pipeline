"""Incremental Debezium CDC reads from Iceberg Bronze."""
from __future__ import annotations

from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from scripts.water_mask import read_new_spark

from .config import DimensionSpec


VALID_CDC_OPERATIONS = ("c", "r", "u", "d")


def _hash_values(values: list[Column]) -> Column:
    canonical = [
        F.coalesce(value.cast("string"), F.lit("\u0000")) for value in values
    ]
    return F.sha2(F.concat_ws("|", *canonical), 256)


def _event_id(spec: DimensionSpec) -> Column:
    """Build a deterministic replay key from Kafka coordinates."""
    return F.sha2(
        F.concat_ws(
            "|",
            F.lit(spec.name),
            F.coalesce(F.col("kafka_partition").cast("string"), F.lit("-1")),
            F.coalesce(F.col("kafka_offset").cast("string"), F.lit("-1")),
            F.coalesce(F.col("debezium_ts_ms").cast("string"), F.lit("-1")),
            F.coalesce(F.col("_raw_payload"), F.lit("")),
        ),
        256,
    )


def normalize_cdc_events(bronze: DataFrame, spec: DimensionSpec) -> DataFrame:
    """Flatten Debezium envelopes and remove non-SCD updates."""
    projected = bronze.select(
        "op",
        "debezium_ts_ms",
        "kafka_ts",
        "kafka_partition",
        "kafka_offset",
        "_raw_payload",
        *[
            F.col(f"before.{field.source}")
            .cast(field.spark_type)
            .alias(f"_before_{field.target}")
            for field in spec.fields
        ],
        *[
            F.col(f"after.{field.source}")
            .cast(field.spark_type)
            .alias(f"_after_{field.target}")
            for field in spec.fields
        ],
    )

    selected = projected.select(
        "*",
        *[
            F.when(
                F.col("op") == "d",
                F.col(f"_before_{field.target}"),
            )
            .otherwise(F.col(f"_after_{field.target}"))
            .alias(field.target)
            for field in spec.fields
        ],
    )
    before_hash = _hash_values(
        [F.col(f"_before_{column}") for column in spec.tracked_columns]
    )
    after_hash = _hash_values(
        [F.col(f"_after_{column}") for column in spec.tracked_columns]
    )

    events = (
        selected.withColumn("event_id", _event_id(spec))
        .withColumn(
            "event_ts",
            F.expr("timestamp_micros(debezium_ts_ms * 1000)"),
        )
        .withColumn("before_hash", before_hash)
        .withColumn("row_hash", after_hash)
        .filter(F.col("op").isin(*VALID_CDC_OPERATIONS))
        .filter(F.col(spec.business_key).isNotNull())
        .filter(F.col("event_ts").isNotNull())
        # Stock-only product updates and equivalent no-op updates do not create
        # a Type 2 version.
        .filter((F.col("op") != "u") | (F.col("before_hash") != F.col("row_hash")))
        .dropDuplicates(["event_id"])
    )
    return events.select(
        spec.business_key,
        *spec.tracked_columns,
        "op",
        "event_id",
        "event_ts",
        "debezium_ts_ms",
        "kafka_partition",
        "kafka_offset",
        "row_hash",
    )


def read_cdc_batch(
    spark: SparkSession,
    spec: DimensionSpec,
) -> tuple[DataFrame, Optional[str]]:
    """Read only Bronze snapshots not yet committed to this dimension."""
    bronze, snapshot_id = read_new_spark(
        spark,
        table_name=spec.bronze_table,
        pipeline_name=f"silver_scd2_{spec.name}",
    )
    return normalize_cdc_events(bronze, spec), snapshot_id
