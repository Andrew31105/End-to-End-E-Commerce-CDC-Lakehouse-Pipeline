"""Incremental Debezium CDC to Iceberg SCD Type 2 engine."""
from __future__ import annotations

import os
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts.water_mask import (
    get_current_snapshot_id,
    read_watermark,
    write_watermark,
)

from .config import BOOTSTRAP_VALID_FROM, OPEN_ENDED_VALID_TO, DimensionSpec
from .source import read_cdc_batch
from .validation import validate_history


ACTION_CLOSE = "close"
ACTION_INSERT = "insert"
UPSERT_OPERATIONS = ("c", "r", "u")


def ensure_history_table(spark: SparkSession, spec: DimensionSpec) -> None:
    """Create the table and evolve legacy history schemas in place."""
    spark.sql(spec.ddl)
    existing = set(spark.table(spec.history_table).columns)
    missing = []
    if "source_event_id" not in existing:
        missing.append("source_event_id STRING")
    if "closed_by_event_id" not in existing:
        missing.append("closed_by_event_id STRING")
    if missing:
        spark.sql(
            f"ALTER TABLE {spec.history_table} "
            f"ADD COLUMNS ({', '.join(missing)})"
        )


def processed_event_ids(history: DataFrame) -> DataFrame:
    """Return every CDC event already represented in the history table."""
    opened = history.select(F.col("source_event_id").alias("event_id"))
    closed = history.select(F.col("closed_by_event_id").alias("event_id"))
    return (
        opened.unionByName(closed)
        .filter(F.col("event_id").isNotNull())
        .distinct()
    )


def canonical_current_versions(
    history: DataFrame,
    spec: DimensionSpec,
) -> DataFrame:
    """Return one comparison row per key from potentially dirty legacy data."""
    key = spec.business_key
    latest_first = Window.partitionBy(key).orderBy(
        F.col("valid_from").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col(spec.surrogate_key).desc_nulls_last(),
    )
    return (
        history.filter(F.col("is_current"))
        .withColumn("_rank", F.row_number().over(latest_first))
        .filter(F.col("_rank") == 1)
        .select(
            key,
            F.col("valid_from").alias("current_valid_from"),
            F.col("row_hash").alias("current_hash"),
        )
    )


def prepare_events(
    events: DataFrame,
    history: DataFrame,
    spec: DimensionSpec,
) -> DataFrame:
    """Remove replays/no-op snapshots and establish deterministic ordering."""
    key = spec.business_key
    unprocessed = events.join(
        processed_event_ids(history),
        "event_id",
        "left_anti",
    )
    current = canonical_current_versions(history, spec)

    # A Debezium initial snapshot may be replayed into an already bootstrapped
    # history table. If its state equals current, advancing the watermark is
    # sufficient and no artificial version should be created.
    meaningful = (
        unprocessed.alias("event")
        .join(current.alias("current"), key, "left")
        .filter(
            ~(
                F.col("event.op").isin("c", "r")
                & F.col("current.current_hash").isNotNull()
                & (F.col("event.row_hash") == F.col("current.current_hash"))
            )
        )
        .select("event.*")
    )

    same_millisecond = Window.partitionBy(key, "debezium_ts_ms").orderBy(
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("event_id"),
    )
    ordered = meaningful.withColumn(
        "_timestamp_tie",
        F.row_number().over(same_millisecond) - F.lit(1),
    ).withColumn(
        "effective_ts",
        F.expr(
            "timestamp_micros(debezium_ts_ms * 1000 + _timestamp_tie)"
        ),
    )

    sequence = Window.partitionBy(key).orderBy(
        F.col("effective_ts"),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("event_id"),
    )
    return (
        ordered.withColumn("event_order", F.row_number().over(sequence))
        .withColumn("next_event_ts", F.lead("effective_ts").over(sequence))
        .withColumn("next_event_id", F.lead("event_id").over(sequence))
    )


def validate_event_order(
    prepared: DataFrame,
    history: DataFrame,
    spec: DimensionSpec,
) -> None:
    """Reject genuinely late CDC events rather than corrupting intervals."""
    key = spec.business_key
    first_events = prepared.filter(F.col("event_order") == 1).select(
        key,
        "effective_ts",
    )
    current = canonical_current_versions(history, spec)
    late = (
        first_events.join(current, key, "inner")
        .filter(F.col("effective_ts") <= F.col("current_valid_from"))
        .limit(1)
        .collect()
    )
    if late:
        row = late[0]
        raise RuntimeError(
            f"[{spec.name}] late CDC event for {key}={row[key]}: "
            f"event_ts={row['effective_ts']}, "
            f"current_valid_from={row['current_valid_from']}"
        )


def stage_merge_rows(
    prepared: DataFrame,
    history: DataFrame,
    spec: DimensionSpec,
) -> DataFrame:
    """Build one close row per key plus one row per version to insert."""
    key = spec.business_key
    current = canonical_current_versions(history, spec).select(key)

    close_rows = (
        prepared.filter(F.col("event_order") == 1)
        .join(current, key, "inner")
        .withColumn("action", F.lit(ACTION_CLOSE))
        .withColumn("merge_key", F.col(key))
        .withColumn("version_sk", F.lit(None).cast("bigint"))
        .withColumn("valid_from", F.lit(None).cast("timestamp"))
        .withColumn("valid_to", F.col("effective_ts"))
        .withColumn("is_current", F.lit(False))
        .withColumn("source_event_id", F.lit(None).cast("string"))
        .withColumn("closed_by_event_id", F.col("event_id"))
    )

    version_rows = (
        prepared.filter(F.col("op").isin(*UPSERT_OPERATIONS))
        .join(current.withColumn("_has_current", F.lit(True)), key, "left")
        .withColumn("action", F.lit(ACTION_INSERT))
        .withColumn("merge_key", F.lit(None).cast(prepared.schema[key].dataType))
        .withColumn("version_sk", F.xxhash64(F.col(key), F.col("event_id")))
        .withColumn(
            "valid_from",
            F.when(
                F.col("_has_current").isNull()
                & (F.col("event_order") == 1)
                & (F.col("op") == "r"),
                F.lit(BOOTSTRAP_VALID_FROM).cast("timestamp"),
            ).otherwise(F.col("effective_ts")),
        )
        .withColumn(
            "valid_to",
            F.coalesce(
                F.col("next_event_ts"),
                F.lit(OPEN_ENDED_VALID_TO).cast("timestamp"),
            ),
        )
        .withColumn("is_current", F.col("next_event_ts").isNull())
        .withColumn("source_event_id", F.col("event_id"))
        .withColumn("closed_by_event_id", F.col("next_event_id"))
    )

    columns = [
        key,
        *spec.tracked_columns,
        "op",
        "effective_ts",
        "action",
        "merge_key",
        "version_sk",
        "valid_from",
        "valid_to",
        "is_current",
        "row_hash",
        "source_event_id",
        "closed_by_event_id",
    ]
    return close_rows.select(*columns).unionByName(version_rows.select(*columns))


def merge_history(
    spark: SparkSession,
    staged: DataFrame,
    spec: DimensionSpec,
) -> None:
    """Commit every event in the micro-batch with one idempotent MERGE."""
    key = spec.business_key
    tracked_values = ", ".join(
        f"source.{column}" for column in spec.tracked_columns
    )
    insert_columns = ", ".join(
        (
            spec.surrogate_key,
            key,
            *spec.tracked_columns,
            "source_ts",
            "valid_from",
            "valid_to",
            "is_current",
            "row_hash",
            "cdc_op",
            "ingested_at",
            "source_event_id",
            "closed_by_event_id",
        )
    )

    staged.createOrReplaceTempView("scd2_cdc_staged")
    spark.sql(f"""
        MERGE INTO {spec.history_table} AS target
        USING scd2_cdc_staged AS source
           ON (
                source.action = '{ACTION_CLOSE}'
            AND target.{key} = source.merge_key
            AND target.is_current = true
            AND target.valid_from < source.valid_to
           )
           OR (
                source.action = '{ACTION_INSERT}'
            AND target.{spec.surrogate_key} = source.version_sk
           )
        WHEN MATCHED AND source.action = '{ACTION_CLOSE}' THEN
          UPDATE SET
            valid_to = source.valid_to,
            is_current = false,
            cdc_op = source.op,
            closed_by_event_id = source.closed_by_event_id
        WHEN NOT MATCHED AND source.action = '{ACTION_INSERT}' THEN
          INSERT ({insert_columns})
          VALUES (
            source.version_sk,
            source.{key},
            {tracked_values},
            source.effective_ts,
            source.valid_from,
            source.valid_to,
            source.is_current,
            source.row_hash,
            source.op,
            current_timestamp(),
            source.source_event_id,
            source.closed_by_event_id
          )
    """)


def _commit_watermark(
    spark: SparkSession,
    spec: DimensionSpec,
    snapshot_id: Optional[str],
) -> None:
    if snapshot_id is not None:
        write_watermark(
            spark,
            pipeline_name=f"silver_scd2_{spec.name}",
            snapshot_id=snapshot_id,
        )


def initialize_legacy_baseline(
    spark: SparkSession,
    spec: DimensionSpec,
) -> bool:
    """Start CDC after existing snapshot-built history without replaying it.

    This branch runs only once during migration. A clean installation has an
    empty history table and therefore consumes Debezium's initial ``op='r'``
    events instead.
    """
    pipeline_name = f"silver_scd2_{spec.name}"
    if read_watermark(spark, pipeline_name) is not None:
        return False

    history = spark.table(spec.history_table)
    has_history = bool(history.limit(1).count())
    has_event_ledger = bool(
        history.filter(
            F.col("source_event_id").isNotNull()
            | F.col("closed_by_event_id").isNotNull()
        ).limit(1).count()
    )
    if not has_history or has_event_ledger:
        return False

    snapshot_id = get_current_snapshot_id(spark, spec.bronze_table)
    if snapshot_id is None:
        return False

    accept_baseline = os.getenv(
        "SCD2_ACCEPT_LEGACY_BASELINE", "false"
    ).lower() == "true"
    if not accept_baseline:
        raise RuntimeError(
            f"[{spec.name}] legacy history has no CDC watermark. "
            "Review Bronze/history consistency, then explicitly set "
            "SCD2_ACCEPT_LEGACY_BASELINE=true once to start CDC after "
            f"Bronze snapshot {snapshot_id}."
        )

    write_watermark(spark, pipeline_name, snapshot_id)
    validate_history(spark, spec)
    print(
        f"[SCD2 CDC] {spec.name}: initialized legacy baseline "
        f"at bronze_snapshot={snapshot_id}"
    )
    return True


def apply_cdc_scd2(
    spark: SparkSession,
    spec: DimensionSpec,
    dry_run: bool = False,
) -> None:
    """Apply one incremental Bronze CDC micro-batch to Silver history."""
    ensure_history_table(spark, spec)
    if dry_run:
        events, snapshot_id = read_cdc_batch(spark, spec)
        counts = {
            row["op"]: row["count"]
            for row in events.groupBy("op").count().collect()
        }
        print(
            f"[SCD2 CDC dry-run] {spec.name}: events={sum(counts.values()):,}, "
            f"create={counts.get('c', 0):,}, snapshot={counts.get('r', 0):,}, "
            f"update={counts.get('u', 0):,}, delete={counts.get('d', 0):,}, "
            f"bronze_snapshot={snapshot_id or 'none'}"
        )
        return

    if initialize_legacy_baseline(spark, spec):
        return

    raw_events, snapshot_id = read_cdc_batch(spark, spec)
    events = raw_events.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        history = spark.table(spec.history_table)
        prepared = prepare_events(events, history, spec).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        try:
            counts = {
                row["op"]: row["count"]
                for row in prepared.groupBy("op").count().collect()
            }
            event_count = sum(counts.values())
            if event_count:
                validate_event_order(prepared, history, spec)
                staged = stage_merge_rows(prepared, history, spec)
                merge_history(spark, staged, spec)

            validate_history(spark, spec)
            _commit_watermark(spark, spec, snapshot_id)
            print(
                f"[SCD2 CDC] {spec.name}: events={event_count:,}, "
                f"create={counts.get('c', 0):,}, "
                f"snapshot={counts.get('r', 0):,}, "
                f"update={counts.get('u', 0):,}, "
                f"delete={counts.get('d', 0):,}, "
                f"bronze_snapshot={snapshot_id or 'none'}"
            )
        finally:
            prepared.unpersist()
    finally:
        events.unpersist()
