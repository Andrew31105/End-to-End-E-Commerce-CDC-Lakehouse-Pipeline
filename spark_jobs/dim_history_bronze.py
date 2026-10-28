"""
spark_jobs/dim_history_bronze.py
================================
Nạp CDC dimension (users / categories / products) từ Kafka vào Bronze.

Vì sao tách khỏi bronze_ingestion.py?
  * orders_raw và dims có schema/envelope khác nhau → mỗi job một trách nhiệm.
  * Dim là nguồn cho SCD2 nên Bronze phải giữ NGUYÊN envelope
    (before / after / op / ts_ms) để Silver dựng lại lịch sử.

Chuẩn hoá ở Bronze (không chứa business logic):
  1. Bỏ tombstone (value = NULL).
  2. from_json với schema TYPED cho từng bảng (không lưu JSON lỏng).
  3. Tách metadata: op, debezium_ts_ms, kafka_ts, ingestion_ts, batch_id.
  4. Giữ _raw_payload để replay/audit khi schema nguồn thay đổi.
  5. Partition theo ngày nạp (dim nhỏ, 1 partition/ngày là đủ).

Bảng đích:
  iceberg.bronze.users_raw
  iceberg.bronze.categories_raw
  iceberg.bronze.products_raw

Chạy:
  docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /opt/spark/jobs/dim_history_bronze.py
"""
import os
import sys
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from feature_store.config import get_spark_session

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKPOINT_ROOT = os.getenv(
    "BRONZE_CHECKPOINT_ROOT", "s3a://iceberg-datalake/checkpoint/raw/dims"
)

# ---------------------------------------------------------------------------
# Schema bản ghi trong envelope Debezium
# ---------------------------------------------------------------------------
# Lưu ý: register_cdc.sh đặt decimal.handling.mode=double → NUMERIC thành DOUBLE.
# Timestamp Debezium (time.precision.mode=adaptive) là ISO-8601 STRING;
# Silver sẽ cast bằng to_timestamp(). updated_at là cột thêm cho SCD2,
# nếu DB chưa có thì field này chỉ nhận NULL (không lỗi).
USERS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

CATEGORIES_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("base_price", DoubleType(), True),
    StructField("stock", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

DIMS = [
    {
        "name": "users",
        "topic": "pg.public.users",
        "table": "iceberg.bronze.users_raw",
        "schema": USERS_SCHEMA,
    },
    {
        "name": "categories",
        "topic": "pg.public.categories",
        "table": "iceberg.bronze.categories_raw",
        "schema": CATEGORIES_SCHEMA,
    },
    {
        "name": "products",
        "topic": "pg.public.products",
        "table": "iceberg.bronze.products_raw",
        "schema": PRODUCTS_SCHEMA,
    },
]

_SQL_TYPES = {
    IntegerType: "INT",
    LongType: "BIGINT",
    StringType: "STRING",
    DoubleType: "DOUBLE",
    BooleanType: "BOOLEAN",
}


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
def _struct_sql(schema: StructType) -> str:
    """Sinh STRUCT<...> từ StructType để dùng trong CREATE TABLE."""
    fields = []
    for field in schema.fields:
        sql_type = _SQL_TYPES.get(type(field.dataType))
        if sql_type is None:
            raise TypeError(
                f"Chưa map kiểu {field.dataType} của cột '{field.name}'"
            )
        fields.append(f"{field.name}: {sql_type}")
    return "STRUCT<" + ", ".join(fields) + ">"


def ensure_table(spark: SparkSession, cfg: dict) -> None:
    struct_sql = _struct_sql(cfg["schema"])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg['table']} (
            before          {struct_sql},
            after           {struct_sql},
            op              STRING,
            debezium_ts_ms  BIGINT,
            kafka_ts        TIMESTAMP,
            _raw_payload    STRING,
            ingestion_ts    TIMESTAMP,
            batch_id        STRING
        ) USING iceberg
        PARTITIONED BY (days(ingestion_ts))
    """)


# ---------------------------------------------------------------------------
# Parse + normalize
# ---------------------------------------------------------------------------
def envelope_schema(record_schema: StructType) -> StructType:
    return StructType([
        StructField("before", record_schema, True),
        StructField("after", record_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ])


def parse_envelope(raw: DataFrame, record_schema: StructType) -> DataFrame:
    """Kafka value -> (before, after, op, ts_ms, raw payload).

    Tombstone (value = NULL) bị loại: Debezium phát tombstone sau DELETE,
    giữ lại chỉ tạo dòng rỗng vô nghĩa.
    """
    envelope = envelope_schema(record_schema)
    return (
        raw.filter(F.col("value").isNotNull())
        .selectExpr(
            "CAST(value AS STRING) AS json_value",
            "timestamp AS kafka_ts",
        )
        .withColumn("data", F.from_json("json_value", envelope))
        .select(
            F.col("data.before").alias("before"),
            F.col("data.after").alias("after"),
            F.col("data.op").alias("op"),
            F.col("data.ts_ms").alias("debezium_ts_ms"),
            F.col("kafka_ts"),
            F.col("json_value").alias("_raw_payload"),
        )
    )


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------
def start_stream(spark: SparkSession, cfg: dict, batch_id: str):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", cfg["topic"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        parse_envelope(raw, cfg["schema"])
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )

    return (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{cfg['name']}/")
        .toTable(cfg["table"])
    )


def main() -> None:
    spark = get_spark_session("Bronze Dimension Ingestion")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    batch_id = str(uuid.uuid4())
    queries = []
    for cfg in DIMS:
        ensure_table(spark, cfg)
        queries.append(start_stream(spark, cfg, batch_id))

    # 3 stream chạy song song; availableNow sẽ tự dừng sau khi đọc hết offset
    for query in queries:
        query.awaitTermination()

    for cfg in DIMS:
        total = spark.table(cfg["table"]).count()
        print(f"[Bronze dims] {cfg['table']}: {total:,} dòng")

    print(f"[Bronze dims] Completed (batch_id={batch_id}).")
    spark.stop()


if __name__ == "__main__":
    main()
