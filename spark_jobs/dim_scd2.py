"""
spark_jobs/dim_scd2.py
======================
SCD Type 2 cho 3 dimension: users / categories / products.

Nguồn hiện tại: snapshot JDBC từ PostgreSQL (Phase A).
  * Lần đầu (hist rỗng): valid_from = 1900-01-01 → phủ toàn bộ đơn backfill.
  * Các lần sau:
      - row_hash đổi → đóng version cũ (valid_to = source_ts) + mở version mới
      - key biến mất → đóng version (cdc_op = 'd', soft delete)

KHÔNG ghi dim_*_current: bảng đó vẫn do dim_snapshot.py cập nhật (Type 1,
có cột stock biến động nhanh) nên API/Gold không phải sửa.

Chạy:
  docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /opt/spark/jobs/dim_scd2.py
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from feature_store.config import get_spark_session

SENTINEL = "TIMESTAMP '9999-12-31 00:00:00'"
BOOTSTRAP_VALID_FROM = "1900-01-01 00:00:00"

DIMS = [
    {
        "name": "categories",
        "table": "public.categories",
        "hist": "iceberg.silver.dim_categories_hist",
        "key": "category_id",
        "sk": "category_sk",
        "tracked": ["category_name", "description"],
        "exprs": [
            "id AS category_id",
            "name AS category_name",
            "description",
            "updated_at AS source_ts",
        ],
        "ddl": """CREATE TABLE IF NOT EXISTS iceberg.silver.dim_categories_hist (
                    category_sk BIGINT, category_id INT, category_name STRING,
                    description STRING, source_ts TIMESTAMP, valid_from TIMESTAMP,
                    valid_to TIMESTAMP, is_current BOOLEAN, row_hash STRING,
                    cdc_op STRING, ingested_at TIMESTAMP) USING iceberg""",
    },
    {
        "name": "products",
        "table": "public.products",
        "hist": "iceberg.silver.dim_products_hist",
        "key": "product_id",
        "sk": "product_sk",
        "tracked": ["product_name", "category_id", "base_price", "is_active"],
        "exprs": [
            "id AS product_id",
            "name AS product_name",
            "category_id",
            "CAST(base_price AS DECIMAL(12,2)) AS base_price",
            "is_active",
            "updated_at AS source_ts",
        ],
        "ddl": """CREATE TABLE IF NOT EXISTS iceberg.silver.dim_products_hist (
                    product_sk BIGINT, product_id BIGINT, product_name STRING,
                    category_id INT, base_price DECIMAL(12,2), is_active BOOLEAN,
                    source_ts TIMESTAMP, valid_from TIMESTAMP, valid_to TIMESTAMP,
                    is_current BOOLEAN, row_hash STRING, cdc_op STRING,
                    ingested_at TIMESTAMP) USING iceberg""",
    },
    {
        "name": "users",
        "table": "public.users",
        "hist": "iceberg.silver.dim_users_hist",
        "key": "user_id",
        "sk": "user_sk",
        "tracked": ["full_name", "email", "phone", "city"],
        "exprs": [
            "id AS user_id",
            "full_name",
            "email",
            "phone",
            "city",
            "updated_at AS source_ts",
        ],
        "ddl": """CREATE TABLE IF NOT EXISTS iceberg.silver.dim_users_hist (
                    user_sk BIGINT, user_id BIGINT, full_name STRING, email STRING,
                    phone STRING, city STRING, source_ts TIMESTAMP,
                    valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN,
                    row_hash STRING, cdc_op STRING, ingested_at TIMESTAMP)
                  USING iceberg""",
    },
]


def jdbc_options(table: str) -> dict:
    return {
        "url": (f"jdbc:postgresql://{os.getenv('PG_HOST', 'localhost')}:"
                f"{os.getenv('PG_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'ecommerce')}"),
        "dbtable": table,
        "user": os.getenv("POSTGRES_USER", "admin"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }


def read_snapshot(spark: SparkSession, cfg: dict) -> DataFrame:
    """Đọc 1 dimension từ Postgres và tính row_hash trên cột tracked."""
    raw = spark.read.format("jdbc").options(**jdbc_options(cfg["table"])).load()
    return raw.selectExpr(*cfg["exprs"]).withColumn(
        "row_hash",
        F.sha2(F.concat_ws("|", *[
            F.coalesce(F.col(c).cast("string"), F.lit("\u0000"))
            for c in cfg["tracked"]
        ]), 256),
    )


def apply_scd2(spark: SparkSession, cfg: dict, src: DataFrame) -> None:
    hist, key = cfg["hist"], cfg["key"]
    spark.sql(cfg["ddl"])

    is_bootstrap = spark.table(hist).limit(1).count() == 0
    current = (
        spark.table(hist).filter("is_current")
        .select(F.col(key), F.col("row_hash").alias("current_hash"))
    )

    changes = (
        src.alias("s")
        .join(current.alias("c"), key, "full_outer")
        .select(
            F.coalesce(F.col(f"s.{key}"), F.col(f"c.{key}")).alias(key),
            *[F.col(f"s.{c}").alias(c) for c in cfg["tracked"]],
            F.col("s.row_hash").alias("row_hash"),
            F.col("s.source_ts").alias("source_ts"),
            F.when(F.col(f"s.{key}").isNull(), F.lit("d"))
             .when(F.col(f"c.{key}").isNull(), F.lit("i"))
             .otherwise(F.lit("u")).alias("op"),
            F.col("c.current_hash").alias("current_hash"),
        )
        .filter(
            (F.col("op") == "d")
            | F.col("current_hash").isNull()
            | (F.col("row_hash") != F.col("current_hash"))
        )
    )

    valid_from = (
        F.lit(BOOTSTRAP_VALID_FROM).cast("timestamp") if is_bootstrap
        else F.coalesce(F.col("source_ts"), F.current_timestamp())
    )
    changes = changes.withColumn("valid_from", valid_from).persist()
    changes.createOrReplaceTempView("dim_changes")
    affected = changes.count()

    # 1) Đóng version đang hiệu lực
    spark.sql(f"""
        MERGE INTO {hist} AS t
        USING dim_changes AS s
        ON t.{key} = s.{key} AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            valid_to   = COALESCE(s.source_ts, current_timestamp()),
            is_current = false,
            cdc_op     = s.op
    """)

    # 2) Mở version mới (idempotent theo key + valid_from)
    spark.sql(f"""
        INSERT INTO {hist}
        SELECT xxhash64(s.{key}, s.valid_from),
               {", ".join("s." + c for c in cfg["tracked"])},
               s.source_ts, s.valid_from, {SENTINEL}, true,
               s.row_hash, s.op, current_timestamp()
        FROM dim_changes s
        WHERE s.op IN ('i', 'u')
          AND NOT EXISTS (
              SELECT 1 FROM {hist} h
              WHERE h.{key} = s.{key} AND h.valid_from = s.valid_from
          )
    """)

    changes.unpersist()
    print(f"[SCD2] {hist}: {affected} thay đổi "
          f"(bootstrap={is_bootstrap})")


def main() -> None:
    spark = get_spark_session("Silver SCD2 Dimensions")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    for cfg in DIMS:
        apply_scd2(spark, cfg, read_snapshot(spark, cfg))

    print("[SCD2] Completed.")
    spark.stop()


if __name__ == "__main__":
    main()
