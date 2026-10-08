"""
============================================================================
 spark_jobs/bronze_ingestion.py
 Bronze Layer – Đọc CDC events từ Kafka, lưu raw vào MinIO (Bronze Zone)
============================================================================
 Kiến trúc Feature Store / Lakehouse:
   Kafka (CDC events) → PySpark batch → MinIO Bronze (Parquet raw)
============================================================================
"""
import os
import sys
import time
import uuid

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    LongType
)

from feature_store.config import get_spark_session

# Schema 1 bản ghi đơn hàng trong envelope Debezium (before/after).
# Dùng chung cho cả luồng CDC thật và luồng backfill (backfill_olist.py).
ORDER_RECORD_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("total_amount", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
    StructField("ordered_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])


def ensure_bronze_schema(spark) -> None:
    """Tạo namespace + table nếu chưa tồn tại (schema khớp sql/iceberg_ddl.sql).

    Nếu thiếu namespace, Iceberg báo NoSuchNamespaceException; .toTable() chỉ tạo
    table bên trong namespace đã tồn tại.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
            before          STRUCT<
                                id: INT,
                                user_id: INT,
                                product_id: INT,
                                quantity: INT,
                                unit_price: DECIMAL(10, 2),
                                total_amount: DECIMAL(10, 2),
                                status: STRING,
                                ordered_at: STRING,
                                updated_at: STRING
                            >,
            after           STRUCT<
                                id: INT,
                                user_id: INT,
                                product_id: INT,
                                quantity: INT,
                                unit_price: DECIMAL(10, 2),
                                total_amount: DECIMAL(10, 2),
                                status: STRING,
                                ordered_at: STRING,
                                updated_at: STRING
                            >,
            op              STRING,
            debezium_ts_ms  BIGINT,
            kafka_ts        TIMESTAMP,
            _raw_payload    STRING,
            ingestion_ts    TIMESTAMP,
            batch_id        STRING
        ) USING iceberg
        PARTITIONED BY (days(ingestion_ts))
    """)


def main():
    spark = get_spark_session("Bronze Ingestion Job")
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    batch_id = str(uuid.uuid4())

    # Đảm bảo namespace + bảng tồn tại trước khi mở streaming sink
    ensure_bronze_schema(spark)

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "pg.public.orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    order_record_schema = ORDER_RECORD_SCHEMA

    debezium_schema =  StructType([
            StructField("before", order_record_schema, True),
            StructField("after", order_record_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])
    
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_ts")
        .withColumn("data", from_json(col("json_value"), debezium_schema))
        .select(
            col("data.before").alias("before"),
            col("data.after").alias("after"),
            col("data.op").alias("op"),
            col("data.ts_ms").alias("debezium_ts_ms"),
            col("kafka_ts"),
            col("json_value").alias("_raw_payload")
        )
    )

    enriched_df = (
        parsed_df.withColumn("ingestion_ts", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )
    enriched_df.printSchema()

    # Ghi vào Iceberg table (không phải file parquet rời) để
    # tầng Silver có thể incremental read theo snapshot id.
    # Lưu ý: DataStreamWriter dùng .toTable(...), KHÔNG có .table(...)
    query = (
        enriched_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", "s3a://iceberg-datalake/checkpoint/raw/orders/")
        .toTable("iceberg.bronze.orders_raw")
    )

    query.awaitTermination()
    print(f"[Bronze] Ingestion batch {batch_id} completed.")
    df = spark.read.format("iceberg").table("iceberg.bronze.orders_raw")
    df.show()
    spark.stop()


if __name__ == "__main__":
    main()
