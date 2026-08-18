"""
============================================================================
 spark_jobs/bronze_ingestion.py
 Bronze Layer – Đọc CDC events từ Kafka, lưu raw vào MinIO (Bronze Zone)
============================================================================
 Kiến trúc Feature Store / Lakehouse:
   Kafka (CDC events) → PySpark batch → MinIO Bronze (Parquet raw)
============================================================================
 (PLACEHOLDER – Code triển khai sau)
"""
import os
import sys
import time

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


def main():
    spark = get_spark_session("Bronze Ingestion Job")
    spark.sparkContext.setLogLevel("WARN")

    batch_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", "pg.public.orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    if raw_df.rdd.isEmpty():
        print("[Bronze] No new CDC events found in Kafka topic 'pg.public.orders'.")
        spark.stop()
        return


    


    order_record_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("order_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    debezium_schema =  StructType([
            StructField("before", order_record_schema, True),
            StructField("after", order_record_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])
    


    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) as json_str", "CAST(timestamp as kafka_ts")
        withColumn(
            "data",
            from_json(col("json_str"), debezium_schema)
        )
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


    

    # TODO: Đọc CDC events từ Kafka topic 'pg.public.orders'
    # TODO: Parse Debezium envelope (before/after/op)
    # TODO: Ghi raw Parquet vào MinIO bucket 'bronze' (s3a://bronze/orders/)
    # TODO: Thêm ingestion timestamp, batch_id

    print("[Bronze] Ingestion job started (placeholder).")
    spark.stop()


if __name__ == "__main__":
    main()
