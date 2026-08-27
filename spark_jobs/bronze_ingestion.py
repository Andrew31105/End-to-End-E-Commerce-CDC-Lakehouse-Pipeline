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


def main():
    spark = get_spark_session("Bronze Ingestion Job")
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    batch_id = str(uuid.uuid4())

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "pg.public.orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )



    


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

    query = (
        enriched_df.writeStream
        .format("parquet")
        .option("path", "s3a://iceberg-datalake/bronze/raw/orders/")
        .option("checkpointLocation", "s3a://iceberg-datalake/checkpoint/raw/orders/")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )


    query.awaitTermination()
    print(f"[Bronze] Ingestion batch {batch_id} completed.")
    df = spark.read.parquet("s3a://iceberg-datalake/bronze/raw/orders/")
    df.show()
    spark.stop()


if __name__ == "__main__":
    main()
