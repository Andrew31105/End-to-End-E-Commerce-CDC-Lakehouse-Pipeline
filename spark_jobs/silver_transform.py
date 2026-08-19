"""
============================================================================
 spark_jobs/silver_transform.py
 Silver Layer – Làm sạch & chuẩn hóa dữ liệu, ghi vào Iceberg Silver
============================================================================
 Kiến trúc Feature Store / Lakehouse:
   MinIO Bronze (raw Parquet) → PySpark → Iceberg Silver tables
============================================================================
 (PLACEHOLDER – Code triển khai sau)
"""
from pyparsing import col

from feature_store.config import get_spark_session


def main():

    get_spark_session("Silver Transform Job")

    spark.sparkContext.setLogLevel("WARN")

    brozene_path = "s3a://bronze/orders/"

    order_record_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("order_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    bronze_schema = StructType([
        StructField("before", order_record_schema, True),
        StructField("after", order_record_schema, True),
        StructField("op", StringType(), True),
        StructField("debezium_ts_ms", LongType(), True),
        StructField("kafka_ts", TimestampType(), True),
        StructField("_raw_payload", StringType(), True),
        StructField("ingestion_ts", TimestampType(), True),
        StructField("batch_id", StringType(), True),
    ])

    bronze_df = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .load(bronze_path)
    )

    if batch_df.isEmpty():
        print(f"[Silver] Batch {batch_num}: không có dữ liệu mới.")
        return

    flattened_df = (
        batch_df
        .filter(col("op") != "d")
        .select(
            col("after.id").alias("order_id"),
            col("after.user_id").alias("user_id"),
            col("after.product_id").alias("product_id"),
            col("after.quantity").alias("quantity"),
            col("after.unit_price").alias("unit_price"),
            col("after.total_amount").alias("total_amount"),
            col("after.status").alias("status"),
            col("after.order_at").alias("order_at"),
            col("after.updated_at").alias("updated_at"),
            col("op"),
            col("debezium_ts_ms"),
            col("batch_id")
        )
    )

    deleted_ids_df = (
        batch_df
        .filter(col("op") == "d")
        .select(col("before.id").alias("order_id"))
    )
    
    # TODO: Đọc raw data từ MinIO Bronze (s3a://bronze/orders/)
    # TODO: Parse & flatten Debezium CDC records (lấy 'after' payload)
    # TODO: Chuẩn hóa data types, xử lý null, dedup theo primary key
    # TODO: Join với dimension tables (products, users, categories)
    # TODO: Ghi vào Iceberg Silver table (iceberg.silver.orders_clean)
    # TODO: Thêm data quality checks (null ratio, row count)

    print("[Silver] Transform job started (placeholder).")
    spark.stop()


if __name__ == "__main__":
    main()
