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
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, row_number, desc
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    LongType
)
from pyspark.sql import DataFrame
from feature_store.config import get_spark_session
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from scripts.water_mask import read_new_spark
from scripts.water_mask import  write_watermark

bronze_path = "s3a://bronze/orders/"

def join_dimension(df: DataFrame, spark: SparkSession) -> DataFrame:
        products_df = spark.read.table("iceberg.silver.dim_products_current")
        categories_df = spark.read.table("iceberg.silver.dim_categories_current")
        users_df = spark.read.table("iceberg.silver.dim_users_current")

        joined_df1 = (
            df.join(products_df, df.product_id == products_df.product_id, "left")
            .drop(products_df.product_id)
        )

        joined_df2 = (
            joined_df1.join(categories_df, joined_df1.category_id == categories_df.category_id, "left")
            .drop(categories_df.category_id)
        )
        joined_df3 = (
            joined_df2.join(users_df, joined_df2.user_id == users_df.user_id, "left")
            .drop(users_df.user_id)
        )


        return joined_df3


def main():

    spark = get_spark_session("Silver Transform Job")

    spark.sparkContext.setLogLevel("WARN")

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

    bronze_df, new_water_mark = read_new_spark(spark, bronze_path)

    if bronze_df.isEmpty():
        print(f"[Silver] Batch {batch_num}: không có dữ liệu mới.")
        return

    flattened_df = (
        bronze_df
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
        bronze_df
        .filter(col("op") == "d")
        .select(
                col("before.id").alias("order_id"),
                col("before.user_id").alias("user_id"),
                col("before.product_id").alias("product_id"),
                col("before.quantity").alias("quantity"),
                col("before.unit_price").alias("unit_price"),
                col("before.total_amount").alias("total_amount"),
                col("before.status").alias("status"),
                col("before.order_at").alias("order_at"),
                col("before.updated_at").alias("updated_at"),
        )
        
    )

    type_df = (
        flattened_df
        .withColumn("order_id", col("order_id").cast("bigint"))
        .withColumn("user_id", col("user_id").cast("bigint"))
        .withColumn("product_id", col("product_id").cast("bigint"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .withColumn("total_amount", col("total_amount").cast("decimal(10,2)"))
        .withColumn("status", col("status").cast("string"))
        .withColumn("order_at", col("order_at").cast("timestamp"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )


    typed_df = (
    type_df
    .withColumn("order_at", to_timestamp(col("order_at")))
    .withColumn("updated_at", to_timestamp(col("updated_at")))
    .withColumn("quantity", coalesce(col("quantity"), lit(0)))
    .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
    .withColumn("total_amount", coalesce(col("total_amount"), lit(0.0)))
    .withColumn("status", coalesce(col("status"), lit("unknown")))
    .withColumn("updated_at", coalesce(col("updated_at"), col("order_at")))   
    )

    valid_df = (
        typed_df.filter(
            (col("order_id").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("quantity") >= 0) &
            (col("unit_price") >= 0.0) &
            (col("total_amount") >= 0.0)
        )
    )

    data_quality_df = type_df.size() - valid_df.size()

    print(f"[Silver] Data quality issues found: {data_quality_df}")

    window_spec = Window.partitionBy("id").orderBy(desc("debezium_ts_ms"))

    deduped_df = (
        valid_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    joined_df = join_dimension(deduped_df, spark)



    joined_df.createOrReplaceTempView("silver_orders_temp")

    spark.sql(""" MERGE INTO iceberg.silver.orders_clean AS target
                USING silver_orders_temp AS source
                ON target.order_id = source.order_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""" )

    write_watermark(spark, "s3a://spark-checkpoint/silver/water_mask", new_water_mark)
    

    

    print("[Silver] Transform job started (placeholder).")
    spark.stop()


if __name__ == "__main__":
    main()
