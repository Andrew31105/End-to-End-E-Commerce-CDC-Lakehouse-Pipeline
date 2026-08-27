from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import StructField, StructType, TimestampType


WATERMARK_PATH = "s3a://iceberg-datalake/checkpoint/silver/water_mask"


def read_watermark(spark: SparkSession, path: str) -> Optional[datetime]:
    try:
        # Read the watermark file as a DataFrame
        schema = StructType([
            StructField("last_ingestion_ts", TimestampType(), True),
        ])
        df = spark.read.schema(schema).json(path)
        rows = df.take(1)
        return rows[0]["last_ingestion_ts"] if rows else None
    except Exception as e:
        print(f"Error reading watermark from {path}: {e}")
        return None


def write_watermark(
    spark: SparkSession, path: str, watermark_value: datetime
) -> None:
    # Dùng Spark SQL thay vì createDataFrame để tránh phụ thuộc pandas/numpy
    ts_str = (
        watermark_value.strftime("%Y-%m-%d %H:%M:%S")
        if hasattr(watermark_value, "strftime")
        else str(watermark_value)
    )
    df = spark.sql(f"SELECT CAST('{ts_str}' AS TIMESTAMP) AS last_ingestion_ts")
    df.write.mode("overwrite").json(path)


def read_new_spark(spark: SparkSession, bronze_path: str) -> DataFrame:
    full_df = spark.read.format("parquet").load(bronze_path)

    last_watermark = read_watermark(spark, WATERMARK_PATH)

    if last_watermark is None:
        new_df = full_df
    else:
        new_df = full_df.filter(col("ingestion_ts") > lit(last_watermark))

    if new_df.isEmpty():
        return new_df, last_watermark

    # Không ghi watermark ở đây — caller phải ghi SAU KHI transform thành công,
    # để nếu pipeline fail thì dữ liệu vẫn được xử lý lại ở lần chạy kế tiếp.
    current_max_watermark = new_df.agg(spark_max("ingestion_ts")).collect()[0][0]
    return new_df,current_max_watermark