from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import StructField, StructType, TimestampType


WATERMARK_PATH = "s3a://spark-checkpoint/silver/water_mask"


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
    data = [{"last_ingestion_ts": watermark_value}]
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").json(path)


def read_new_spark(spark: SparkSession, bronze_path: str) -> DataFrame:
    full_df = spark.read.format("parquet").load(bronze_path)

    last_watermark = read_watermark(spark, WATERMARK_PATH)

    if last_watermark is None:
        new_df = full_df
    else:
        new_df = full_df.filter(col("ingestion_ts") > lit(last_watermark))

    if new_df.isEmpty():
        return new_df

    current_max_watermark = new_df.agg(spark_max("ingestion_ts")).collect()[0][0]
    write_watermark(spark, WATERMARK_PATH, current_max_watermark)
    return new_df,current_max_watermark