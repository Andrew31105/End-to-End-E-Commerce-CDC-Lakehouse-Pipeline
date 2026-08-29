from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import StructField, StructType, TimestampType


WATERMARK_TABLE = "iceberg.metadata.watermarks"


def _ensure_watermark_table(spark: SparkSession) -> None:
    """Tạo namespace và bảng Iceberg lưu watermark nếu chưa tồn tại."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.metadata")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            pipeline_name       STRING,
            last_ingestion_ts   TIMESTAMP
        ) USING iceberg
    """)


def read_watermark(spark: SparkSession, pipeline_name: str = "silver") -> Optional[datetime]:
    """Đọc watermark từ bảng Iceberg."""
    try:
        _ensure_watermark_table(spark)
        df = spark.sql(f"""
            SELECT last_ingestion_ts
            FROM {WATERMARK_TABLE}
            WHERE pipeline_name = '{pipeline_name}'
        """)
        rows = df.take(1)
        return rows[0]["last_ingestion_ts"] if rows else None
    except Exception as e:
        print(f"Error reading watermark for pipeline '{pipeline_name}': {e}")
        return None


def write_watermark(
    spark: SparkSession, pipeline_name: str, watermark_value: datetime
) -> None:
    """Ghi/cập nhật watermark vào bảng Iceberg."""
    _ensure_watermark_table(spark)
    ts_str = (
        watermark_value.strftime("%Y-%m-%d %H:%M:%S")
        if hasattr(watermark_value, "strftime")
        else str(watermark_value)
    )

    # MERGE: update nếu đã có, insert nếu chưa
    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT '{pipeline_name}' AS pipeline_name,
                      CAST('{ts_str}' AS TIMESTAMP) AS last_ingestion_ts) AS source
        ON target.pipeline_name = source.pipeline_name
        WHEN MATCHED THEN UPDATE SET last_ingestion_ts = source.last_ingestion_ts
        WHEN NOT MATCHED THEN INSERT (pipeline_name, last_ingestion_ts)
            VALUES (source.pipeline_name, source.last_ingestion_ts)
    """)


def read_new_spark(
    spark: SparkSession, bronze_path: str = "s3a://iceberg-datalake/bronze/raw/orders/"
) -> Tuple[DataFrame, Optional[datetime]]:
    """Đọc dữ liệu mới từ Bronze (Parquet trên MinIO) dựa trên watermark Iceberg."""
    full_df = spark.read.format("parquet").load(bronze_path)

    last_watermark = read_watermark(spark, "silver")

    if last_watermark is None:
        new_df = full_df
    else:
        new_df = full_df.filter(col("ingestion_ts") > lit(last_watermark))

    if new_df.isEmpty():
        return new_df, last_watermark

    # Không ghi watermark ở đây — caller phải ghi SAU KHI transform thành công,
    # để nếu pipeline fail thì dữ liệu vẫn được xử lý lại ở lần chạy kế tiếp.
    current_max_watermark = new_df.agg(spark_max("ingestion_ts")).collect()[0][0]
    return new_df, current_max_watermark