import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max



def read_watermark(spark : SparkSession, path: str) -> int:
    """
    Read the watermark from the specified path in iceberg.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The S3 path to read the watermark from.

    Returns:
        int: The watermark value. Returns 0 if the file does not exist or is empty.
    """
    try:
        # Read the watermark file as a DataFrame
        df = spark.read.json(path)
        # Extract the watermark value from the DataFrame
        watermark_value = df.collect()[0]
        return row["last_ingestion_ts"] if watermark_value else 0
    except Exception as e:
        print(f"Error reading watermark from {path}: {e}")
        return 0  # Return 0 if the file does not exist or is empty


def write_watermark(spark: SparkSession, path: str, watermark_value: int):
    data = [{"last_ingestion_ts": watermark_value}]
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").json(path)