"""
============================================================================
 feature_store/config.py
 Feature Store – Định nghĩa feature groups, entity, metadata
============================================================================
 (PLACEHOLDER – Code triển khai sau)
"""
from pyspark.sql import SparkSession
import os 

load_dotenv_flag = os.getenv("LOAD_DOTENV", "True").lower() == "true"
if load_dotenv_flag:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()

# Feature Groups Definition
FEATURE_GROUPS = {
    "order_features": {
        "description": "Real-time order aggregation features (1-min window)",
        "source": "clickhouse",
        "entity": "product_id",
        "features": [
            # TODO: "total_orders_1min", "total_revenue_1min",
            # TODO: "avg_order_value_1min", "status_changed_count"
        ],
        "sla": "1 minute",
    },
    "user_features": {
        "description": "User-level historical features (batch, daily)",
        "source": "trino",
        "entity": "user_id",
        "features": [
            # TODO: "total_orders_30d", "total_spent_30d",
            # TODO: "favorite_category", "avg_basket_size"
        ],
        "sla": "1 day",
    },
    "product_features": {
        "description": "Product popularity & trend features",
        "source": "trino",
        "entity": "product_id",
        "features": [
            # TODO: "order_count_7d", "revenue_7d",
            # TODO: "stock_turnover_rate", "trending_score"
        ],
        "sla": "1 hour",
    },
}


def get_spark_session(app_name: str) -> SparkSession:
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minio")

    """
    Create and return a SparkSession with the given application name.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0")
                #minio
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        #iceberg
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/")
        .getOrCreate()
    )
    return spark