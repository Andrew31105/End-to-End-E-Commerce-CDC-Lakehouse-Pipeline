"""
============================================================================
 feature_store/config.py
 Feature Store – Định nghĩa feature groups, entity, metadata
 + SparkSession factory dùng chung cho các spark job (Bronze/Silver/Gold)
============================================================================
"""
import os

load_dotenv_flag = os.getenv("LOAD_DOTENV", "True").lower() == "true"
if load_dotenv_flag:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()

# Feature Groups Definition
FEATURE_GROUPS = {
    "order_features": {
        "description": "Per-product 1-min window features (Flink -> ClickHouse)",
        "source": "clickhouse",
        "table": "product_metrics_1min",
        "entity": "product_id",
        "features": [
            "total_orders_1min",
            "total_revenue_1min",
            "avg_order_value_1min",
            "status_changed_count",
        ],
        "sla": "1 minute",
    },
    "user_features": {
        "description": "User-level historical features (batch, daily)",
        "source": "trino",
        "table": "silver.orders_clean",
        "entity": "user_id",
        "features": [
            "total_orders_30d",
            "total_spent_30d",
            "favorite_category",
            "avg_basket_size",
        ],
        "sla": "1 day",
    },
    "product_features": {
        "description": "Product popularity & trend features",
        "source": "trino",
        "table": "gold.product_features_daily",
        "entity": "product_id",
        "features": [
            "order_count_7d",
            "revenue_7d",
            "stock_turnover_rate",
            "trending_score",
        ],
        "sla": "1 hour",
    },
}


def get_spark_session(app_name: str) -> "SparkSession":
    from pyspark.sql import SparkSession  # import nhẹ: chỉ khi thực sự tạo Spark

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    hive_metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg-datawarehouse/")

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.4",
    ]
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Packages JARs
        .config("spark.jars.packages", ",".join(packages))

        # Iceberg SQL Extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalogImplementation", "hive")

        # MinIO S3A Configuration
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Default warehouse / Hive metastore config
        .config("spark.sql.warehouse.dir", iceberg_warehouse)
        .config("spark.hadoop.hive.metastore.uris", hive_metastore_uri)

        # Iceberg Catalog Configuration (Hive Metastore)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", hive_metastore_uri)
        .config("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
        .getOrCreate()
    )
    return spark