"""
============================================================================
 spark_jobs/dim_snapshot.py
 Snapshot dimensions from PostgreSQL into Iceberg Silver current tables
============================================================================
"""
import os
import sys

from pyspark.sql.functions import col, current_timestamp

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from feature_store.config import get_spark_session


def get_postgres_jdbc_options(table: str) -> dict:
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "ecommerce")
    pg_user = os.getenv("POSTGRES_USER", "admin")
    pg_password = os.getenv("POSTGRES_PASSWORD", "admin")

    return {
        "url": f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}",
        "dbtable": table,
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver",
    }


def snapshot_users(spark) -> None:
    users_df = (
        spark.read.format("jdbc")
        .options(**get_postgres_jdbc_options("public.users"))
        .load()
        .select(
            col("id").cast("bigint").alias("user_id"),
            col("full_name"),
            col("email"),
            col("phone"),
            col("city"),
            col("created_at").cast("timestamp").alias("source_created_at"),
        )
        .withColumn("snapshot_ts", current_timestamp())
    )

    (
        users_df.writeTo("iceberg.silver.dim_users_current")
        .using("iceberg")
        .createOrReplace()
    )


def snapshot_categories(spark) -> None:
    categories_df = (
        spark.read.format("jdbc")
        .options(**get_postgres_jdbc_options("public.categories"))
        .load()
        .select(
            col("id").cast("int").alias("category_id"),
            col("name").alias("category_name"),
            col("description"),
        )
        .withColumn("snapshot_ts", current_timestamp())
    )

    (
        categories_df.writeTo("iceberg.silver.dim_categories_current")
        .using("iceberg")
        .createOrReplace()
    )


def snapshot_products(spark) -> None:
    products_df = (
        spark.read.format("jdbc")
        .options(**get_postgres_jdbc_options("public.products"))
        .load()
        .select(
            col("id").cast("bigint").alias("product_id"),
            col("name").alias("product_name"),
            col("category_id").cast("int").alias("category_id"),
            col("base_price").cast("decimal(12,2)").alias("base_price"),
            col("stock").cast("int").alias("stock"),
            col("is_active").cast("boolean").alias("is_active"),
            col("created_at").cast("timestamp").alias("source_created_at"),
            col("updated_at").cast("timestamp").alias("source_updated_at"),
        )
        .withColumn("snapshot_ts", current_timestamp())
    )

    (
        products_df.writeTo("iceberg.silver.dim_products_current")
        .using("iceberg")
        .createOrReplace()
    )


def ensure_schemas(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")


def main() -> None:
    spark = get_spark_session("Dimension Snapshot Job")
    spark.sparkContext.setLogLevel("WARN")

    ensure_schemas(spark)

    snapshot_users(spark)
    snapshot_categories(spark)
    snapshot_products(spark)

    print("[Snapshot] Completed users/products/categories snapshots to Iceberg.")
    spark.stop()


if __name__ == "__main__":
    main()
