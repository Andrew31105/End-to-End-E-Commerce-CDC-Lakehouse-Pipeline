"""
============================================================================
 spark_jobs/gold_aggregation.py
 Gold Layer – Aggregation cho Feature Store (KPI, windowed features)
============================================================================
 Kiến trúc Feature Store / Lakehouse:
   Iceberg Silver → PySpark → Iceberg Gold (feature tables)

 Pipeline:
   iceberg.silver.orders_clean  →  iceberg.gold.city_revenue_hourly
                                →  iceberg.gold.category_revenue_hourly
                                →  iceberg.gold.product_features_daily
============================================================================
"""
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    sum as spark_sum,
    window,
)

from feature_store.config import get_spark_session

SILVER_ORDERS_TABLE = "iceberg.silver.orders_clean"


def ensure_schemas(spark: SparkSession) -> None:
    """Tạo namespace Iceberg nếu chưa tồn tại (Hive Metastore database).

    `CREATE TABLE ... iceberg.gold.*` sẽ lỗi NoSuchNamespaceException nếu
    namespace chưa tồn tại.
    """
    for namespace in ("iceberg.silver", "iceberg.gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def build_city_kpis(spark: SparkSession, df: DataFrame) -> None:
    valid_orders = df.filter(~col("status").isin("cancelled", "returned"))

    city_kpis = (
        valid_orders
        .groupBy(window(col("ordered_at"), "1 hour"), col("user_city"))
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("user_city").alias("city"),
            col("total_revenue").cast("decimal(18, 2)").alias("total_revenue"),
            col("order_count").cast("bigint").alias("order_count"),
            col("avg_order_value").cast("decimal(12, 2)").alias("avg_order_value"),
        )
    )
    city_kpis.createOrReplaceTempView("city_kpis_src")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.city_revenue_hourly (
            window_start    TIMESTAMP,
            city            STRING,
            total_revenue   DECIMAL(18, 2),
            order_count     BIGINT,
            avg_order_value DECIMAL(12, 2)
        ) USING iceberg
        PARTITIONED BY (hours(window_start))
    """)

    spark.sql("""
        MERGE INTO iceberg.gold.city_revenue_hourly AS target
        USING city_kpis_src AS source
        ON target.window_start = source.window_start AND target.city = source.city
        WHEN MATCHED THEN UPDATE SET
            total_revenue = source.total_revenue,
            order_count = source.order_count,
            avg_order_value = source.avg_order_value
        WHEN NOT MATCHED THEN INSERT (
            window_start, city, total_revenue, order_count, avg_order_value
        ) VALUES (
            source.window_start, source.city, source.total_revenue,
            source.order_count, source.avg_order_value
        )
    """)



def build_category_kpis(spark: SparkSession, df: DataFrame) -> None:
    valid_orders = df.filter(~col("status").isin("cancelled", "returned"))

    category_kpis = (
        valid_orders
        .groupBy(
            window(col("ordered_at"), "1 hour"),
            col("category_id"),
            col("category_name"),
        )
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("category_id").cast("int").alias("category_id"),
            col("category_name").cast("string").alias("category_name"),
            col("total_revenue").cast("decimal(18, 2)").alias("total_revenue"),
            col("order_count").cast("bigint").alias("order_count"),
        )
    )
    category_kpis.createOrReplaceTempView("category_kpis_src")

    # Schema khớp sql/iceberg_ddl.sql (gold.category_revenue_hourly)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.category_revenue_hourly (
            window_start    TIMESTAMP,
            category_id     INT,
            category_name   STRING,
            total_revenue   DECIMAL(18, 2),
            order_count     BIGINT
        ) USING iceberg
        PARTITIONED BY (hours(window_start))
    """)

    spark.sql("""
        MERGE INTO iceberg.gold.category_revenue_hourly AS target
        USING category_kpis_src AS source
        ON target.window_start = source.window_start
           AND target.category_id = source.category_id
        WHEN MATCHED THEN UPDATE SET
            category_name = source.category_name,
            total_revenue = source.total_revenue,
            order_count = source.order_count
        WHEN NOT MATCHED THEN INSERT (
            window_start, category_id, category_name, total_revenue, order_count
        ) VALUES (
            source.window_start, source.category_id, source.category_name,
            source.total_revenue, source.order_count
        )
    """)
                    


def build_product_kpis(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("silver_orders_v")

    spark.sql("""
        SELECT product_id,
               COUNT(CASE WHEN ordered_at >= date_sub(CURRENT_DATE, 1)  THEN 1 END) AS order_count_1d,
               COUNT(CASE WHEN ordered_at >= date_sub(CURRENT_DATE, 7)  THEN 1 END) AS order_count_7d,
               COUNT(CASE WHEN ordered_at >= date_sub(CURRENT_DATE, 30) THEN 1 END) AS order_count_30d,
               COALESCE(SUM(CASE WHEN ordered_at >= date_sub(CURRENT_DATE, 1)  THEN total_amount END), 0) AS revenue_1d,
               COALESCE(SUM(CASE WHEN ordered_at >= date_sub(CURRENT_DATE, 7)  THEN total_amount END), 0) AS revenue_7d,
               COALESCE(SUM(CASE WHEN ordered_at >= date_sub(CURRENT_DATE, 30) THEN total_amount END), 0) AS revenue_30d
        FROM silver_orders_v
        WHERE ordered_at >= date_sub(CURRENT_DATE, 30)
          AND status NOT IN ('cancelled', 'returned')
        GROUP BY product_id
    """).createOrReplaceTempView("product_kpis_src")

    product_features_daily = spark.sql("""
        SELECT
            CURRENT_DATE                                           AS snapshot_date,
            CAST(p.product_id   AS BIGINT)                         AS product_id,
            p.product_name,
            CAST(p.category_id  AS INT)                            AS category_id,
            CAST(COALESCE(k.order_count_1d, 0)  AS BIGINT)         AS order_count_1d,
            CAST(COALESCE(k.order_count_7d, 0)  AS BIGINT)         AS order_count_7d,
            CAST(COALESCE(k.order_count_30d, 0) AS BIGINT)         AS order_count_30d,
            CAST(COALESCE(k.revenue_1d, 0)  AS DECIMAL(18, 2))     AS revenue_1d,
            CAST(COALESCE(k.revenue_7d, 0)  AS DECIMAL(18, 2))     AS revenue_7d,
            CAST(COALESCE(k.revenue_30d, 0) AS DECIMAL(18, 2))     AS revenue_30d,
            -- trending_score = đơn 1 ngày gần nhất / TB đơn mỗi ngày trong 30 ngày
            CAST(
                COALESCE(k.order_count_1d, 0)
                / GREATEST(COALESCE(k.order_count_30d, 0) / 30.0, 0.01)
                AS DOUBLE
            )                                                      AS trending_score
        FROM iceberg.silver.dim_products_current p
        LEFT JOIN product_kpis_src k
               ON p.product_id = k.product_id
    """)
    product_features_daily.createOrReplaceTempView("product_features_src")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.product_features_daily (
            snapshot_date   DATE,
            product_id      BIGINT,
            product_name    STRING,
            category_id     INT,
            order_count_1d  BIGINT,
            order_count_7d  BIGINT,
            order_count_30d BIGINT,
            revenue_1d      DECIMAL(18, 2),
            revenue_7d      DECIMAL(18, 2),
            revenue_30d     DECIMAL(18, 2),
            trending_score  DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(snapshot_date))
    """)

    spark.sql("""
        MERGE INTO iceberg.gold.product_features_daily AS target
        USING product_features_src AS source
        ON target.snapshot_date = source.snapshot_date
           AND target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET
            product_name    = source.product_name,
            category_id     = source.category_id,
            order_count_1d  = source.order_count_1d,
            order_count_7d  = source.order_count_7d,
            order_count_30d = source.order_count_30d,
            revenue_1d      = source.revenue_1d,
            revenue_7d      = source.revenue_7d,
            revenue_30d     = source.revenue_30d,
            trending_score  = source.trending_score
        WHEN NOT MATCHED THEN INSERT (
            snapshot_date, product_id, product_name, category_id,
            order_count_1d, order_count_7d, order_count_30d,
            revenue_1d, revenue_7d, revenue_30d, trending_score
        ) VALUES (
            source.snapshot_date, source.product_id, source.product_name, source.category_id,
            source.order_count_1d, source.order_count_7d, source.order_count_30d,
            source.revenue_1d, source.revenue_7d, source.revenue_30d, source.trending_score
        )
    """)


def main() -> None:
    spark = get_spark_session("FeatureStore-Gold-Aggregation")
    spark.sparkContext.setLogLevel("WARN")

    ensure_schemas(spark)

    df = spark.read.table(SILVER_ORDERS_TABLE)

    build_city_kpis(spark, df)
    build_category_kpis(spark, df)
    build_product_kpis(spark, df)

    print("[Gold] Aggregation completed → iceberg.gold.*")
    spark.stop()


if __name__ == "__main__":
    main()
