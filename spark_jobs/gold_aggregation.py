"""
============================================================================
 spark_jobs/gold_aggregation.py
 Gold Layer – Aggregation cho Feature Store (KPI, windowed features)
============================================================================
 Kiến trúc Feature Store / Lakehouse:
   Iceberg Silver → PySpark → Iceberg Gold (feature tables)
============================================================================
 (PLACEHOLDER – Code triển khai sau)
"""
from curses import window

from pyparsing import col

from pyspark.sql import SparkSession
from feature_store.config import get_spark_session
from pyspark.sql.functions import avg, expr, count, sum as spark_sum, countDistinct

def build_city_kpis(spark: SparkSession, df : DataFrame):
    city_kpis = (df.groupBy(window("order_at", "1 hour"), "user_city") 
                    .agg(
                spark_sum("total_amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("total_amount").alias("avg_order_value"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_city").alias("city"),
                col("total_revenue").cast("decimal(10, 2)").alias("total_revenue"),
                col("order_count").cast("int").alias("order_count"),
                col("avg_order_value").cast("decimal(10, 2)").alias("avg_order_value"),

            ))
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.gold.city_kpis (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    city STRING,
    total_revenue DECIMAL(10, 2),
    order_count INT,
    avg_order_value DECIMAL(10, 2)
    """)

    spark.sql("""
    MERGE INTO iceberg.gold.city_kpis AS target
    USING city_kpis AS source
    ON target.window_start = source.window_start AND target.city = source.city
    WHEN MATCHED THEN UPDATE SET
        total_revenue = source.total_revenue,
        order_count = source.order_count,
        avg_order_value = source.avg_order_value
    WHEN NOT MATCHED THEN INSERT (
        window_start,
        window_end,
        city,
        total_revenue,
        order_count,
        avg_order_value
    ) VALUES (
        source.window_start,
        source.window_end,
        source.city,
        source.total_revenue,
        source.order_count,
        source.avg_order_value
    )
    """)


def build_product_kpis(spark: SparkSession, df : DataFrame):
    product_kpis = (
        
    )



def main():
    spark = get_spark_session("FeatureStore-Gold-Aggregation")
    df = spark.read.table("iceberg.silver.orders_cleaned")

    build_city_kpis(spark, df)
    


    

    print("[Gold] Aggregation job started (placeholder).")
    spark.stop()


if __name__ == "__main__":
    main()
