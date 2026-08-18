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
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("FeatureStore-Gold-Aggregation")
        .getOrCreate()
    )

    # TODO: Đọc từ Iceberg Silver tables
    # TODO: Build city-level KPIs: avg/median price, order volume
    # TODO: Build brand/category-level KPIs: market share, price trends
    # TODO: Windowed aggregations (1-min, 5-min, 1-hour)
    # TODO: Ghi vào Iceberg Gold tables (iceberg.gold.*)
    # TODO: Publish feature metadata vào Feature Store registry

    print("[Gold] Aggregation job started (placeholder).")
    spark.stop()


if __name__ == "__main__":
    main()
