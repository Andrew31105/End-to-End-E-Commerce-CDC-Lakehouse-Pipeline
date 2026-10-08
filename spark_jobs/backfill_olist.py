"""
============================================================================
 spark_jobs/backfill_olist.py
 Backfill dữ liệu Olist (Parquet đã chuẩn hóa) vào Iceberg Bronze
============================================================================
 Vai trò trong kiến trúc:
   Parquet (lịch sử Olist) → Spark  →  iceberg.bronze.orders_raw
   Sau đó Silver/Gold xử lý y hệt như dữ liệu CDC thật (không cần sửa gì).

 Điểm quan trọng:
   1. Bronze lưu envelope Debezium ⇒ job này TÁI TẠO envelope cho dữ liệu
      lịch sử: before = NULL, after = bản ghi đơn hàng, op = 'c' (create).
      Nhờ vậy tầng Silver chỉ có MỘT đường xử lý duy nhất.
   2. `ingestion_ts = current_timestamp()` (thời điểm nạp thật) chứ KHÔNG phải
      ordered_at, để bảng được ghi vào đúng partition theo ngày nạp.
   3. Ghi 1 snapshot Iceberg mới ⇒ scripts/water_mask.py (đọc incremental theo
      snapshot id) tự nhận ra phần dữ liệu mới ở lần chạy Silver kế tiếp.
   4. order_id của dữ liệu này nằm ở dải 100_000_000+ nên không đụng id do
      CDC sinh ra (xem scripts/olist_prepare.py).

 Cách chạy:
   docker exec spark-master /opt/spark/bin/spark-submit \\
       --master spark://spark-master:7077 \\
       /opt/spark/jobs/backfill_olist.py
"""
from __future__ import annotations

import argparse
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    least,
    lit,
    struct,
    to_json,
    unix_timestamp,
)

from feature_store.config import get_spark_session

# ./spark_jobs được mount vào /opt/spark/jobs trong container ⇒ khi chạy bằng
# spark-submit thì import như module cùng thư mục, trên host thì import theo package.
try:
    from bronze_ingestion import ORDER_RECORD_SCHEMA, ensure_bronze_schema
except ImportError:  # pragma: no cover - chạy trên host
    from spark_jobs.bronze_ingestion import ORDER_RECORD_SCHEMA, ensure_bronze_schema

BRONZE_TABLE = "iceberg.bronze.orders_raw"
DEFAULT_INPUT = "/opt/spark/data/olist/orders_backfill.parquet"
BATCH_ID = "backfill-olist"

# Trần của DECIMAL(10,2) dùng trong Bronze; vượt ngưỡng sẽ bị cắt bớt.
MAX_DECIMAL_10_2 = 99_999_999.99


def build_payload(spark: SparkSession, input_path: str) -> DataFrame:
    """Đọc Parquet lịch sử → DataFrame đúng schema của iceberg.bronze.orders_raw."""
    source_df = spark.read.parquet(input_path)

    after = struct(
        col("order_id").cast("int").alias("id"),
        col("user_id").cast("int").alias("user_id"),
        col("product_id").cast("int").alias("product_id"),
        col("quantity").cast("int").alias("quantity"),
        least(col("unit_price"), lit(MAX_DECIMAL_10_2)).cast("decimal(10,2)").alias("unit_price"),
        least(col("total_amount"), lit(MAX_DECIMAL_10_2)).cast("decimal(10,2)").alias("total_amount"),
        col("status").cast("string").alias("status"),
        col("ordered_at").cast("timestamp").cast("string").alias("ordered_at"),
        col("updated_at").cast("timestamp").cast("string").alias("updated_at"),
    )

    # Khối "source" mô phỏng metadata connector của Debezium, đồng thời giữ
    # vết truy xuất về đơn hàng Olist gốc.
    source = struct(
        lit("olist-backfill").alias("connector"),
        lit(BATCH_ID).alias("batch_id"),
        col("olist_order_id").cast("string").alias("olist_order_id"),
        (unix_timestamp(col("ordered_at")) * 1000).alias("ts_ms"),
    )

    return source_df.select(
        lit(None).cast(ORDER_RECORD_SCHEMA).alias("before"),
        after.alias("after"),
        lit("c").alias("op"),
        (unix_timestamp(col("ordered_at")) * 1000).cast("long").alias("debezium_ts_ms"),
        current_timestamp().alias("kafka_ts"),
        to_json(struct(
            lit(None).cast(ORDER_RECORD_SCHEMA).alias("before"),
            after.alias("after"),
            lit("c").alias("op"),
            source.alias("source"),
        )).alias("_raw_payload"),
        current_timestamp().alias("ingestion_ts"),
        lit(BATCH_ID).alias("batch_id"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Olist history → Iceberg Bronze")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Đường dẫn Parquet đầu vào")
    parser.add_argument("--limit", type=int, default=0, help="Giới hạn số dòng (0 = tất cả)")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ đọc và in thống kê")
    args = parser.parse_args()

    if not os.path.isfile(args.input) and not args.input.startswith("s3"):
        raise SystemExit(
            f"Không tìm thấy {args.input}.\n"
            "Kiểm tra volume ./data/olist trong docker-compose.yml (spark-master/worker)."
        )

    spark = get_spark_session("Olist Backfill Job")
    spark.sparkContext.setLogLevel("WARN")

    ensure_bronze_schema(spark)

    payload = build_payload(spark, args.input)
    if args.limit:
        payload = payload.limit(args.limit)
    payload.cache()

    rows = payload.count()
    stats = payload.selectExpr(
        "min(after.ordered_at) AS min_ordered_at",
        "max(after.ordered_at) AS max_ordered_at",
        "sum(after.total_amount) AS revenue",
    ).first()
    print(f"[Backfill] Sẽ nạp {rows:,} đơn | {stats['min_ordered_at']} → {stats['max_ordered_at']}")
    print(f"[Backfill] Doanh thu   {float(stats['revenue']):,.0f}")

    existing = spark.sql(
        f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE} WHERE batch_id = '{BATCH_ID}'"
    ).first()["n"]
    if existing:
        print(f"[Backfill] Lưu ý: Bronze đã có {existing:,} dòng batch_id='{BATCH_ID}'. "
              "Chạy lại sẽ append thêm (Silver dedup theo order_id + debezium_ts_ms nên kết quả không đổi).")

    if args.dry_run:
        payload.printSchema()
        print("[Backfill] dry-run: không ghi dữ liệu.")
        spark.stop()
        return

    payload.writeTo(BRONZE_TABLE).append()

    total = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE}").first()["n"]
    print(f"[Backfill] Hoàn tất. Bronze hiện có {total:,} dòng.")

    spark.stop()


if __name__ == "__main__":
    main()
