"""
============================================================================
 spark_jobs/silver_transform.py
 Silver Layer – Làm sạch & chuẩn hóa dữ liệu, ghi vào Iceberg Silver
============================================================================
 Kiến trúc Feature Store / Lakehouse:
   MinIO Bronze (raw Parquet) → PySpark → Iceberg Silver tables
============================================================================
"""
import os
import sys

from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    current_timestamp,
    desc,
    lit,
    row_number,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    LongType,
    TimestampType
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from feature_store.config import get_spark_session
from scripts.water_mask import read_new_spark, write_watermark

bronze_table = "iceberg.bronze.orders_raw"

ORDERS_TABLE = "iceberg.silver.orders_clean"
HIST_TABLES = {
    "product": "iceberg.silver.dim_products_hist",
    "category": "iceberg.silver.dim_categories_hist",
    "user": "iceberg.silver.dim_users_hist",
}

# valid_from của version bootstrap (xem dim_scd2.py) → đánh dấu "inferred"
BOOTSTRAP_VALID_FROM = "1900-01-01 00:00:00"

# Cột thêm vào orders_clean (bảng cũ sẽ được ALTER idempotent)
EXTRA_COLUMNS = {
    "source_ts": "TIMESTAMP",   # debezium ts của event → guard cho MERGE
    "product_sk": "BIGINT",
    "category_sk": "BIGINT",
    "user_sk": "BIGINT",
    "dim_match_type": "STRING",
}


def ensure_dim_history(spark: SparkSession) -> None:
    """Fail-fast nếu SCD2 chưa chạy.

    Join với bảng hist rỗng sẽ trả NULL âm thầm → dashboard mất doanh thu
    theo city/category mà không ai biết. Thà fail sớm với thông báo rõ.
    """
    for table in HIST_TABLES.values():
        try:
            has_rows = spark.read.table(table).limit(1).count() > 0
        except Exception as exc:
            raise RuntimeError(
                f"[Silver] Không đọc được {table}: {exc}. "
                "Chạy spark_jobs/dim_scd2.py trước."
            ) from exc
        if not has_rows:
            raise RuntimeError(
                f"[Silver] {table} rỗng – chạy spark_jobs/dim_scd2.py trước."
            )


def join_dimension(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Point-in-time join orders với 3 dimension SCD2.

    Chuỗi join (điểm dễ sai nhất):
      1. orders → product version tại `order_at`
      2. product version → category version tại **cùng** `order_at`
         (khoá join là p.category_id của PRODUCT VERSION, không phải từ orders)
      3. orders → user version tại `order_at`

    Điều kiện thời gian luôn là `valid_from <= order_at < valid_to`.
    """
    orders = df.alias("o")

    with_product = orders.join(
        broadcast(spark.read.table(HIST_TABLES["product"])).alias("p"),
        (col("o.product_id") == col("p.product_id"))
        & (col("o.order_at") >= col("p.valid_from"))
        & (col("o.order_at") < col("p.valid_to")),
        "left",
    )

    # SCD2 #2: category theo thời điểm đặt, khoá lấy từ version của product.
    with_category = with_product.join(
        broadcast(spark.read.table(HIST_TABLES["category"])).alias("c"),
        (col("p.category_id") == col("c.category_id"))
        & (col("o.order_at") >= col("c.valid_from"))
        & (col("o.order_at") < col("c.valid_to")),
        "left",
    )

    with_user = with_category.join(
        broadcast(spark.read.table(HIST_TABLES["user"])).alias("u"),
        (col("o.user_id") == col("u.user_id"))
        & (col("o.order_at") >= col("u.valid_from"))
        & (col("o.order_at") < col("u.valid_to")),
        "left",
    )

    bootstrap = lit(BOOTSTRAP_VALID_FROM).cast("timestamp")
    is_inferred = (
        (col("p.valid_from") == bootstrap)
        | (col("c.valid_from") == bootstrap)
        | (col("u.valid_from") == bootstrap)
    )

    return with_user.select(
        col("o.*"),
        col("p.product_sk"),
        col("p.product_name"),
        col("c.category_sk"),
        # Nếu category hist thiếu key thì vẫn giữ category_id của product version
        coalesce(col("c.category_id"), col("p.category_id")).alias("category_id"),
        col("c.category_name"),
        col("u.user_sk"),
        col("u.city"),
        when(col("p.product_sk").isNull(), "missing")
        .when(col("c.category_sk").isNull() | col("u.user_sk").isNull(), "partial")
        .when(is_inferred, "inferred")
        .otherwise("exact")
        .alias("dim_match_type"),
    )


def ensure_orders_schema(spark: SparkSession) -> None:
    """Tạo bảng orders_clean + bổ sung cột SCD2 cho bảng đã tồn tại."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
            order_id        BIGINT,
            user_id         BIGINT,
            product_id      BIGINT,
            product_name    STRING,
            category_id     INT,
            category_name   STRING,
            user_city       STRING,
            quantity        INT,
            unit_price      DECIMAL(12, 2),
            total_amount    DECIMAL(12, 2),
            status          STRING,
            ordered_at      TIMESTAMP,
            updated_at      TIMESTAMP,
            cdc_op          STRING,
            ingested_at     TIMESTAMP,
            source_ts       TIMESTAMP,
            product_sk      BIGINT,
            category_sk     BIGINT,
            user_sk         BIGINT,
            dim_match_type  STRING
        ) USING iceberg
        PARTITIONED BY (months(ordered_at))
    """)

    existing = set(spark.table(ORDERS_TABLE).columns)
    missing = {
        name: dtype
        for name, dtype in EXTRA_COLUMNS.items()
        if name not in existing
    }
    if missing:
        columns = ", ".join(f"{name} {dtype}" for name, dtype in missing.items())
        spark.sql(f"ALTER TABLE {ORDERS_TABLE} ADD COLUMNS ({columns})")
        print(f"[Silver] Added columns: {', '.join(missing)}")


def main():

    spark = get_spark_session("Silver Transform Job")

    spark.sparkContext.setLogLevel("WARN")

    # Fail sớm nếu dim_scd2 chưa chạy (hist rỗng → join trả NULL âm thầm)
    ensure_dim_history(spark)

    order_record_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("ordered_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    bronze_schema = StructType([
        StructField("before", order_record_schema, True),
        StructField("after", order_record_schema, True),
        StructField("op", StringType(), True),
        StructField("debezium_ts_ms", LongType(), True),
        StructField("kafka_ts", TimestampType(), True),
        StructField("_raw_payload", StringType(), True),
        StructField("ingestion_ts", TimestampType(), True),
        StructField("batch_id", StringType(), True),
    ])

    bronze_df, new_water_mark = read_new_spark(spark, bronze_table)

    if bronze_df.isEmpty():
        print(f"[Silver] {bronze_table}: không có dữ liệu mới.")
        return

    flattened_df = (
        bronze_df
        .filter(col("op") != "d")
        .select(
            col("after.id").alias("order_id"),
            col("after.user_id").alias("user_id"),
            col("after.product_id").alias("product_id"),
            col("after.quantity").alias("quantity"),
            col("after.unit_price").alias("unit_price"),
            col("after.total_amount").alias("total_amount"),
            col("after.status").alias("status"),
            col("after.ordered_at").alias("order_at"),
            col("after.updated_at").alias("updated_at"),
            col("op"),
            col("debezium_ts_ms"),
            col("batch_id")
        )
    )

    # Hard delete (op='d') hiện chưa được xử lý: luồng demo không xoá đơn và
    # bảng Silver giữ trạng thái cuối theo order_id. Nếu cần soft-delete, thêm
    # status='deleted' và đồng bộ filter ở Gold (đừng chỉ thêm MERGE ở đây).

    type_df = (
        flattened_df
        .withColumn("order_id", col("order_id").cast("bigint"))
        .withColumn("user_id", col("user_id").cast("bigint"))
        .withColumn("product_id", col("product_id").cast("bigint"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .withColumn("total_amount", col("total_amount").cast("decimal(10,2)"))
        .withColumn("status", col("status").cast("string"))
        .withColumn("order_at", col("order_at").cast("timestamp"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )


    typed_df = (
    type_df
    .withColumn("order_at", to_timestamp(col("order_at")))
    .withColumn("updated_at", to_timestamp(col("updated_at")))
    .withColumn("quantity", coalesce(col("quantity"), lit(0)))
    .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
    .withColumn("total_amount", coalesce(col("total_amount"), lit(0.0)))
    .withColumn("status", coalesce(col("status"), lit("unknown")))
    .withColumn("updated_at", coalesce(col("updated_at"), col("order_at")))   
    )

    valid_df = (
        typed_df.filter(
            (col("order_id").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("quantity") >= 0) &
            (col("unit_price") >= 0.0) &
            (col("total_amount") >= 0.0)
        )
    )

    data_quality_df = type_df.count() - valid_df.count()

    print(f"[Silver] Data quality issues found: {data_quality_df}")

    window_spec = Window.partitionBy("order_id").orderBy(desc("debezium_ts_ms"))

    deduped_df = (
        valid_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    joined_df = join_dimension(deduped_df, spark)

    # Tạo bảng + bổ sung cột SCD2 nếu bảng đã tồn tại
    ensure_orders_schema(spark)

    # Iceberg mở 1 Parquet writer cho MỖI partition mà một task gặp phải (fanout).
    # Partition theo NGÀY ⇒ backfill 24 tháng tạo ~730 partition, mỗi task phải
    # mở hàng trăm writer song song → tràn heap executor.
    #   • Partition theo THÁNG ⇒ chỉ ~24 partition cho 2 năm lịch sử, số writer
    #     mở đồng thời luôn bị chặn trên rất thấp.
    #   • Thêm 'range' để Spark cắt dữ liệu theo khoảng thời gian liên tục trước
    #     khi ghi, giảm tiếp số partition mà mỗi task chạm tới.
    spark.sql("""
        ALTER TABLE iceberg.silver.orders_clean
        SET TBLPROPERTIES ('write.distribution-mode' = 'range')
    """)

    target_df = (
        joined_df.select(
            col("order_id"),
            col("user_id"),
            col("product_id"),
            col("product_name"),
            col("category_id"),
            col("category_name"),
            col("city").alias("user_city"),
            col("quantity"),
            col("unit_price").cast("decimal(12, 2)"),
            col("total_amount").cast("decimal(12, 2)"),
            col("status"),
            col("order_at").alias("ordered_at"),
            col("updated_at"),
            col("op").alias("cdc_op"),
            current_timestamp().alias("ingested_at"),
            (col("debezium_ts_ms") / 1000).cast("timestamp").alias("source_ts"),
            col("product_sk"),
            col("category_sk"),
            col("user_sk"),
            col("dim_match_type"),
        )
    )
    target_df.createOrReplaceTempView("silver_orders_temp")

    spark.sql("""
        MERGE INTO iceberg.silver.orders_clean AS target
        USING silver_orders_temp AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.source_ts IS NOT NULL
                     AND source.source_ts > COALESCE(
                         target.source_ts, TIMESTAMP '1970-01-01')
        THEN UPDATE SET
            user_id       = source.user_id,
            product_id    = source.product_id,
            product_name  = source.product_name,
            category_id   = source.category_id,
            category_name = source.category_name,
            user_city     = source.user_city,
            quantity      = source.quantity,
            unit_price    = source.unit_price,
            total_amount  = source.total_amount,
            status        = source.status,
            ordered_at    = source.ordered_at,
            updated_at    = source.updated_at,
            cdc_op        = source.cdc_op,
            ingested_at   = source.ingested_at,
            source_ts     = source.source_ts,
            product_sk    = source.product_sk,
            category_sk   = source.category_sk,
            user_sk       = source.user_sk,
            dim_match_type = source.dim_match_type
        WHEN NOT MATCHED THEN INSERT (
            order_id, user_id, product_id, product_name, category_id, category_name,
            user_city, quantity, unit_price, total_amount, status, ordered_at,
            updated_at, cdc_op, ingested_at, source_ts,
            product_sk, category_sk, user_sk, dim_match_type
        ) VALUES (
            source.order_id, source.user_id, source.product_id, source.product_name,
            source.category_id, source.category_name, source.user_city, source.quantity,
            source.unit_price, source.total_amount, source.status, source.ordered_at,
            source.updated_at, source.cdc_op, source.ingested_at, source.source_ts,
            source.product_sk, source.category_sk, source.user_sk, source.dim_match_type
        )
    """)

    write_watermark(spark, "silver", new_water_mark)
    

    

    print("[Silver] Transform completed → iceberg.silver.orders_clean.")
    spark.stop()


if __name__ == "__main__":
    main()
