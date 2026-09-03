from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession


WATERMARK_TABLE = "iceberg.metadata.watermarks"
BRONZE_TABLE = "iceberg.bronze.orders_raw"


def _ensure_watermark_table(spark: SparkSession) -> None:
    """Tạo namespace và bảng Iceberg lưu watermark (snapshot id) nếu chưa tồn tại.
    KHÔNG dùng CREATE OR REPLACE mỗi lần gọi vì sẽ xóa sạch dữ liệu watermark
    vừa ghi. Chỉ tạo nếu chưa có; nếu bảng cũ (schema last_ingestion_ts) thì
    drop một lần để migrate sang schema mới."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.metadata")
    try:
        cols = [r["col_name"] for r in spark.sql(f"DESCRIBE {WATERMARK_TABLE}").collect()]
        if "last_snapshot_id" not in cols:
            spark.sql(f"DROP TABLE IF EXISTS {WATERMARK_TABLE} PURGE")
    except Exception:
        pass
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            pipeline_name       STRING,
            last_snapshot_id    STRING
        ) USING iceberg
    """)


def read_watermark(spark: SparkSession, pipeline_name: str = "silver") -> Optional[str]:
    """Đọc snapshot id đã xử lý gần nhất từ bảng watermark."""
    try:
        _ensure_watermark_table(spark)
        df = spark.sql(f"""
            SELECT last_snapshot_id
            FROM {WATERMARK_TABLE}
            WHERE pipeline_name = '{pipeline_name}'
        """)
        rows = df.take(1)
        return str(rows[0]["last_snapshot_id"]) if rows else None
    except Exception as e:
        print(f"Error reading watermark for pipeline '{pipeline_name}': {e}")
        return None


def write_watermark(
    spark: SparkSession, pipeline_name: str, snapshot_id: str
) -> None:
    """Ghi/cập nhật snapshot id đã xử lý vào bảng watermark."""
    _ensure_watermark_table(spark)
    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT '{pipeline_name}' AS pipeline_name,
                      '{snapshot_id}' AS last_snapshot_id) AS source
        ON target.pipeline_name = source.pipeline_name
        WHEN MATCHED THEN UPDATE SET last_snapshot_id = source.last_snapshot_id
        WHEN NOT MATCHED THEN INSERT (pipeline_name, last_snapshot_id)
            VALUES (source.pipeline_name, source.last_snapshot_id)
    """)


def get_current_snapshot_id(
    spark: SparkSession, table_name: str = BRONZE_TABLE
) -> Optional[str]:
    """Lấy snapshot id mới nhất của bảng Bronze (metadata table .snapshots)."""
    try:
        rows = spark.sql(f"""
            SELECT snapshot_id
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
        """).collect()
        return str(rows[0]["snapshot_id"]) if rows else None
    except Exception as e:
        print(f"Error reading current snapshot of '{table_name}': {e}")
        return None


def read_new_spark(
    spark: SparkSession, table_name: str = BRONZE_TABLE
) -> Tuple[DataFrame, Optional[str]]:
    """
    Incremental read Bronze (Iceberg table) theo snapshot id:
    - Lần đầu (chưa có watermark)          → đọc TOÀN BỘ bảng
    - Các lần sau (có watermark snapshot)  → chỉ đọc dữ liệu mới
      từ các snapshot SAU snapshot đã xử lý (start-snapshot-id exclusive)
    Không ghi watermark ở đây — caller phải ghi SAU KHI transform thành công,
    để nếu pipeline fail thì dữ liệu vẫn được xử lý lại ở lần chạy kế tiếp.
    """
    last_snapshot = read_watermark(spark, "silver")
    current_snapshot = get_current_snapshot_id(spark, table_name)

    if current_snapshot is None:
        # Bảng chưa có snapshot nào (hoặc chưa tồn tại) → trả về DataFrame rỗng
        try:
            new_df = spark.read.format("iceberg").table(table_name)
        except Exception:
            new_df = spark.range(0).limit(0)
        return new_df, None

    if last_snapshot is None:
        # Lần đầu: chưa có watermark → đọc toàn bộ bảng
        new_df = spark.read.format("iceberg").table(table_name)
    elif last_snapshot == current_snapshot:
        # Không có commit mới → incremental range rỗng sẽ lỗi,
        # nên trả DataFrame rỗng (giữ schema) thay vì gọi start-snapshot-id
        new_df = spark.read.format("iceberg").table(table_name).limit(0)
    else:
        # Có snapshot mới → chỉ đọc dữ liệu từ các snapshot sau watermark
        new_df = (
            spark.read.format("iceberg")
            .option("start-snapshot-id", last_snapshot)
            .table(table_name)
        )

    if new_df.isEmpty():
        return new_df, current_snapshot

    return new_df, current_snapshot