#!/usr/bin/env python3
"""
============================================================================
 scripts/olist_prepare.py
 Chuẩn hóa dataset Olist (Kaggle CSV) → Parquet sẵn sàng nạp vào Lakehouse
============================================================================
 Input : Dataset/*.csv   (Olist Brazilian E-Commerce, license CC BY-NC-SA 4.0)
 Output: data/olist/prepared/
           ├── dim_categories.parquet
           ├── dim_products.parquet
           ├── dim_users.parquet
           └── orders_resolved.parquet

 VÌ SAO DÙNG DUCKDB (thay vì pandas):
   - SQL-first: join 6 file CSV (~300 MB) bằng SQL đọc/rà soát dễ hơn nhiều
     so với chuỗi pandas merge + rename + reset_index.
   - Columnar, xử lý ngoài RAM: vài triệu dòng vẫn chạy trên laptop.
   - Ghi Parquet native (ZSTD), không cần dựng Spark chỉ để transform local.

 ÁNH XẠ Olist → schema hệ thống:
   1 đơn Olist có nhiều order_items (mỗi item = 1 sản phẩm/dòng), nhưng bảng
   `orders` của hệ thống phẳng (1 đơn = 1 sản phẩm) ⇒ gộp items về 1 dòng:
       quantity     = COUNT(items)
       total_amount = SUM(price + freight_value) × fx
       unit_price   = total_amount / quantity      (giữ tổng = đơn giá × SL)
       product_id   = sản phẩm có price lớn nhất trong đơn (arg_max)

   Olist không có các trường hệ thống cần ⇒ sinh thêm (xem build_dim_*):
       products.name/base_price/stock, users.full_name/email/phone.

 KHÔNG GIAN ID (offset) – tránh đụng seed data & id do CDC sinh ra:
       categories 1_000+   |  products 100_000+
       users    1_000_000+ |  orders  100_000_000+
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CATEGORY_ID_OFFSET = 1_000
PRODUCT_ID_OFFSET = 100_000
USER_ID_OFFSET = 1_000_000
ORDER_ID_OFFSET = 100_000_000

# tên file CSV trong Dataset/
CSV_ORDERS = "olist_orders_dataset.csv"
CSV_ITEMS = "olist_order_items_dataset.csv"
CSV_CUSTOMERS = "olist_customers_dataset.csv"
CSV_PRODUCTS = "olist_products_dataset.csv"
CSV_TRANSLATION = "product_category_name_translation.csv"

FIRST_NAMES = [
    "Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Henrique",
    "Isabela", "Joao", "Karina", "Lucas", "Mariana", "Nelson", "Olivia",
    "Paulo", "Renata", "Rafael", "Sofia", "Thiago",
]
LAST_NAMES = [
    "Almeida", "Barbosa", "Cardoso", "Dias", "Esteves", "Ferreira", "Gomes",
    "Henriques", "Iglesias", "Junqueira", "Klein", "Lima", "Moreira", "Nogueira",
    "Oliveira", "Pereira", "Queiroz", "Ribeiro", "Santos", "Teixeira",
]

# order_status của Olist → status hợp lệ của hệ thống
# (pending, confirmed, shipped, delivered, cancelled, returned)
STATUS_CASE = """
    CASE o.order_status
        WHEN 'created'     THEN 'pending'
        WHEN 'approved'    THEN 'confirmed'
        WHEN 'invoiced'    THEN 'confirmed'
        WHEN 'processing'  THEN 'confirmed'
        WHEN 'shipped'     THEN 'shipped'
        WHEN 'delivered'   THEN 'delivered'
        WHEN 'canceled'    THEN 'cancelled'
        WHEN 'unavailable' THEN 'returned'
        ELSE 'pending'
    END
"""


# ---------------------------------------------------------------- helpers
def read_csv(path: Path) -> str:
    """Biểu thức SQL đọc CSV (auto-detect schema/header)."""
    return f"read_csv_auto('{path.as_posix()}', header = true)"


def sql_list(values: list[str]) -> str:
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


def scalar(con: duckdb.DuckDBPyConnection, query: str):
    return con.execute(query).fetchone()[0]


# ---------------------------------------------------------------- dimensions
def build_dim_categories(con: duckdb.DuckDBPyConnection, src: Path) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_categories AS
        WITH mapped AS (
            SELECT COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS en_name,
                   COALESCE(p.product_category_name, 'n/a')                                        AS pt_name
            FROM {read_csv(src / CSV_PRODUCTS)} p
            LEFT JOIN {read_csv(src / CSV_TRANSLATION)} t
                   ON t.product_category_name = p.product_category_name
        ), dedup AS (
            SELECT en_name, MIN(pt_name) AS pt_name
            FROM mapped
            GROUP BY en_name
        )
        SELECT CAST({CATEGORY_ID_OFFSET} + row_number() OVER (ORDER BY en_name) AS INTEGER) AS id,
               en_name                                       AS name,
               'Olist category (' || pt_name || ')'           AS description
        FROM dedup
    """)


def build_dim_products(con: duckdb.DuckDBPyConnection, src: Path, fx: float) -> None:
    # giá tham chiếu = median giá bán thực tế của sản phẩm trong order_items
    con.execute(f"""
        CREATE OR REPLACE TABLE item_stats AS
        SELECT product_id, MEDIAN(price) AS med_price
        FROM {read_csv(src / CSV_ITEMS)}
        GROUP BY product_id
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE dim_products AS
        WITH base AS (
            SELECT p.product_id AS olist_product_id,
                   COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS en_name,
                   COALESCE(s.med_price, 0) AS med_price
            FROM {read_csv(src / CSV_PRODUCTS)} p
            LEFT JOIN {read_csv(src / CSV_TRANSLATION)} t
                   ON t.product_category_name = p.product_category_name
            LEFT JOIN item_stats s ON s.product_id = p.product_id
        ), numbered AS (
            SELECT CAST({PRODUCT_ID_OFFSET} + row_number() OVER (ORDER BY olist_product_id) AS INTEGER) AS id,
                   olist_product_id, en_name, med_price
            FROM base
        )
        SELECT n.id,
               n.olist_product_id,
               replace(n.en_name, '_', ' ') || ' #' || CAST(n.id AS VARCHAR)      AS name,
               c.id                                                               AS category_id,
               CAST(ROUND(n.med_price * {fx}, 2) AS DECIMAL(18, 2))               AS base_price,
               CAST(20 + (hash(n.olist_product_id) % 181) AS INTEGER)             AS stock,
               TRUE                                                               AS is_active,
               TIMESTAMP '2016-09-01 00:00:00'                                    AS created_at,
               TIMESTAMP '2016-09-01 00:00:00'                                    AS updated_at
        FROM numbered n
        JOIN dim_categories c ON c.name = n.en_name
    """)


def build_dim_users(con: duckdb.DuckDBPyConnection, src: Path) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_users AS
        WITH customers AS (
            SELECT customer_unique_id,
                   MIN(customer_city)  AS city,
                   MIN(customer_state) AS state
            FROM {read_csv(src / CSV_CUSTOMERS)}
            GROUP BY customer_unique_id
        ), numbered AS (
            SELECT CAST({USER_ID_OFFSET} + row_number() OVER (ORDER BY customer_unique_id) AS INTEGER) AS id,
                   customer_unique_id, city, state
            FROM customers
        )
        SELECT n.id,
               n.customer_unique_id                                                            AS olist_customer_id,
               {sql_list(FIRST_NAMES)}[CAST(hash(n.customer_unique_id) % 20 AS INTEGER) + 1]
                 || ' ' ||
               {sql_list(LAST_NAMES)}[CAST((hash(n.customer_unique_id) // 20) % 20 AS INTEGER) + 1] AS full_name,
               'customer' || CAST(n.id AS VARCHAR) || '@olist.local'                            AS email,
               '+55' || lpad(CAST(hash(n.customer_unique_id) % 100000000 AS VARCHAR), 8, '0')   AS phone,
               upper(substr(COALESCE(NULLIF(TRIM(n.city), ''), 'unknown'), 1, 1))
                 || substr(COALESCE(NULLIF(TRIM(n.city), ''), 'unknown'), 2)                     AS city,
               TIMESTAMP '2016-09-01 00:00:00'                                                   AS created_at
        FROM numbered n
    """)


# ---------------------------------------------------------------- facts
def build_orders_joined(con: duckdb.DuckDBPyConnection, src: Path,
                        fx: float, max_date: str | None) -> None:
    """Gộp orders + order_items + customers + dims → bảng orders_joined.

    Chưa cấp order_id mới và CHƯA dịch trục thời gian, vì mốc dịch phải lấy
    trên chính tập đã join (vài đơn ở đầu/cuối bị loại do không có item).
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE item_agg AS
        SELECT order_id,
               COUNT(*)                   AS quantity,
               SUM(price)                 AS sum_price,
               SUM(freight_value)         AS sum_freight,
               arg_max(product_id, price) AS top_product_id
        FROM {read_csv(src / CSV_ITEMS)}
        GROUP BY order_id
    """)

    # Olist chỉ còn vài chục đơn sau 2018-08-31 (đuôi dữ liệu thưa) — cắt đi để
    # sau khi dịch trục thời gian, "cửa sổ mới nhất" của BI vẫn có dữ liệu.
    date_filter = (
        f"AND CAST(o.order_purchase_timestamp AS TIMESTAMP) <= TIMESTAMP '{max_date} 23:59:59'"
        if max_date else ""
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE orders_joined AS
        SELECT o.order_id                AS olist_order_id,
               u.id                      AS user_id,
               p.id                      AS product_id,
               CAST(i.quantity AS INTEGER) AS quantity,
               CAST(ROUND((i.sum_price + i.sum_freight) * {fx}, 2) AS DECIMAL(18, 2)) AS total_amount,
               {STATUS_CASE}             AS status,
               CAST(o.order_purchase_timestamp AS TIMESTAMP) AS ordered_at,
               CAST(COALESCE(o.order_delivered_customer_date,
                             o.order_delivered_carrier_date,
                             o.order_approved_at,
                             o.order_purchase_timestamp) AS TIMESTAMP) AS updated_at
        FROM {read_csv(src / CSV_ORDERS)} o
        JOIN {read_csv(src / CSV_CUSTOMERS)} c ON c.customer_id = o.customer_id
        JOIN dim_users    u ON u.olist_customer_id = c.customer_unique_id
        JOIN item_agg     i ON i.order_id = o.order_id
        JOIN dim_products p ON p.olist_product_id = i.top_product_id
        WHERE o.order_purchase_timestamp IS NOT NULL
          {date_filter}
    """)


def finalize_orders(con: duckdb.DuckDBPyConnection, sample: int, shift_seconds: int) -> None:
    """Dịch trục thời gian, cấp order_id mới và lấy mẫu (nếu cần)."""
    sample_clause = f"USING SAMPLE {sample} ROWS (reservoir, 42)" if sample else ""

    con.execute(f"""
        CREATE OR REPLACE TABLE orders_resolved AS
        WITH shifted AS (
            SELECT j.*,
                   j.ordered_at + to_seconds({shift_seconds}) AS ordered_shifted,
                   j.updated_at + to_seconds({shift_seconds}) AS updated_shifted
            FROM orders_joined j
        ), numbered AS (
            SELECT s.*,
                   CAST({ORDER_ID_OFFSET} + row_number() OVER (ORDER BY ordered_shifted, olist_order_id) AS BIGINT) AS new_order_id
            FROM shifted s
        )
        SELECT new_order_id                        AS order_id,
               user_id,
               product_id,
               quantity,
               status,
               ordered_shifted                     AS ordered_at,
               updated_shifted                     AS updated_at,
               CAST(ROUND(total_amount / quantity, 2) AS DECIMAL(18, 2)) AS unit_price,
               total_amount,
               olist_order_id
        FROM numbered
        {sample_clause}
        ORDER BY ordered_shifted, olist_order_id
    """)


# ---------------------------------------------------------------- export
def export(con: duckdb.DuckDBPyConnection, out: Path) -> dict[str, int]:
    out.mkdir(parents=True, exist_ok=True)
    tables = {
        "dim_categories": "id",
        "dim_products": "id",
        "dim_users": "id",
        "orders_resolved": "order_id",
    }
    counts = {}
    for table in tables:
        con.execute(f"""
            COPY {table} TO '{(out / f'{table}.parquet').as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        counts[table] = scalar(con, f"SELECT COUNT(*) FROM {table}")
    return counts


# ---------------------------------------------------------------- main
def main() -> None:
    parser = argparse.ArgumentParser(description="Chuẩn hóa dataset Olist → Parquet")
    parser.add_argument("--src", default="Dataset", help="Thư mục chứa CSV Olist (default: Dataset)")
    parser.add_argument("--out", default="data/olist/prepared", help="Thư mục ghi Parquet")
    parser.add_argument("--fx", type=float, default=5000.0,
                        help="Tỷ giá quy đổi BRL → VND để đồng nhất đơn vị tiền tệ (default: 5000)")
    parser.add_argument("--timeline", choices=["recent", "original"], default="recent",
                        help="recent: dịch toàn bộ trục thời gian để đơn mới nhất = hiện tại "
                             "(giúp metrics 1d/7d/30d của Gold không bị 0)")
    parser.add_argument("--sample", type=int, default=0,
                        help="Chỉ lấy ngẫu nhiên N đơn hàng (0 = tất cả) – dùng để chạy thử")
    parser.add_argument("--max-date", default="2018-08-31",
                        help="Bỏ các đơn sau ngày này (đuôi Olist rất thưa); 'none' để giữ nguyên")
    args = parser.parse_args()

    src = (PROJECT_ROOT / args.src).resolve() if not Path(args.src).is_absolute() else Path(args.src)
    out = (PROJECT_ROOT / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out)
    max_date = None if args.max_date.lower() == "none" else args.max_date

    missing = [f for f in (CSV_ORDERS, CSV_ITEMS, CSV_CUSTOMERS, CSV_PRODUCTS, CSV_TRANSLATION)
               if not (src / f).is_file()]
    if missing:
        raise SystemExit(f"Thiếu file CSV trong {src}: {', '.join(missing)}")

    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")

    print(f"[Prepare] Nguồn : {src}")
    print(f"[Prepare] Đích  : {out}")

    build_dim_categories(con, src)
    build_dim_products(con, src, args.fx)
    build_dim_users(con, src)
    build_orders_joined(con, src, args.fx, max_date)

    # ---- dịch trục thời gian (tuỳ chọn) ----
    shift_seconds = 0
    if args.timeline == "recent":
        # Mốc lấy trên chính tập ĐÃ JOIN ⇒ đơn cuối cùng rơi đúng vào hiện tại.
        max_ts = scalar(con, "SELECT MAX(ordered_at) FROM orders_joined")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        shift_seconds = int((now - max_ts).total_seconds())
        print(f"[Prepare] Timeline: {max_ts} → {now} (dịch {shift_seconds / 86400:.1f} ngày)")

    finalize_orders(con, args.sample, shift_seconds)

    counts = export(con, out)

    # ---- báo cáo nhanh ----
    revenue, min_ts, max_ts, products_no_price = con.execute("""
        SELECT SUM(total_amount), MIN(ordered_at), MAX(ordered_at),
               (SELECT COUNT(*) FROM dim_products WHERE base_price <= 0)
        FROM orders_resolved
    """).fetchone()

    print("\n[Prepare] Kết quả:")
    for table, rows in counts.items():
        print(f"  - {table:<17} {rows:>9,} dòng")
    print(f"  - Doanh thu tổng   {float(revenue):>15,.0f} VND")
    print(f"  - Khoảng thời gian {min_ts} → {max_ts}")
    if products_no_price:
        print(f"  ! Cảnh báo: {products_no_price} sản phẩm không có giá bán trong order_items")

    print("\n[Prepare] Xong. Bước tiếp theo: scripts/olist_load_postgres.py")


if __name__ == "__main__":
    main()
