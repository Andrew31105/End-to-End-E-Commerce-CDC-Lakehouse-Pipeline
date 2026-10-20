#!/usr/bin/env python3
"""
============================================================================
 scripts/olist_load_postgres.py
 Nạp 3 bảng dimension của Olist (Parquet) vào PostgreSQL source DB
============================================================================
 Input : data/olist/prepared/{dim_categories,dim_products,dim_users}.parquet
 Target: PostgreSQL - categories, products, users

 Vì sao phải nạp dims vào Postgres?
   Silver join `orders_clean` với `silver.dim_*` (được snapshot từ Postgres
   bằng spark_jobs/dim_snapshot.py) để lấy product_name / category_name /
   user_city. Nếu KHÔNG nạp dims, mọi đơn backfill sẽ có city/category = NULL
   và toàn bộ dashboard BI sẽ trống.

 Đặc điểm:
   - Ghi id tường minh theo đúng offset trong olist_prepare.py, không đụng
     seed data (5 categories / 7 products / 5 users) và id do SERIAL sinh ra.
   - Idempotent: chạy lại nhiều lần chỉ UPDATE, dùng ON CONFLICT (id).
   - Truyền số liệu hàng loạt bằng execute_values (page_size 1000).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.db import connect, db_config  # noqa: E402

# table -> (file parquet, danh sách cột non-PK, conflict key)
TABLES: dict[str, tuple[str, list[str]]] = {
    "categories": ("dim_categories.parquet", ["name", "description"]),
    "products":   ("dim_products.parquet",   ["name", "category_id", "base_price",
                                              "stock", "is_active", "created_at", "updated_at"]),
    "users":      ("dim_users.parquet",      ["full_name", "email", "phone", "city", "created_at"]),
}

# thứ tự nạp quan trọng: products.category_id → categories(id)
LOAD_ORDER = ("categories", "products", "users")


def upsert_table(cur, table: str, columns: list[str], frame: pd.DataFrame) -> int:
    """UPSERT hàng loạt theo khoá chính `id`."""
    all_columns = ["id", *columns]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
    sql = (
        f"INSERT INTO {table} ({', '.join(all_columns)}) VALUES %s "
        f"ON CONFLICT (id) DO UPDATE SET {set_clause}"
    )
    rows = list(frame[all_columns].itertuples(index=False, name=None))
    execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def sync_sequences(cur) -> None:
    """Đẩy sequence SERIAL vượt id lớn nhất vừa nạp.

    Olist dùng dải id offset (categories 1.000+, products 100.000+,
    users 1.000.000+). Nếu không setval, INSERT tiếp theo sẽ cấp id 6, 7…
    và sẽ đụng id Olist sau một thời gian chạy mock.
    """
    for table in LOAD_ORDER:
        cur.execute(
            f"""SELECT setval(
                    pg_get_serial_sequence('{table}', 'id'),
                    GREATEST(COALESCE((SELECT max(id) FROM {table}), 1), 1)
                )"""
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Nạp dimension Olist vào PostgreSQL")
    parser.add_argument("--prepared", default="data/olist/prepared",
                        help="Thư mục chứa Parquet đã chuẩn hóa")
    args = parser.parse_args()

    prepared = (PROJECT_ROOT / args.prepared).resolve() \
        if not Path(args.prepared).is_absolute() else Path(args.prepared)

    config = db_config()
    print(f"[LoadDims] PostgreSQL {config['user']}@{config['host']}:{config['port']}/{config['dbname']}")
    print(f"[LoadDims] Nguồn      {prepared}")

    conn = connect(config)
    try:
        with conn.cursor() as cur:
            for table in LOAD_ORDER:
                filename, columns = TABLES[table]
                path = prepared / filename
                if not path.is_file():
                    raise SystemExit(f"Không tìm thấy {path} — chạy olist_prepare.py trước.")

                frame = pd.read_parquet(path)
                inserted = upsert_table(cur, table, columns, frame)
                print(f"  - {table:<11} {inserted:>8,} dòng  ←  {filename}")

            sync_sequences(cur)
            print("  - sequences    đã setval theo max(id)")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("\n[LoadDims] Xong. Bảng dim trong Postgres đã sẵn sàng cho dim_snapshot.py.")
    print("[LoadDims] Kiểm tra nhanh:")
    print("  docker exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \\")
    print("    -c \"SELECT (SELECT count(*) FROM categories) AS cats,")
    print("               (SELECT count(*) FROM products)   AS prods,")
    print("               (SELECT count(*) FROM users)      AS users;\"")


if __name__ == "__main__":
    main()
