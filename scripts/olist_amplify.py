#!/usr/bin/env python3
"""
============================================================================
 scripts/olist_amplify.py
 Nhân bản (amplify) tập đơn hàng Olist đã chuẩn hóa lên quy mô ~1 triệu đơn
============================================================================
 Input : data/olist/prepared/orders_resolved.parquet   (~99k đơn thật)
 Output: data/olist/orders_backfill.parquet            (~1,000,000 đơn)

 NGUYÊN TẮC NHÂN BẢN (giữ nguyên "hình dạng" thật của dữ liệu):
   - Mỗi đơn gốc sinh ra `factor` bản sao, cấp order_id mới liên tục:
         order_id = ORDER_ID_OFFSET + (rn - 1) * factor + copy_no
   - copy_no = 0  : bản gốc, giữ nguyên giá / số lượng / thời gian.
   - copy_no >= 1 : nhiễu nhỏ (giữ tổng = đơn giá × số lượng):
         quantity   += 0 hoặc 1 (15% xác suất)
         unit_price × U(0.90, 1.10)
         ordered_at/updated_at lùi ngẫu nhiên ≤ 1 giờ (cùng một độ lệch cho
         cả 2 cột để updated_at luôn ≥ ordered_at)
   - KHÔNG dịch trục thời gian ở bước này: giữ nguyên phân bố theo giờ/ngày
     của Olist để biểu đồ BI không bị "gai" nhân tạo.
     ⇒ Mật độ tăng đều ~factor lần trên toàn bộ 25 tháng.
   - Nếu yêu cầu `--target` không chia hết cho số đơn gốc, tập kết quả được
     lấy mẫu đều (ORDER BY hash(order_id) LIMIT target) để không cắt cụt
     một khoảng thời gian nào.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ORDER_ID_OFFSET = 100_000_000


def scalar(con: duckdb.DuckDBPyConnection, query: str):
    return con.execute(query).fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Amplify đơn hàng Olist lên quy mô lớn")
    parser.add_argument("--src", default="data/olist/prepared/orders_resolved.parquet")
    parser.add_argument("--out", default="data/olist/orders_backfill.parquet")
    parser.add_argument("--target", type=int, default=1_000_000,
                        help="Số đơn mong muốn (0 = chỉ dùng --factor, default: 1_000_000)")
    parser.add_argument("--factor", type=int, default=10,
                        help="Hệ số nhân bản khi không đặt --target (default: 10)")
    parser.add_argument("--base-limit", type=int, default=0,
                        help="Chỉ dùng ngẫu nhiên N đơn gốc (0 = tất cả) – để chạy thử")
    parser.add_argument("--seed", type=float, default=0.42, help="Seed cho random()")
    args = parser.parse_args()

    src = (PROJECT_ROOT / args.src).resolve() if not Path(args.src).is_absolute() else Path(args.src)
    out = (PROJECT_ROOT / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out)
    if not src.is_file():
        raise SystemExit(f"Không tìm thấy {src} — chạy olist_prepare.py trước.")

    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute(f"SELECT setseed({args.seed})")

    sample_clause = f"USING SAMPLE {args.base_limit} ROWS (reservoir, 42)" if args.base_limit else ""
    con.execute(f"""
        CREATE OR REPLACE TABLE base AS
        SELECT * FROM read_parquet('{src.as_posix()}')
        {sample_clause}
    """)

    n_base = scalar(con, "SELECT COUNT(*) FROM base")
    factor = max(1, math.ceil(args.target / n_base)) if args.target else max(1, args.factor)
    if args.base_limit and not args.target:
        factor = max(1, args.factor)
    print(f"[Amplify] Đơn gốc: {n_base:,} | factor: {factor} | dự kiến: {n_base * factor:,} dòng")

    con.execute(f"""
        CREATE OR REPLACE TABLE expanded AS
        SELECT b.*, k.copy_no
        FROM base b
        CROSS JOIN range(0, {factor}) AS k(copy_no)
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE amplified AS
        WITH numbered AS (
            SELECT CAST(row_number() OVER (ORDER BY ordered_at, order_id) AS BIGINT) AS rn, *
            FROM expanded
        ), jittered AS (
            SELECT *,
                   CASE WHEN copy_no = 0 THEN 0 ELSE CAST(random() * 3600 AS INTEGER) END AS jitter_secs,
                   CASE WHEN copy_no = 0 THEN 0
                        ELSE (CAST(random() * 11 AS INTEGER) - 5) END AS price_permille
            FROM numbered
        ), priced AS (
            SELECT
                CAST({ORDER_ID_OFFSET} + (rn - 1) * {factor} + copy_no AS BIGINT) AS order_id,
                CAST(user_id    AS INTEGER) AS user_id,
                CAST(product_id AS INTEGER) AS product_id,
                CAST(CASE WHEN copy_no = 0 THEN quantity ELSE quantity + (random() < 0.15)::INT END AS INTEGER) AS quantity,
                CAST(
                    CASE WHEN copy_no = 0 THEN unit_price
                         ELSE ROUND(unit_price * (1.0 + price_permille / 100.0), 2)
                    END AS DECIMAL(18, 2)
                ) AS unit_price,
                CAST(status AS VARCHAR) AS status,
                CAST(ordered_at - to_seconds(CAST(jitter_secs AS DOUBLE)) AS TIMESTAMP) AS ordered_at,
                CAST(updated_at - to_seconds(CAST(jitter_secs AS DOUBLE)) AS TIMESTAMP) AS updated_at,
                CAST(olist_order_id AS VARCHAR) AS olist_order_id
            FROM jittered
        )
        SELECT order_id, user_id, product_id, quantity, status, ordered_at, updated_at,
               unit_price,
               CAST(ROUND(unit_price * quantity, 2) AS DECIMAL(18, 2)) AS total_amount,
               olist_order_id
        FROM priced
    """)

    trimmed = False
    if args.target and n_base * factor > args.target:
        con.execute(f"""
            CREATE OR REPLACE TABLE final AS
            SELECT * FROM amplified ORDER BY hash(order_id) LIMIT {args.target}
        """)
        trimmed = True
    else:
        con.execute("CREATE OR REPLACE TABLE final AS SELECT * FROM amplified")

    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY final TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    rows, min_ts, max_ts, revenue, dup = con.execute("""
        SELECT COUNT(*), MIN(ordered_at), MAX(ordered_at), SUM(total_amount),
               COUNT(*) - COUNT(DISTINCT order_id)
        FROM final
    """).fetchone()

    print(f"\n[Amplify] Đã ghi: {out}")
    print(f"  - Số đơn           {rows:>12,}{'  (đã lấy mẫu đều về đúng --target)' if trimmed else ''}")
    print(f"  - order_id trùng   {dup:>12,}")
    print(f"  - Khoảng thời gian {min_ts} → {max_ts}")
    print(f"  - Doanh thu tổng   {float(revenue):>15,.0f} VND")
    print("\n[Amplify] Xong. Bước tiếp theo: spark_jobs/backfill_olist.py")


if __name__ == "__main__":
    main()
