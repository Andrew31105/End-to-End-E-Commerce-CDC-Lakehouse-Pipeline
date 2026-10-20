"""CLI của mock traffic generator."""
from __future__ import annotations

import argparse
import random
from pathlib import Path

from .generator import TrafficGenerator
from .orders import FALLBACK_RATES, build_transitions
from .sources import (
    DIM_FILES,
    ORDERS_FILE,
    load_dimensions,
    load_orders,
)
from .state import DbState

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ORDERS = f"data/olist/prepared/{ORDERS_FILE}"
DEFAULT_DIMS = "data/olist/prepared"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="E-Commerce Mock Traffic Generator (orders + dimension CDC)"
    )
    parser.add_argument(
        "--source", choices=["olist", "synthetic"], default="olist",
        help="olist: sinh đơn từ dataset Olist (mặc định) | synthetic: random từ Postgres",
    )
    parser.add_argument(
        "--olist-orders", default=DEFAULT_ORDERS,
        help="Parquet đơn Olist dùng làm nguồn mẫu",
    )
    parser.add_argument(
        "--olist-dims", default=DEFAULT_DIMS,
        help=f"Thư mục chứa {', '.join(DIM_FILES.values())}",
    )
    parser.add_argument(
        "--dim-source", choices=["olist", "synthetic"], default=None,
        help="Nguồn giá trị bảng chiều (mặc định giống --source)",
    )
    parser.add_argument(
        "--rate", type=float, default=0.0,
        help="Số event mỗi giây, ví dụ 20 (ưu tiên hơn --interval)",
    )
    parser.add_argument(
        "--interval", type=float, default=2.0,
        help="Giây nghỉ giữa 2 event khi không dùng --rate (default: 2.0)",
    )
    parser.add_argument(
        "--update-ratio", type=float, default=0.3,
        help="Tỉ lệ UPDATE so với INSERT (0.0-1.0, default: 0.3)",
    )
    parser.add_argument(
        "--duration", type=float, default=0.0,
        help="Tự dừng sau N giây (0 = chạy tới khi Ctrl+C)",
    )
    parser.add_argument(
        "--dim-ratio", type=float, default=0.2,
        help="Xác suất mỗi vòng lặp sinh thêm 1 thay đổi bảng chiều (0 = tắt)",
    )
    parser.add_argument(
        "--stock-updates", type=float, default=0.0,
        help="Xác suất mỗi vòng lặp chỉ cập nhật products.stock (Type 1, default: 0)",
    )
    parser.add_argument(
        "--seed", type=float, default=None,
        help="Seed cho random – dùng khi cần tái lập kịch bản test",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="In log chi tiết từng event (mặc định chỉ in thống kê mỗi 5 giây)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    # Import muộn: --help không cần psycopg2 / kết nối DB
    from scripts.db import connect, db_config

    orders = load_orders(args.source, args.olist_orders, PROJECT_ROOT)
    values = load_dimensions(
        args.dim_source or args.source, args.olist_dims, PROJECT_ROOT
    )

    conn = connect(db_config())
    try:
        cur = conn.cursor()
        state = DbState.load(cur, values.category_ids)
        rates = (orders.cancel_rate, orders.return_rate) if orders else FALLBACK_RATES
        generator = TrafficGenerator(
            conn, cur, state, orders, values, build_transitions(*rates),
            verbose=args.verbose,
            dim_ratio=args.dim_ratio,
            stock_ratio=args.stock_updates,
        )
        generator.run(
            update_ratio=args.update_ratio,
            rate=args.rate,
            interval=args.interval,
            duration=args.duration,
        )
    finally:
        conn.close()
