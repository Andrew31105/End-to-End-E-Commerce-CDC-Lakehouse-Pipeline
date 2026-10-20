#!/usr/bin/env python3
"""Entry point mỏng cho mock traffic generator.

Cài đặt thật nằm trong package ``scripts/mockgen`` (mỗi module một trách nhiệm):

    sources.py     – đọc Olist (orders + dims) / pool synthetic
    state.py       – cache bảng chiều trong PostgreSQL
    orders.py      – sinh INSERT/UPDATE orders
    dimensions.py  – sinh thay đổi users/products/categories (nguồn CDC cho SCD2)
    generator.py   – vòng lặp traffic
    cli.py         – argparse

Chạy:
    python scripts/mock_data.py --rate 20 --update-ratio 0.3
    python scripts/mock_data.py --rate 20 --dim-ratio 0.3
"""
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.mockgen.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
