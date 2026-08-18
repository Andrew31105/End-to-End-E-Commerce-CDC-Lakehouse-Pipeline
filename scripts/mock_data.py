#!/usr/bin/env python3
"""
mock_data.py – Real-Time E-Commerce Data Generator
===================================================
Giả lập traffic thật: liên tục INSERT đơn hàng mới và UPDATE trạng thái
đơn hàng cũ trong PostgreSQL để Debezium bắt CDC events.

Usage:
    python scripts/mock_data.py --interval 2.0 --update-ratio 0.3

Cấu hình kết nối DB qua biến môi trường (ưu tiên POSTGRES_*, fallback PG_*):
    PG_HOST, PG_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
from faker import Faker

# ============================================================
#  Nạp file .env (để chạy trực tiếp mà không cần export biến)
# ============================================================

def _load_env_file() -> None:
    """Nạp các biến từ .env vào os.environ (không ghi đè biến đã set).

    Tự động tìm .env theo thứ tự:
      1. Thư mục hiện tại (./.env)
      2. Thư mục gốc dự án (cạnh script: ../.env)
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [os.path.join(os.getcwd(), ".env"),
                  os.path.join(script_dir, "..", ".env")]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return  # đã nạp file đầu tiên tìm thấy


_load_env_file()

# ============================================================
#  Configuration
# ============================================================

fake = Faker(["vi_VN"])

# Ưu tiên POSTGRES_*, fallback PG_* (đọc từ .env đã nạp bên trên)
DB_CONFIG: dict[str, Any] = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB", os.getenv("PG_DB") or "ecommerce"),
    "user":     os.getenv("POSTGRES_USER", os.getenv("PG_USER")),
    "password": os.getenv("POSTGRES_PASSWORD", os.getenv("PG_PASSWORD")),
}

if not DB_CONFIG["user"] or not DB_CONFIG["password"]:
    raise SystemExit(
        "Thiếu thông tin đăng nhập PostgreSQL. "
        "Hãy tạo file .env từ .env.example rồi chạy lại."
    )

STATUS_TRANSITIONS: dict[str, list[str]] = {
    "pending":    ["confirmed", "cancelled"],
    "confirmed":  ["shipped", "cancelled"],
    "shipped":    ["delivered", "returned"],
    "delivered":  ["returned"],
    "cancelled":  [],
    "returned":   [],
}

# ============================================================
#  Helper
# ============================================================

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}")


# ============================================================
#  Generator Logic
# ============================================================

class TrafficGenerator:
    def __init__(self, conn) -> None:
        self.conn = conn
        self.cur = conn.cursor()
        self.product_ids: list[int] = []
        self.user_ids: list[int] = []
        self.order_ids: list[int] = []
        self._load_reference_data()

    def _load_reference_data(self) -> None:
        """Load danh sách products và users hiện có trong DB."""
        self.cur.execute("SELECT id, base_price FROM products WHERE is_active = TRUE")
        rows = self.cur.fetchall()
        self.product_ids = [r[0] for r in rows]
        self.product_prices = {r[0]: float(r[1]) for r in rows}

        self.cur.execute("SELECT id FROM users")
        self.user_ids = [r[0] for r in self.cur.fetchall()]

        log(f"Loaded {len(self.product_ids)} products, {len(self.user_ids)} users")

    def _pick_product(self) -> tuple[int, float]:
        pid = random.choice(self.product_ids)
        return pid, self.product_prices[pid]

    def insert_order(self) -> int:
        """Tạo một đơn hàng mới với trạng thái 'pending'."""
        user_id = random.choice(self.user_ids)
        product_id, base_price = self._pick_product()
        quantity = random.randint(1, 3)
        unit_price = round(base_price * random.uniform(0.85, 1.15), -3)  # làm tròn đến 1000
        total = unit_price * quantity

        self.cur.execute(
            """INSERT INTO orders (user_id, product_id, quantity, unit_price, total_amount, status)
               VALUES (%s, %s, %s, %s, %s, 'pending')
               RETURNING id""",
            (user_id, product_id, quantity, unit_price, total),
        )
        order_id = self.cur.fetchone()[0]
        self.conn.commit()
        log(f" INSERT order_id={order_id} | user={user_id} product={product_id} "
            f"qty={quantity} total={total:,.0f}₫")
        return order_id

    def update_order_status(self, order_id: int) -> None:
        """Chuyển trạng thái đơn hàng theo transition hợp lệ."""
        self.cur.execute(
            "SELECT status FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        row = self.cur.fetchone()
        if not row:
            return  # order đã bị xóa

        current_status: str = row[0]
        next_statuses = STATUS_TRANSITIONS.get(current_status, [])
        if not next_statuses:
            return  # trạng thái cuối, không chuyển nữa

        new_status = random.choice(next_statuses)
        self.cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, order_id),
        )
        self.conn.commit()
        log(f" UPDATE order_id={order_id}: {current_status} → {new_status}")

    def get_updatable_orders(self) -> list[int]:
        """Lấy danh sách order_id có thể chuyển trạng thái."""
        self.cur.execute(
            "SELECT id FROM orders WHERE status NOT IN ('delivered','cancelled','returned')"
        )
        return [r[0] for r in self.cur.fetchall()]

    def run(self, interval: float, update_ratio: float) -> None:
        """
        Vòng lặp chính:
          - interval: giây nghỉ giữa các thao tác
          - update_ratio: tỉ lệ UPDATE so với INSERT (0.0 – 1.0)
        """
        log(" Traffic Generator started. Press Ctrl+C to stop.")
        try:
            while True:
                if random.random() < update_ratio:
                    candidates = self.get_updatable_orders()
                    if candidates:
                        self.update_order_status(random.choice(candidates))
                    else:
                        # fallback: tạo đơn mới
                        oid = self.insert_order()
                        if oid:
                            self.order_ids.append(oid)
                else:
                    oid = self.insert_order()
                    if oid:
                        self.order_ids.append(oid)

                # Giới hạn danh sách order_ids để tránh rò rỉ memory
                if len(self.order_ids) > 10_000:
                    self.order_ids = self.order_ids[-5_000:]

                time.sleep(interval)
        except KeyboardInterrupt:
            log(" Stopped by user.")
        finally:
            self.cur.close()


# ============================================================
#  Entry Point
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="E-Commerce Mock Traffic Generator")
    parser.add_argument(
        "--interval", type=float, default=2.0,
        help="Seconds to wait between operations (default: 2.0)",
    )
    parser.add_argument(
        "--update-ratio", type=float, default=0.3,
        help="Probability of UPDATE vs INSERT (0.0-1.0, default: 0.3)",
    )
    args = parser.parse_args()

    conn = get_conn()
    try:
        gen = TrafficGenerator(conn)
        gen.run(interval=args.interval, update_ratio=args.update_ratio)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
