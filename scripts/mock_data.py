#!/usr/bin/env python3
"""
mock_data.py – Real-Time E-Commerce Data Generator
===================================================
Giả lập traffic thật: liên tục INSERT đơn hàng mới và UPDATE trạng thái
đơn hàng cũ trong PostgreSQL để Debezium bắt CDC events.

Mặc định mỗi đơn được SINH TỪ DATASET OLIST: lấy mẫu 1 đơn có thật trong
data/olist/prepared/orders_resolved.parquet (giữ nguyên user_id, product_id,
quantity, unit_price) rồi phát lại với thời điểm hiện tại — nhờ vậy phân bố
theo sản phẩm/thành phố/giá của traffic realtime khớp dữ liệu lịch sử.
Không có file đó thì tự chuyển sang chế độ tổng hợp (random từ Postgres).

Usage:
    python scripts/mock_data.py --rate 20 --update-ratio 0.3        # 20 event/giây, nguồn Olist
    python scripts/mock_data.py --rate 20 --duration 60             # chạy 60s rồi tự dừng
    python scripts/mock_data.py --source synthetic --rate 5         # chế độ tổng hợp
    python scripts/mock_data.py --interval 2.0 --update-ratio 0.3   # chế độ cũ

Cấu hình kết nối DB qua biến môi trường (ưu tiên POSTGRES_*, fallback PG_*):
    PG_HOST, PG_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.db import db_config, load_env_file

# ============================================================
#  Configuration
# ============================================================

load_env_file()

FINAL_STATUSES = {"delivered", "cancelled", "returned"}


def build_status_transitions(cancel_rate: float, return_rate: float
                             ) -> dict[str, list[tuple[str, float]]]:
    """Bảng chuyển trạng thái CÓ TRỌNG SỐ, tỉ lệ lấy từ dữ liệu Olist.

    Olist thật: ~99% đơn đi tới 'delivered', huỷ/trả rất hiếm. Bảng cũ cho
    'pending → cancelled' = 50% nên dashboard bị nhiễu bởi đơn huỷ.
    'delivered' là trạng thái cuối (Olist không có bước sau khi giao).
    """
    return {
        "pending":   [("confirmed", 1.0 - cancel_rate), ("cancelled", cancel_rate)],
        "confirmed": [("shipped",   1.0 - cancel_rate), ("cancelled", cancel_rate)],
        "shipped":   [("delivered", 1.0 - return_rate),  ("returned",  return_rate)],
        "delivered": [],
        "cancelled": [],
        "returned":  [],
    }


def pick_next_status(options: list[tuple[str, float]]) -> str:
    """Chọn trạng thái kế tiếp theo trọng số."""
    roll = random.random() * sum(weight for _, weight in options)
    for status, weight in options:
        roll -= weight
        if roll <= 0:
            return status
    return options[-1][0]

# ============================================================
#  Helper
# ============================================================

def get_conn():
    from scripts.db import connect

    return connect(db_config())


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}")


# ============================================================
#  Nguồn đơn hàng Olist
# ============================================================

OLIST_ORDERS_DEFAULT = "data/olist/prepared/orders_resolved.parquet"

# Tỉ lệ huỷ/trả dùng khi không đọc được dataset (Olist thật: ~0,6% / ~0,6%)
OLIST_FALLBACK_RATES = (0.006, 0.006)


class OlistSampler:
    """Lấy mẫu đơn hàng từ chính dataset Olist đã chuẩn hóa.

    Mỗi event live = 1 đơn Olist thật (giữ nguyên user_id / product_id /
    quantity / unit_price), chỉ khác thời điểm đặt và vòng đời trạng thái.
    """

    def __init__(self, path: Path) -> None:
        import duckdb

        con = duckdb.connect()
        con.execute("PRAGMA threads = 2")
        self.orders: list[tuple[int, int, int, float]] = con.execute(f"""
            SELECT user_id, product_id, quantity, CAST(unit_price AS DOUBLE)
            FROM read_parquet('{path.as_posix()}')
        """).fetchall()
        cancel_rate, return_rate = con.execute(f"""
            SELECT AVG(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END),
                   AVG(CASE WHEN status = 'returned'  THEN 1 ELSE 0 END)
            FROM read_parquet('{path.as_posix()}')
        """).fetchone()
        con.close()

        if not self.orders:
            raise ValueError(f"{path}: không có đơn nào")

        self.cancel_rate = float(cancel_rate or 0.0)
        self.return_rate = float(return_rate or 0.0)
        log(f" Nguồn Olist: {len(self.orders):,} đơn mẫu từ {path.name} "
            f"| huỷ {self.cancel_rate:.2%} | trả {self.return_rate:.2%}")

    def sample(self) -> tuple[int, int, int, float]:
        return random.choice(self.orders)


# ============================================================
#  Generator Logic
# ============================================================

class TrafficGenerator:
    def __init__(self, conn, verbose: bool = False, source: str = "olist",
                 olist_orders: str = OLIST_ORDERS_DEFAULT,
                 pool_ttl: float = 15.0, pool_max: int = 50_000) -> None:
        self.conn = conn
        self.cur = conn.cursor()
        self.verbose = verbose
        self.product_ids: list[int] = []
        self.user_ids: list[int] = []
        # Pool các đơn còn chuyển được trạng thái (id -> status) giữ trong RAM.
        # Nhờ vậy ở rate cao KHÔNG phải SELECT bảng orders ở mỗi event.
        self.pool: dict[int, str] = {}
        self.pool_ttl = pool_ttl
        self.pool_max = pool_max
        self._pool_refresh_at = 0.0

        self.sampler = self._load_olist_sampler(source, olist_orders)
        if self.sampler is None:
            self._load_reference_data()
            self.transitions = build_status_transitions(*OLIST_FALLBACK_RATES)
        else:
            self.transitions = build_status_transitions(
                self.sampler.cancel_rate, self.sampler.return_rate
            )

    @staticmethod
    def _load_olist_sampler(source: str, path_str: str) -> "OlistSampler | None":
        if source != "olist":
            log(" Chế độ synthetic: chọn sản phẩm/user ngẫu nhiên từ Postgres")
            return None

        path = Path(path_str)
        if not path.is_absolute():
            path = Path(PROJECT_ROOT) / path
        if not path.is_file():
            log(f" ! Không thấy {path} → dùng chế độ synthetic "
                "(chạy scripts/olist_prepare.py để có nguồn Olist)")
            return None
        try:
            return OlistSampler(path)
        except ImportError:
            log(" ! Chưa cài duckdb → dùng chế độ synthetic")
            return None

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

    def _event_log(self, msg: str) -> None:
        if self.verbose:
            log(msg)

    # ---------------- pool các đơn có thể UPDATE ----------------
    def _refresh_pool(self) -> None:
        """Nạp lại danh sách đơn chưa ở trạng thái cuối (có cache theo TTL).

        Nếu SELECT lại mỗi event thì ở 20 event/s ta sẽ full-scan bảng orders
        20 lần/giây — cache giúp rate cao vẫn ổn định.
        """
        now = time.monotonic()
        if now < self._pool_refresh_at:
            return
        self.cur.execute(
            """SELECT id, status FROM orders
               WHERE status NOT IN ('delivered', 'cancelled', 'returned')
               ORDER BY id DESC
               LIMIT %s""",
            (self.pool_max,),
        )
        self.pool = {row[0]: row[1] for row in self.cur.fetchall()}
        self._pool_refresh_at = now + self.pool_ttl

    def _pick_updatable(self) -> int | None:
        self._refresh_pool()
        return random.choice(list(self.pool)) if self.pool else None

    def insert_order(self) -> bool:
        """Tạo một đơn hàng mới với trạng thái 'pending'.

        Nguồn: lấy mẫu 1 đơn Olist thật (user/product/quantity/price) nếu có,
        không thì sinh tổng hợp từ products/users trong Postgres.
        """
        if self.sampler is not None:
            user_id, product_id, quantity, unit_price = self.sampler.sample()
        else:
            user_id = random.choice(self.user_ids)
            product_id, base_price = self._pick_product()
            quantity = random.randint(1, 3)
            unit_price = round(base_price * random.uniform(0.85, 1.15), -3)
        total = round(unit_price * quantity, 2)

        self.cur.execute(
            """INSERT INTO orders (user_id, product_id, quantity, unit_price, total_amount, status)
               VALUES (%s, %s, %s, %s, %s, 'pending')
               RETURNING id""",
            (user_id, product_id, quantity, unit_price, total),
        )
        order_id = self.cur.fetchone()[0]
        self.conn.commit()
        self.pool[order_id] = "pending"
        self._event_log(f" INSERT order_id={order_id} | user={user_id} product={product_id} "
                        f"qty={quantity} total={total:,.0f}₫")
        return True

    def update_order_status(self, order_id: int) -> bool:
        """Chuyển trạng thái đơn hàng theo transition hợp lệ.

        UPDATE kèm điều kiện `AND status = <trạng thái đã biết>` (optimistic
        lock) nên KHÔNG cần SELECT ... FOR UPDATE ⇒ bớt 1 round-trip mỗi event.
        """
        current_status = self.pool.get(order_id)
        if current_status is None:
            self.cur.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
            row = self.cur.fetchone()
            if not row:
                self.pool.pop(order_id, None)
                return False
            current_status = row[0]

        options = self.transitions.get(current_status, [])
        if not options:
            self.pool.pop(order_id, None)
            return False

        new_status = pick_next_status(options)
        self.cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s AND status = %s",
            (new_status, order_id, current_status),
        )
        updated = self.cur.rowcount
        self.conn.commit()

        if not updated:
            # Trạng thái đã bị nguồn khác đổi ⇒ bỏ khỏi pool, lần sau nạp lại
            self.pool.pop(order_id, None)
            return False

        if new_status in FINAL_STATUSES:
            self.pool.pop(order_id, None)
        else:
            self.pool[order_id] = new_status
        self._event_log(f" UPDATE order_id={order_id}: {current_status} → {new_status}")
        return True

    def run(self, update_ratio: float, rate: float = 0.0, interval: float = 2.0,
            duration: float = 0.0) -> None:
        """Vòng lặp chính.

        - rate > 0     : bắn đúng `rate` event/giây (có bù trừ thời gian xử lý)
        - rate = 0     : nghỉ `interval` giây giữa 2 event (chế độ cũ)
        - update_ratio : tỉ lệ UPDATE so với INSERT (0.0 – 1.0)
        - duration > 0 : tự dừng sau `duration` giây
        """
        target_dt = 1.0 / rate if rate > 0 else max(interval, 0.001)
        mode = f"rate {rate:g} event/s" if rate > 0 else f"interval {interval}s"
        log(f" Traffic Generator started | {mode} | update_ratio {update_ratio} "
            f"| Ctrl+C để dừng")

        started = time.monotonic()
        next_report = started + 5.0
        events = inserts = updates = 0
        try:
            while True:
                cycle_start = time.monotonic()

                if random.random() < update_ratio:
                    order_id = self._pick_updatable()
                    if order_id is not None and self.update_order_status(order_id):
                        updates += 1
                    elif self.insert_order():   # pool rỗng ⇒ tạo đơn mới
                        inserts += 1
                else:
                    if self.insert_order():
                        inserts += 1
                events += 1

                now = time.monotonic()
                if now >= next_report:
                    elapsed = now - started
                    log(f"  {events} event | {events / elapsed:.1f} event/s "
                        f"| INSERT {inserts} / UPDATE {updates} | pool {len(self.pool)}")
                    next_report = now + 5.0

                if duration and now - started >= duration:
                    break

                # Bù trừ: trừ thời gian xử lý để giữ đúng rate
                sleep_for = target_dt - (time.monotonic() - cycle_start)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except KeyboardInterrupt:
            log(" Stopped by user.")
        finally:
            elapsed = time.monotonic() - started
            if elapsed > 0:
                log(f" Tổng kết: {events} event trong {elapsed:.1f}s "
                    f"≈ {events / elapsed:.1f} event/s | INSERT {inserts} / UPDATE {updates}")
            self.cur.close()


# ============================================================
#  Entry Point
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="E-Commerce Mock Traffic Generator")
    parser.add_argument(
        "--source", choices=["olist", "synthetic"], default="olist",
        help="olist: sinh đơn từ dataset Olist (mặc định) | synthetic: random từ Postgres",
    )
    parser.add_argument(
        "--olist-orders", default=OLIST_ORDERS_DEFAULT,
        help="Parquet đơn Olist dùng làm nguồn mẫu",
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
        help="Probability of UPDATE vs INSERT (0.0-1.0, default: 0.3)",
    )
    parser.add_argument(
        "--duration", type=float, default=0.0,
        help="Tự dừng sau N giây (0 = chạy tới khi Ctrl+C)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="In log chi tiết từng event (mặc định chỉ in thống kê mỗi 5 giây)",
    )
    args = parser.parse_args()

    conn = get_conn()
    try:
        gen = TrafficGenerator(conn, verbose=args.verbose,
                               source=args.source, olist_orders=args.olist_orders)
        gen.run(
            update_ratio=args.update_ratio,
            rate=args.rate,
            interval=args.interval,
            duration=args.duration,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
