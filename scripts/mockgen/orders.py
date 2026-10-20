"""Sinh INSERT/UPDATE đơn hàng trong PostgreSQL."""
from __future__ import annotations

import random
import time

from .sources import OlistOrders
from .state import DbState
from .util import log, weighted_choice

FINAL_STATUSES = {"delivered", "cancelled", "returned"}

# Tỉ lệ huỷ/trả khi không đọc được Olist (Olist thật: ~0,6% / ~0,6%)
FALLBACK_RATES = (0.006, 0.006)


def build_transitions(
    cancel_rate: float, return_rate: float
) -> dict[str, list[tuple[str, float]]]:
    """Bảng chuyển trạng thái có trọng số, tỉ lệ lấy từ dữ liệu Olist.

    Olist thật: ~99% đơn tới ``delivered``; huỷ/trả rất hiếm. Nếu dùng tỉ lệ
    50% như bản cũ, dashboard sẽ bị nhiễu bởi đơn huỷ.
    """
    return {
        "pending": [("confirmed", 1.0 - cancel_rate), ("cancelled", cancel_rate)],
        "confirmed": [("shipped", 1.0 - cancel_rate), ("cancelled", cancel_rate)],
        "shipped": [("delivered", 1.0 - return_rate), ("returned", return_rate)],
        "delivered": [],
        "cancelled": [],
        "returned": [],
    }


class OrderTraffic:
    """Tạo traffic đơn hàng; giữ pool id còn đổi trạng thái được trong RAM.

    Pool có TTL để không phải SELECT bảng ``orders`` ở mỗi event khi chạy
    rate cao. Id được lưu trong list và xoá lazy (swap-remove) để tránh tạo
    list mới mỗi lần chọn.
    """

    def __init__(
        self,
        conn,
        cur,
        state: DbState,
        olist_orders: OlistOrders | None,
        transitions: dict[str, list[tuple[str, float]]],
        *,
        verbose: bool = False,
        pool_ttl: float = 15.0,
        pool_max: int = 50_000,
    ) -> None:
        self.conn = conn
        self.cur = cur
        self.state = state
        self.olist_orders = olist_orders
        self.transitions = transitions
        self.verbose = verbose
        self.pool_ttl = pool_ttl
        self.pool_max = pool_max
        self._pool: dict[int, str] = {}
        self._pool_ids: list[int] = []
        self._pool_refresh_at = 0.0

    @property
    def pool_size(self) -> int:
        return len(self._pool)

    # ------------------------------------------------------------------
    # Vòng lặp
    # ------------------------------------------------------------------
    def step(self, update_ratio: float) -> str:
        """Thực hiện 1 event; trả về ``insert`` / ``update`` / ``noop``."""
        if random.random() < update_ratio:
            order_id = self._pick_updatable()
            if order_id is not None and self.update_status(order_id):
                return "update"
        return "insert" if self.insert() else "noop"

    # ------------------------------------------------------------------
    # Ghi DB
    # ------------------------------------------------------------------
    def insert(self) -> bool:
        """Tạo đơn mới ở trạng thái ``pending``."""
        if self.olist_orders is not None:
            user_id, product_id, quantity, unit_price = self.olist_orders.sample()
        else:
            if not (self.state.user_ids and self.state.active_product_ids):
                raise RuntimeError(
                    "Không có user/product active – hãy nạp dim Olist hoặc seed trước"
                )
            user_id = random.choice(self.state.user_ids)
            product_id = random.choice(self.state.active_product_ids)
            quantity = random.randint(1, 3)
            base_price = self.state.products[product_id].base_price
            unit_price = round(base_price * random.uniform(0.85, 1.15), -3)

        total = round(unit_price * quantity, 2)
        self.cur.execute(
            """INSERT INTO orders
                   (user_id, product_id, quantity, unit_price, total_amount, status)
               VALUES (%s, %s, %s, %s, %s, 'pending')
               RETURNING id""",
            (user_id, product_id, quantity, unit_price, total),
        )
        order_id = self.cur.fetchone()[0]
        self.conn.commit()

        self._pool[order_id] = "pending"
        self._pool_ids.append(order_id)
        self._event(
            f" INSERT order_id={order_id} | user={user_id} product={product_id} "
            f"qty={quantity} total={total:,.0f}₫"
        )
        return True

    def update_status(self, order_id: int) -> bool:
        """Chuyển trạng thái theo transition hợp lệ.

        Dùng optimistic lock (``AND status = <trạng thái đã biết>``) nên không
        cần ``SELECT ... FOR UPDATE`` → bớt 1 round-trip mỗi event.
        """
        current = self._pool.get(order_id)
        if current is None:
            self.cur.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
            row = self.cur.fetchone()
            if row is None:
                self._drop(order_id)
                return False
            current = row[0]

        options = self.transitions.get(current, [])
        if not options:
            self._drop(order_id)
            return False

        new_status = weighted_choice(options)
        self.cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() "
            "WHERE id = %s AND status = %s",
            (new_status, order_id, current),
        )
        self.conn.commit()

        if self.cur.rowcount == 0:
            # Trạng thái đã bị nguồn khác đổi → nạp lại pool ở vòng sau
            self._drop(order_id)
            return False

        if new_status in FINAL_STATUSES:
            self._drop(order_id)
        else:
            self._pool[order_id] = new_status
        self._event(f" UPDATE order_id={order_id}: {current} → {new_status}")
        return True

    # ------------------------------------------------------------------
    # Pool nội bộ
    # ------------------------------------------------------------------
    def _refresh_pool(self) -> None:
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
        self._pool = dict(self.cur.fetchall())
        self._pool_ids = list(self._pool)
        self._pool_refresh_at = now + self.pool_ttl

    def _pick_updatable(self) -> int | None:
        self._refresh_pool()
        while self._pool_ids:
            index = random.randrange(len(self._pool_ids))
            order_id = self._pool_ids[index]
            if order_id in self._pool:
                return order_id
            # Dọn lazy: hoán đổi với phần tử cuối rồi pop → O(1)
            self._pool_ids[index] = self._pool_ids[-1]
            self._pool_ids.pop()
        return None

    def _drop(self, order_id: int) -> None:
        self._pool.pop(order_id, None)

    def _event(self, message: str) -> None:
        if self.verbose:
            log(message)
