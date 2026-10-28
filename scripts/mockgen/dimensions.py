"""Sinh thay đổi trên bảng chiều (users / products / categories).

Đây là nguồn CDC để tầng Silver dựng SCD Type 2. Chỉ các cột **chậm thay đổi**
mới được sinh ở đây; ``stock`` tách riêng vì là cột biến động nhanh (Type 1).
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from .sources import OlistDimensions, SyntheticDimensions
from .state import DbState, Product
from .util import log, weighted_choice

DimensionValues = OlistDimensions | SyntheticDimensions

# (operation, trọng số)
DIM_OPERATIONS = (
    ("update_product", 0.34),   # base_price / category_id / is_active / name
    ("update_user", 0.24),      # city / full_name
    ("insert_product", 0.18),
    ("update_category", 0.12),  # description
    ("insert_user", 0.10),
    ("insert_category", 0.02),
)


class DimensionMutator:
    def __init__(
        self,
        conn,
        cur,
        state: DbState,
        values: DimensionValues,
        *,
        verbose: bool = False,
    ) -> None:
        self.conn = conn
        self.cur = cur
        self.state = state
        self.values = values
        self.verbose = verbose
        self._handlers = {
            "update_product": self._update_product,
            "update_user": self._update_user,
            "insert_product": self._insert_product,
            "update_category": self._update_category,
            "insert_user": self._insert_user,
            "insert_category": self._insert_category,
            "update_stock": self._update_stock,
        }

    # ------------------------------------------------------------------
    # API công khai
    # ------------------------------------------------------------------
    def mutate(self) -> str | None:
        """Sinh đúng một thay đổi chiều; trả mô tả hoặc ``None``."""
        return self._run(weighted_choice(DIM_OPERATIONS))

    def mutate_stock(self) -> bool:
        """Mô phỏng biến động tồn kho (tuỳ chọn, mặc định tắt)."""
        return self._run("update_stock") is not None

    # ------------------------------------------------------------------
    # Hạ tầng
    # ------------------------------------------------------------------
    def _run(self, operation: str) -> str | None:
        """Chạy handler + commit; lỗi DB không được làm dừng traffic."""
        try:
            message = self._handlers[operation]()
            if message is None:
                self.conn.rollback()
                return None
            self.conn.commit()
        except Exception as exc:
            self.conn.rollback()
            log(f" ! Lỗi sinh dimension ({operation}): {exc}")
            return None

        if self.verbose:
            log(f" DIM {message}")
        return message

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------
    def _insert_product(self) -> str | None:
        if not self.state.categories:
            return None

        category_id = self.state.pick_category_id()
        name = self.values.product_name()
        base_price = self.values.price(category_id)
        stock = random.randint(0, 100)
        self.cur.execute(
            """INSERT INTO products (name, category_id, base_price, stock)
               VALUES (%s, %s, %s, %s)
               RETURNING id""",
            (name, category_id, base_price, stock),
        )
        product_id = self.cur.fetchone()[0]
        self.state.add_product(
            product_id, Product(name, base_price, category_id, is_active=True)
        )
        return f"INSERT product={product_id} price={base_price:,.0f}₫"

    def _update_product(self) -> str | None:
        """Đổi đúng MỘT cột tracked (chậm thay đổi) để test SCD2."""
        if not self.state.product_ids:
            return None

        product_id = self.state.pick_product()
        product = self.state.products[product_id]
        roll = random.random()

        if roll < 0.40:                       # base_price
            new_price = round(
                max(1.0, product.base_price * random.uniform(0.85, 1.15)), 2
            )
            self.cur.execute(
                "UPDATE products SET base_price = %s, updated_at = NOW() WHERE id = %s",
                (new_price, product_id),
            )
            product.base_price = new_price
            detail = f"base_price → {new_price:,.0f}₫"

        elif roll < 0.65:                     # category_id
            category_id = self.state.pick_category_id()
            self.cur.execute(
                "UPDATE products SET category_id = %s, updated_at = NOW() WHERE id = %s",
                (category_id, product_id),
            )
            product.category_id = category_id
            detail = f"category_id → {category_id}"

        elif roll < 0.85:                     # is_active
            is_active = not product.is_active
            self.cur.execute(
                "UPDATE products SET is_active = %s, updated_at = NOW() WHERE id = %s",
                (is_active, product_id),
            )
            self.state.set_active(product_id, is_active)
            detail = f"is_active → {is_active}"

        else:                                 # name
            base = product.name.split(" #")[0][:230]
            new_name = f"{base} #{random.randint(1, 99)}"
            self.cur.execute(
                "UPDATE products SET name = %s, updated_at = NOW() WHERE id = %s",
                (new_name, product_id),
            )
            product.name = new_name
            detail = f"name → {new_name}"

        return f"UPDATE product={product_id} {detail}"

    def _update_stock(self) -> str | None:
        """Chỉ đổi ``stock`` – cột biến động nhanh (Type 1), không thuộc SCD2."""
        if not self.state.product_ids:
            return None

        product_id = self.state.pick_product()
        delta = random.randint(1, 3)
        self.cur.execute(
            """UPDATE products
                  SET stock = GREATEST(stock - %s, 0), updated_at = NOW()
                WHERE id = %s""",
            (delta, product_id),
        )
        return f"UPDATE product={product_id} stock -{delta}"

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------
    def _insert_user(self) -> str | None:
        city = self.values.city()
        email = f"mock{uuid.uuid4().hex[:10]}@example.com"
        phone = f"09{random.randint(10_000_000, 99_999_999)}"
        self.cur.execute(
            """INSERT INTO users (full_name, email, phone, city)
               VALUES (%s, %s, %s, %s)
               RETURNING id""",
            (self.values.user_name(), email, phone, city),
        )
        user_id = self.cur.fetchone()[0]
        self.state.add_user(user_id, city)
        return f"INSERT user={user_id} city={city}"

    def _update_user(self) -> str | None:
        if not self.state.user_ids:
            return None

        user_id = random.choice(self.state.user_ids)
        if random.random() < 0.7:
            new_city = self.values.city(exclude=self.state.users[user_id])
            self.cur.execute(
                "UPDATE users SET city = %s, updated_at = NOW() WHERE id = %s",
                (new_city, user_id),
            )
            self.state.users[user_id] = new_city
            return f"UPDATE user={user_id} city → {new_city}"

        new_name = self.values.user_name()
        self.cur.execute(
            "UPDATE users SET full_name = %s, updated_at = NOW() WHERE id = %s",
            (new_name, user_id),
        )
        return f"UPDATE user={user_id} full_name → {new_name}"

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------
    def _insert_category(self) -> str | None:
        raw_name, description = self.values.new_category()
        # Hậu tố giữ UNIQUE(name) và giới hạn VARCHAR(100)
        name = f"{raw_name[:92]}_{random.randint(2, 99)}"
        self.cur.execute(
            """INSERT INTO categories (name, description)
               VALUES (%s, %s)
               ON CONFLICT (name) DO NOTHING
               RETURNING id""",
            (name, description),
        )
        row = self.cur.fetchone()
        if row is None:
            return None
        self.state.add_category(row[0], name, description)
        return f"INSERT category={row[0]} name={name}"

    def _update_category(self) -> str | None:
        if not self.state.categories:
            return None

        category_id = self.state.pick_category_id()
        name, description = self.state.categories[category_id]
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.cur.execute(
            "UPDATE categories SET description = %s, updated_at = NOW() WHERE id = %s",
            (f"{description or name} (cập nhật {now})", category_id),
        )
        return f"UPDATE category={category_id} description"
