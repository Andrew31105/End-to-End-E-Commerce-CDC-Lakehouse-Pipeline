"""Cache trạng thái bảng chiều trong PostgreSQL.

Generator chạy ở rate cao nên không thể SELECT lại bảng chiều cho mỗi event.
``DbState`` nạp một lần, sau đó mọi insert/update đều cập nhật tại chỗ.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from .util import log


@dataclass
class Product:
    name: str
    base_price: float
    category_id: int
    is_active: bool


@dataclass
class DbState:
    categories: dict[int, tuple[str, str]] = field(default_factory=dict)
    products: dict[int, Product] = field(default_factory=dict)
    product_ids: list[int] = field(default_factory=list)
    active_product_ids: list[int] = field(default_factory=list)
    users: dict[int, str | None] = field(default_factory=dict)
    user_ids: list[int] = field(default_factory=list)
    olist_categories: list[int] = field(default_factory=list)

    @classmethod
    def load(cls, cur, olist_category_ids: list[int] | None = None) -> "DbState":
        cur.execute("SELECT id, name, COALESCE(description, '') FROM categories")
        categories = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        cur.execute("SELECT id, name, base_price, category_id, is_active FROM products")
        products = {
            row[0]: Product(
                name=row[1],
                base_price=float(row[2]),
                category_id=row[3],
                is_active=bool(row[4]),
            )
            for row in cur.fetchall()
        }

        cur.execute("SELECT id, city FROM users")
        users = {row[0]: row[1] for row in cur.fetchall()}

        state = cls(
            categories=categories,
            products=products,
            product_ids=list(products),
            active_product_ids=[pid for pid, p in products.items() if p.is_active],
            users=users,
            user_ids=list(users),
            # Chỉ dùng category có thật trong Olist *và* có trong Postgres
            # → INSERT/UPDATE product không bao giờ vi phạm FK.
            olist_categories=[
                cid for cid in (olist_category_ids or []) if cid in categories
            ],
        )
        log(f"Dim reference: {len(categories)} categories, "
            f"{len(products)} products, {len(users)} users")
        return state

    # ---- chọn ngẫu nhiên ----
    def pick_category_id(self) -> int:
        return random.choice(self.olist_categories or list(self.categories))

    def pick_product(self) -> int:
        return random.choice(self.product_ids)

    # ---- cập nhật cache sau khi ghi DB ----
    def add_category(self, category_id: int, name: str, description: str = "") -> None:
        self.categories[category_id] = (name, description)

    def add_product(self, product_id: int, product: Product) -> None:
        self.products[product_id] = product
        self.product_ids.append(product_id)
        if product.is_active:
            self.active_product_ids.append(product_id)

    def add_user(self, user_id: int, city: str) -> None:
        self.users[user_id] = city
        self.user_ids.append(user_id)

    def set_active(self, product_id: int, active: bool) -> None:
        self.products[product_id].is_active = active
        if active:
            if product_id not in self.active_product_ids:
                self.active_product_ids.append(product_id)
        elif product_id in self.active_product_ids:
            self.active_product_ids.remove(product_id)
