"""Nguồn dữ liệu cho mock generator.

Ba thành phần, mỗi thành phần một trách nhiệm:

* :class:`OlistOrders`      – mẫu đơn hàng thật (``orders_resolved.parquet``)
* :class:`OlistDimensions`  – giá trị chiều thật (``dim_*.parquet``)
* :class:`SyntheticDimensions` – pool dựng sẵn khi thiếu Olist / chọn synthetic

Các hàm ``load_*`` không raise khi thiếu file: chúng log cảnh báo và trả về
phương án fallback, nhờ vậy generator luôn chạy được trên máy mới.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .util import log

OrderSample = tuple[int, int, int, float]

ORDERS_FILE = "orders_resolved.parquet"
DIM_FILES = {
    "categories": "dim_categories.parquet",
    "products": "dim_products.parquet",
    "users": "dim_users.parquet",
}

# ---------------------------------------------------------------------------
# Fallback – chỉ dùng khi không đọc được dim Olist
# ---------------------------------------------------------------------------
CITIES = (
    "Hà Nội", "TP.HCM", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
    "Huế", "Nha Trang", "Biên Hòa", "Vũng Tàu", "Đà Lạt",
)
LAST_NAMES = ("Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Vũ", "Đặng", "Bùi", "Đỗ", "Ngô")
FIRST_NAMES = (
    "Văn An", "Thị Bình", "Minh Châu", "Quốc Dũng", "Thu Hà",
    "Đức Huy", "Lan Anh", "Tuấn Kiệt", "Mai Linh", "Hoài Nam",
)
CATEGORY_POOL = (
    "Thiết bị âm thanh", "Đồng hồ thông minh", "Camera an ninh",
    "Thiết bị văn phòng", "Gaming Gear", "Nhà thông minh",
)
PRODUCT_POOL = (
    "Tai nghe", "Loa bluetooth", "Sạc dự phòng", "Bàn phím cơ",
    "Chuột không dây", "Màn hình", "Webcam", "Ổ cứng SSD",
)
BRANDS = ("Anker", "Logitech", "Xiaomi", "Samsung", "Sony", "Asus", "Kingston", "JBL")


def _resolve(path_str: str, root: Path) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else root / path


# ---------------------------------------------------------------------------
# Đơn hàng
# ---------------------------------------------------------------------------
@dataclass
class OlistOrders:
    """Mẫu đơn hàng Olist (user_id, product_id, quantity, unit_price)."""

    samples: list[OrderSample]
    cancel_rate: float
    return_rate: float

    @classmethod
    def load(cls, path: Path) -> "OlistOrders":
        import duckdb

        con = duckdb.connect()
        con.execute("PRAGMA threads = 2")
        samples = con.execute(f"""
            SELECT user_id, product_id, quantity, CAST(unit_price AS DOUBLE)
            FROM read_parquet('{path.as_posix()}')
        """).fetchall()
        cancel_rate, return_rate = con.execute(f"""
            SELECT AVG(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END),
                   AVG(CASE WHEN status = 'returned'  THEN 1 ELSE 0 END)
            FROM read_parquet('{path.as_posix()}')
        """).fetchone()
        con.close()

        if not samples:
            raise ValueError(f"{path}: không có đơn nào")

        log(f" Nguồn Olist: {len(samples):,} đơn mẫu từ {path.name} "
            f"| huỷ {float(cancel_rate or 0):.2%} | trả {float(return_rate or 0):.2%}")
        return cls(
            samples=[tuple(row) for row in samples],  # type: ignore[misc]
            cancel_rate=float(cancel_rate or 0.0),
            return_rate=float(return_rate or 0.0),
        )

    def sample(self) -> OrderSample:
        return random.choice(self.samples)


# ---------------------------------------------------------------------------
# Giá trị bảng chiều
# ---------------------------------------------------------------------------
class OlistDimensions:
    """Giá trị chiều thật từ ``dim_*.parquet``.

    Id trong 3 file trùng với id đã nạp vào Postgres bằng
    ``scripts/olist_load_postgres.py`` nên không cần map lại.
    """

    def __init__(self, prepared_dir: Path) -> None:
        import duckdb

        paths = {name: prepared_dir / filename for name, filename in DIM_FILES.items()}
        missing = [p.name for p in paths.values() if not p.is_file()]
        if missing:
            raise FileNotFoundError(f"thiếu {', '.join(missing)}")

        con = duckdb.connect()
        self.categories: list[tuple[int, str, str]] = con.execute(f"""
            SELECT id, name, COALESCE(description, '')
            FROM read_parquet('{paths["categories"].as_posix()}')
        """).fetchall()
        self.products = con.execute(f"""
            SELECT id, name, category_id, CAST(base_price AS DOUBLE)
            FROM read_parquet('{paths["products"].as_posix()}')
            WHERE is_active
        """).fetchall()
        users = con.execute(f"""
            SELECT full_name, city
            FROM read_parquet('{paths["users"].as_posix()}')
            WHERE city IS NOT NULL AND city <> ''
        """).fetchall()
        con.close()

        self.category_ids = [row[0] for row in self.categories]
        self.cities = sorted({row[1] for row in users})
        self.user_names = [row[0] for row in users]
        self.product_names = [row[1] for row in self.products]
        self.prices = [row[3] for row in self.products if row[3] and row[3] > 0]
        self.prices_by_category: dict[int, list[float]] = {}
        for _, _, category_id, price in self.products:
            if price and price > 0:
                self.prices_by_category.setdefault(category_id, []).append(price)

        if not (self.category_ids and self.cities and self.product_names):
            raise ValueError("dim Olist rỗng")

        log(f" Nguồn dim Olist: {len(self.categories):,} categories | "
            f"{len(self.products):,} products | {len(self.cities):,} cities | "
            f"{len(self.user_names):,} users")

    def city(self, exclude: str | None = None) -> str:
        pool = [c for c in self.cities if c != exclude] if exclude else self.cities
        return random.choice(pool or self.cities)

    def user_name(self) -> str:
        return random.choice(self.user_names)

    def product_name(self) -> str:
        base = random.choice(self.product_names).split(" #")[0]
        return f"{base} v{random.randint(2, 9)}"

    def new_category(self) -> tuple[str, str]:
        _, name, description = random.choice(self.categories)
        return name, description or "Olist category"

    def price(self, category_id: int | None = None) -> float:
        """Giá trong phân bố thật của Olist (±20%).

        Nếu biết category thì lấy đúng phân bố của category đó.
        """
        pool = self.prices
        if category_id is not None:
            pool = self.prices_by_category.get(category_id) or self.prices
        if not pool:
            return 100.0
        return round(max(random.choice(pool) * random.uniform(0.8, 1.2), 0.01), 2)


class SyntheticDimensions:
    """Pool dựng sẵn – dùng khi không có Olist hoặc ``--dim-source synthetic``."""

    category_ids: list[int] | None = None

    def city(self, exclude: str | None = None) -> str:
        pool = [c for c in CITIES if c != exclude] if exclude else list(CITIES)
        return random.choice(pool or list(CITIES))

    def user_name(self) -> str:
        return f"{random.choice(LAST_NAMES)} {random.choice(FIRST_NAMES)}"

    def product_name(self) -> str:
        return (f"{random.choice(BRANDS)} {random.choice(PRODUCT_POOL)} "
                f"{random.randint(100, 999)}")

    def new_category(self) -> tuple[str, str]:
        name = f"{random.choice(CATEGORY_POOL)} {random.randint(1, 9999)}"
        description = f"Danh mục sinh tự động lúc {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}"
        return name, description

    def price(self, category_id: int | None = None) -> float:
        return round(random.uniform(150_000, 40_000_000), -3)


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------
def load_orders(source: str, path_str: str, root: Path) -> OlistOrders | None:
    """Trả về nguồn đơn Olist, hoặc ``None`` để dùng chế độ synthetic."""
    if source != "olist":
        log(" Chế độ synthetic: chọn sản phẩm/user ngẫu nhiên từ Postgres")
        return None

    path = _resolve(path_str, root)
    if not path.is_file():
        log(f" ! Không thấy {path} → dùng chế độ synthetic "
            "(chạy scripts/olist_prepare.py để có nguồn Olist)")
        return None
    try:
        return OlistOrders.load(path)
    except ImportError:
        log(" ! Chưa cài duckdb → dùng chế độ synthetic")
    except Exception as exc:  # parquet lỗi / file dở dang
        log(f" ! Không đọc được {path}: {exc} → dùng chế độ synthetic")
    return None


def load_dimensions(
    source: str, dir_str: str, root: Path
) -> OlistDimensions | SyntheticDimensions:
    """Trả về nguồn giá trị chiều (Olist nếu đọc được, ngược lại synthetic)."""
    if source != "olist":
        log(" Dimension synthetic: dùng pool dựng sẵn")
        return SyntheticDimensions()

    path = _resolve(dir_str, root)
    try:
        return OlistDimensions(path)
    except ImportError:
        log(" ! Chưa cài duckdb → dimension dùng pool synthetic")
    except Exception as exc:  # thiếu file / parquet lỗi
        log(f" ! Không đọc được dim Olist ({path}): {exc} → dùng pool synthetic")
    return SyntheticDimensions()
