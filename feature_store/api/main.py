"""
============================================================================
 feature_store/api/main.py
 Feature Store API – FastAPI phục vụ features cho ML/BI
============================================================================
 Endpoints:
   GET /health                      – health check
   GET /feature-groups              – danh mục feature group (từ FEATURE_GROUPS)
   GET /features/{feature_group}    – feature vector theo entity_id
        query: entity_id (int)  (product_id hoặc user_id tuỳ group)

 Nguồn dữ liệu:
   - order_features   : ClickHouse ecommerce.product_metrics_1min (Flink real-time)
   - product_features : Trino  iceberg.gold.product_features_daily + dim_products (stock)
   - user_features    : Trino  iceberg.silver.orders_clean (agg 30 ngày)

 Chạy (từ thư mục gốc project, dùng env có cài requirements):
   conda run -n crypto python -m uvicorn feature_store.api.main:app \
       --host 127.0.0.1 --port 8000
============================================================================
"""
import os

from fastapi import FastAPI, HTTPException, Query

from ..config import FEATURE_GROUPS

app = FastAPI(
    title="Feature Store API",
    description="Serving layer for Feature Store / Lakehouse (ClickHouse realtime + Trino batch)",
    version="0.2.0",
)

# ---------------------------------------------------------------------------
# Connection config (đọc từ .env; chạy trên host nên dùng port đã map ra host)
# ---------------------------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9002"))   # host 9002 -> container 9000
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "ecommerce")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8084"))


# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------
def _run_clickhouse(sql: str) -> list[dict]:
    from clickhouse_driver import Client

    client = Client(
        host=CH_HOST, port=CH_PORT, user=CH_USER,
        password=CH_PASSWORD, database=CH_DB,
    )
    try:
        rows, cols = client.execute(sql, with_column_types=True)
        names = [c[0] for c in cols]
        return [dict(zip(names, r)) for r in rows]
    finally:
        client.disconnect()


def _run_trino(sql: str) -> list[dict]:
    import trino

    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="feature-api",
        catalog="iceberg", http_scheme="http",
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        names = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(names, r)) for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fetch theo từng feature group -> (features: dict, metadata: dict) hoặc None
# ---------------------------------------------------------------------------
def _fetch_order_features(entity_id: int):
    cfg = FEATURE_GROUPS["order_features"]
    cols = ", ".join(cfg["features"])
    sql = (
        f"SELECT window_start, {cols} FROM {cfg['table']} "
        f"WHERE product_id = {entity_id} ORDER BY window_start DESC LIMIT 1"
    )
    rows = _run_clickhouse(sql)
    if not rows:
        return None
    row = dict(rows[0])
    meta = {"window_start": str(row.pop("window_start"))}
    return {f: row[f] for f in cfg["features"]}, meta


def _fetch_product_features(entity_id: int):
    cfg = FEATURE_GROUPS["product_features"]
    sql = f"""
    WITH latest AS (SELECT max(snapshot_date) AS d FROM iceberg.gold.product_features_daily)
    SELECT p.order_count_7d, p.revenue_7d,
           CAST(p.order_count_7d AS double) / NULLIF(d.stock, 0) AS stock_turnover_rate,
           p.trending_score,
           p.snapshot_date
    FROM iceberg.gold.product_features_daily p
    JOIN iceberg.silver.dim_products_current d ON d.product_id = p.product_id
    CROSS JOIN latest l
    WHERE p.product_id = {entity_id} AND p.snapshot_date = l.d
    """
    rows = _run_trino(sql)
    if not rows:
        return None
    row = dict(rows[0])
    meta = {"snapshot_date": str(row.pop("snapshot_date"))}
    return {f: row[f] for f in cfg["features"]}, meta


def _fetch_user_features(entity_id: int):
    cfg = FEATURE_GROUPS["user_features"]
    sql = f"""
    WITH base AS (
        SELECT order_id, quantity, total_amount, status, category_name
        FROM iceberg.silver.orders_clean
        WHERE user_id = {entity_id} AND ordered_at >= CURRENT_DATE - INTERVAL '30' DAY
    ),
    valid AS (SELECT * FROM base WHERE status NOT IN ('cancelled','returned')),
    basket AS (SELECT order_id, SUM(quantity) AS qty FROM base GROUP BY order_id),
    fav    AS (SELECT category_name FROM valid GROUP BY category_name ORDER BY count(*) DESC LIMIT 1)
    SELECT
        (SELECT count(DISTINCT order_id) FROM valid)       AS total_orders_30d,
        (SELECT COALESCE(SUM(total_amount), 0) FROM valid) AS total_spent_30d,
        (SELECT category_name FROM fav)                    AS favorite_category,
        (SELECT avg(qty) FROM basket)                      AS avg_basket_size
    """
    rows = _run_trino(sql)
    if not rows:
        return None
    row = dict(rows[0])
    return {f: row[f] for f in cfg["features"]}, {}


FETCHERS = {
    "order_features": _fetch_order_features,
    "product_features": _fetch_product_features,
    "user_features": _fetch_user_features,
}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "service": "feature-store-api", "version": app.version}


@app.get("/feature-groups")
async def feature_groups():
    return [
        {
            "name": name,
            "description": cfg["description"],
            "source": cfg["source"],
            "entity": cfg["entity"],
            "features": cfg["features"],
            "sla": cfg["sla"],
        }
        for name, cfg in FEATURE_GROUPS.items()
    ]


@app.get("/features/{feature_group}")
def get_features(
    feature_group: str,
    entity_id: int = Query(..., gt=0, description="entity id (product_id hoặc user_id)"),
):
    cfg = FEATURE_GROUPS.get(feature_group)
    if not cfg:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown feature_group '{feature_group}'. Known: {list(FEATURE_GROUPS)}",
        )

    fetch = FETCHERS.get(feature_group)
    if fetch is None:
        raise HTTPException(status_code=501, detail=f"Feature group '{feature_group}' chưa được serve")

    try:
        result = fetch(entity_id)
    except Exception as exc:  # noqa: BLE001 - trả lỗi provider rõ ràng
        raise HTTPException(status_code=502, detail=f"Provider error: {exc}") from exc

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No feature data for '{feature_group}' entity_id={entity_id}",
        )

    features, meta = result
    return {
        "feature_group": feature_group,
        "entity": {cfg["entity"]: entity_id},
        "source": cfg["source"],
        "features": features,
        "metadata": meta,
    }

