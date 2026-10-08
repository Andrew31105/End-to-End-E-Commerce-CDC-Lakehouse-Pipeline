# Feature Store & Lakehouse – Real-Time E-Commerce Pipeline

A **Lakehouse + Feature Store** system that reads a single CDC source
(PostgreSQL) to serve both **batch features** (Spark -> Iceberg -> Trino; for
training and BI) and **realtime features** (Flink -> ClickHouse; online
inference in <100 ms).

## Architecture

![Lakehouse Architecture](./jpg/lakehhouse2.jpg)


**Data lifecycle**

1. `mock_data.py` inserts/updates orders in PostgreSQL.
2. Debezium reads the WAL and publishes CDC events (`before`/`after`/`op`
   envelope) to Kafka.
3. **Realtime:** Flink (processing-time, 1-minute tumbling windows) computes
   global and per-product metrics, writes them to Kafka topics -> ClickHouse
   (Kafka engine + materialized view) as serving tables -> the Feature Store
   API serves them for online inference.
4. **Batch:** Airflow orchestrates Spark: Kafka -> Bronze (MinIO, raw Parquet)
   -> Silver (Iceberg, cleaned + dimension joins) -> Gold (Iceberg, KPIs and
   features) -> Trino views for BI.
5. **ML:** models are trained from ClickHouse/Iceberg and infer through the
   Feature Store API.

> **Historical backfill (one-off):** `scripts/run_olist_backfill.sh` writes 24
> months of Olist history (~1,000,000 orders) straight into Bronze using the same
> Debezium envelope (`op='c'`), so Silver/Gold process it with no code change.

## Dataset (Olist)

The 1-million-order backfill is built from the **Olist Brazilian E-Commerce**
dataset (license CC BY-NC-SA 4.0). The raw CSVs (121 MB) are **not committed** -
download them from Kaggle and drop them into `Dataset/`:

<https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>

Files used: `olist_orders`, `olist_order_items`, `olist_customers`,
`olist_products`, `product_category_name_translation`
(`payments`, `reviews`, `sellers`, `geolocation` are not used).

```text
Dataset/*.csv
  -> olist_prepare.py        DuckDB: flatten multi-item orders (quantity = COUNT(items),
                             total = SUM(price + freight), product = arg_max(price)),
                             map 8 Olist statuses to the app's 6, translate categories,
                             scale BRL->VND, shift the timeline so the last order = now
  -> olist_load_postgres.py  dims -> PostgreSQL with id offsets (1e3 / 1e5 / 1e6)
  -> olist_amplify.py        x11 + small noise -> 1,000,000 orders (order_id >= 1e8)
  -> backfill_olist.py       Parquet -> iceberg.bronze.orders_raw (before=NULL, op='c')
  -> Airflow DAG             Silver -> Gold, exactly like live CDC data
```

Design decisions and honest limits:

| Point | How it is handled |
|---|---|
| One Olist order has several items, the app's `orders` is flat | `quantity = COUNT(items)`, `total = SUM(price + freight)`, `unit_price = total / quantity`, `product_id = arg_max(price)` |
| Olist statuses (8) vs app statuses (6) | `created->pending`, `approved/invoiced/processing->confirmed`, `shipped`, `delivered`, `canceled->cancelled`, `unavailable->returned` |
| Currency (BRL vs VND seed data) | fixed `--fx 5000` so the whole warehouse uses one currency |
| Timeline 2016-2018 makes `CURRENT_DATE` metrics zero | shift so the newest order = now; drop the sparse tail after 2018-08-31 |
| `orders` id collision with CDC rows | backfill uses `order_id >= 100,000,000`; dims use offset id ranges |
| Amplified data compresses extremely well | 1M rows -> ~45 MB Parquet (11 near-duplicate copies + noise) |
| Realtime realism | `mock_data.py` samples a **real** Olist order (user/product/quantity/price) and uses Olist's real cancel/return rates |

## Repository structure

```text
.
+-- docker-compose.yml          # All infrastructure (16 containers)
+-- Dockerfile.{airflow,flink,spark,superset}
+-- requirements.txt            # Host Python deps (mock_data, ...)
+-- .env.example                # Config template (copy to .env)
+-- Dataset/                    # Olist raw CSVs - NOT committed (see Dataset)
|
+-- scripts/
|   +-- mock_data.py            # Traffic generator (sampled from real Olist orders)
|   +-- db.py                   # Shared PostgreSQL helper (.env -> psycopg2)
|   +-- register_cdc.sh         # Register the Debezium Postgres connector
|   +-- ensure_bi_views.sh      # Recreate iceberg.views after `down -v`
|   +-- spark-entrypoint.sh     # Spark master/worker entrypoint
|   +-- olist_prepare.py        # Olist CSV -> Parquet (DuckDB): dims + orders
|   +-- olist_load_postgres.py  # Olist dims -> PostgreSQL (categories/products/users)
|   +-- olist_amplify.py        # Amplify 98k real orders -> 1,000,000
|   +-- run_olist_backfill.sh   # One-shot: prepare -> dims -> amplify -> backfill -> DAG
|
+-- spark_jobs/                 # Run inside spark-master (Airflow docker exec)
|   +-- bronze_ingestion.py     # Kafka CDC -> MinIO Bronze (raw Parquet)
|   +-- backfill_olist.py       # Olist Parquet -> Bronze (Debezium-shaped envelope)
|   +-- dim_snapshot.py         # Postgres -> Silver dim_* (SCD snapshot)
|   +-- silver_transform.py     # Bronze -> Silver orders_clean (join, dedup)
|   +-- gold_aggregation.py     # Silver -> Gold KPI/feature tables
|
+-- flink_jobs/
|   +-- window_aggregation.py   # Load SQL + env -> statement set (realtime job)
|   +-- window_aggregates.sql   # 2 sinks: global + per-product metrics (1 min)
|
+-- feature_store/              # Shared by Spark jobs and the API
|   +-- config.py               # get_spark_session() + FEATURE_GROUPS metadata
|   +-- api/
|       +-- main.py             # FastAPI on :8000
|       +-- requirements.txt
|
+-- ml/                         # Sprint 4 - ML demo (RandomForest)
|   +-- train_realtime_model.py # Train to predict next-minute orders/revenue
|   +-- predict_next.py         # Online inference through the Feature Store API
|   +-- models/*.joblib
|
+-- airflow/
|   +-- dags/
|       +-- batch_iceberg_dag.py         # @hourly Bronze -> Silver -> Gold
|       +-- flink_realtime_dag.py        # */2 min: keep the Flink job running
|
+-- sql/
|   +-- init_postgres.sql       # Postgres schema + seed data
|   +-- iceberg_ddl.sql         # Bronze/Silver/Gold Iceberg DDL
|   +-- trino_views.sql         # 6 BI views in iceberg.views
|   +-- clickhouse/init.sql     # Serving tables + Kafka engine + MVs
|
+-- configs/
|   +-- spark-defaults.conf     # Spark + Iceberg + MinIO
|   +-- superset_config.py      # Dev-only: CSRF disabled (for REST API calls)
|   +-- metastore-site.xml, hive-site.xml
|   +-- trino-catalog/iceberg.properties
|
+-- jpg/                        # Architecture & ERD images
+-- data/                       # Local mounts (gitignored)
```

## Quick start

### 0. Prerequisites

```bash
cp .env.example .env                        # then fill in strong passwords
python -m pip install -r requirements.txt   # host scripts (mock_data, olist_*)
docker compose version                      # Docker Compose v2+
```

> `.env` holds the real credentials and is gitignored - never commit it.
> `Dataset/` and `data/` are gitignored too: the repo ships **code only**.

### 1. Start the whole stack

```bash
docker compose up -d --build    # --build only needed the first time
docker compose ps               # wait until services are healthy (~2-3 min)
```

### 2. Load the historical dataset (optional, recommended)

Needs the Olist CSVs in `Dataset/` (see the Dataset section above).

```bash
# CSV -> Parquet -> Postgres dims -> amplify to 1,000,000 -> Bronze -> Silver -> Gold
SAMPLE=5000 TARGET=50000 bash scripts/run_olist_backfill.sh   # quick smoke test
bash scripts/run_olist_backfill.sh                            # full 1,000,000 orders
```

### 3. Register CDC and start generating realtime traffic

```bash
bash scripts/register_cdc.sh            # Debezium connector -> pg.public.orders
python scripts/mock_data.py --rate 20   # 20 events/s, sampled from real Olist orders
```

### 4. Enable the pipelines in Airflow

Open http://localhost:8085 (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`) and
**unpause the two DAGs**:

| DAG | Schedule | Role |
|-----|----------|------|
| `batch_iceberg_pipeline` | `@hourly` | Bronze -> Silver -> Gold (Spark batch) |
| `flink_realtime_ensure_running` | `*/2 * * * *` | Submits/keeps the Flink realtime job alive (self-healing) |

```bash
# or via CLI (from the host, requires only the webserver):
docker compose exec airflow-webserver airflow dags unpause batch_iceberg_pipeline
docker compose exec airflow-webserver airflow dags unpause flink_realtime_ensure_running
```

While running, Airflow `docker exec`s into `spark-master` to `spark-submit` and
into `flink-jobmanager` to `flink run` (see `Dockerfile.airflow` - the image
adds the docker CLI reading `/var/run/docker.sock`).

### 5. Verify the data

```bash
# Realtime - ClickHouse (per-product, produced by Flink)
docker compose exec clickhouse clickhouse-client \
  --query "SELECT product_id, window_start, total_orders_1min, total_revenue_1min
           FROM ecommerce.product_metrics_1min ORDER BY window_start DESC LIMIT 5"

# Batch - Trino (Iceberg + views)
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.views.daily_revenue_summary ORDER BY report_date DESC LIMIT 5"
```

## Services

| Service           | URL                          | Credentials (.env)                          |
|-------------------|------------------------------|---------------------------------------------|
| PostgreSQL        | `localhost:5432`             | `POSTGRES_USER` / `POSTGRES_PASSWORD`       |
| Kafka (host)      | `localhost:29092`            | -                                          |
| Debezium REST     | `http://localhost:8083`      | -                                          |
| MinIO Console     | `http://localhost:9001`      | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`   |
| Spark Master UI   | `http://localhost:8080`      | -                                          |
| Flink UI          | `http://localhost:8081`      | -                                          |
| Trino             | `http://localhost:8084`      | -                                          |
| ClickHouse (HTTP) | `http://localhost:8123`      | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`   |
| Airflow           | `http://localhost:8085`      | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Feature Store API | `http://localhost:8000`      | (runs on the host, see below)               |
| Superset          | `http://localhost:8088`      | `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` |

> The Airflow webserver and scheduler must share the same
> `AIRFLOW__WEBSERVER__SECRET_KEY` (already set in compose), otherwise task
> logs fail with the 404 "served logs" error.

## Data model

### PostgreSQL (transactional source) - entities

![PostgreSQL Database Model](./jpg/database.svg)

| Entity | Description | Notes |
|--------|-------------|-------|
| `users` | Customers | `full_name`, `email`, `phone`, `city` |
| `categories` | Product categories | `name`, `description` |
| `products` | Products | FK `category_id`, `base_price`, `stock`, `is_active` |
| `orders` | Orders - **the CDC subject** | FK `user_id`, `product_id`; `quantity`, `unit_price`, `total_amount`, `status` (`pending/confirmed/shipped/delivered/cancelled/returned`) |

Only `public.orders` is captured by Debezium (see `scripts/register_cdc.sh`);
dims are snapshotted by Spark. Schema/seed: `sql/init_postgres.sql`.

### Iceberg (catalog `iceberg`, Hive Metastore) - batch

| Layer  | Tables                                                        |
|--------|---------------------------------------------------------------|
| Bronze | `bronze.orders_raw` (raw Parquet, MinIO `iceberg-datalake`)   |
| Silver | `silver.orders_clean`, `silver.dim_products_current`, `silver.dim_*` |
| Gold   | `gold.city_revenue_hourly`, `gold.category_revenue_hourly`, `gold.product_features_daily` |

### Trino views (`iceberg.views`) - BI query layer

| View | Pattern | Purpose |
|------|---------|---------|
| `city_revenue_latest`, `category_revenue_latest` | A - snapshot | KPIs of the latest closed hour (AOV recomputed as revenue/orders) |
| `city_daily_revenue`, `category_daily_revenue`, `daily_revenue_summary` | B - daily rollup | Daily trends, no hard-coded date |
| `top_trending_products` | C - ranking | Top-10 trending products of the latest snapshot |

Recreate the views if the schema disappears:

```bash
docker compose exec -T trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.views"
cat sql/trino_views.sql | docker compose exec -T trino trino
```

### ClickHouse (`ecommerce`) - realtime serving

| Table | Source | Notes |
|-------|--------|-------|
| `order_events` | Kafka CDC | ReplacingMergeTree (upsert by id) |
| `realtime_metrics_1min` (+`_kafka`+`_mv`) | Flink topic `flink.metrics_1min` | Global metrics per minute |
| `product_metrics_1min` (+`_kafka`+`_mv`) | Flink topic `flink.product_metrics_1min` | Per-product metrics per minute -> `order_features` |

## Feature Store API

Run it on the host (requires `feature_store/api/requirements.txt`; e.g. conda
env `crypto`):

```bash
pip install -r feature_store/api/requirements.txt
conda run -n crypto python -m uvicorn feature_store.api.main:app --host 127.0.0.1 --port 8000
```

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /feature-groups` | List feature groups |
| `GET /features/{feature_group}?entity_id=N` | Get a feature vector |

Feature groups currently served:

| Group | Entity | Source | Example |
|-------|--------|--------|---------|
| `order_features` | `product_id` | ClickHouse `product_metrics_1min` | `GET /features/order_features?entity_id=3` |
| `product_features` | `product_id` | Trino `gold.product_features_daily` + dim | `GET /features/product_features?entity_id=3` |
| `user_features` | `user_id` | Trino `silver.orders_clean` (30d aggregate) | `GET /features/user_features?entity_id=2` |

## BI - Apache Superset

Metabase OSS has no Trino/ClickHouse driver, so **Superset** is used
(`sqlalchemy-trino` and `clickhouse-connect` are added in `Dockerfile.superset`).

![E-commerce BI (Live) dashboard](./jpg/dashboard.jpeg)

1. Open http://localhost:8088 and log in with `admin` / `admin` (or
   `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD`).
2. Two databases are already registered:
   - **Trino** `trino://superset@trino:8080/iceberg/views` - reads Iceberg views
   - **ClickHouse** `clickhousedb+connect://admin:...@clickhouse:8123/ecommerce`
3. Create datasets from the tables/views, then charts, then dashboards (e.g.
   **E-commerce BI (Live)**).
4. `configs/superset_config.py` disables CSRF (dev-only) to ease REST API calls.

## ML demo (Sprint 4)

```bash
# Train: read ClickHouse product_metrics_1min, build lag-1/lag-2 features +
#        hour/minute, time split 80/20, RandomForest -> ml/models/*.joblib
conda run -n crypto python ml/train_realtime_model.py

# Online inference demo: call the Feature Store API :8000 + lags from
# ClickHouse -> predict the next minute
conda run -n crypto python ml/predict_next.py --product-id 3
```

![Realtime ML prediction output](./jpg/dudoanml.jpeg)

> This demo only illustrates the **serving layer -> ML** pipeline (the model is
> trained from/reads ClickHouse/Flink features and infers through the real
> Feature Store endpoint). Data is still small and event-window based, so
> RMSE/R2 are illustrative, not production-grade.

## Operations & troubleshooting

```bash
docker compose logs -f airflow-scheduler flink-jobmanager   # pipeline logs
docker compose exec flink-jobmanager /opt/flink/bin/flink list   # is the realtime job running?
docker compose ps                                           # full stack status
docker compose down                                         # stop (keep data volumes)
docker compose down -v                                      # stop + delete volumes (data loss)
```

**Notable gotchas**

- Airflow runs Spark jobs via `docker exec spark-master spark-submit` with
  `--conf spark.jars.ivy=/tmp/.ivy2` (the `spark` user has `HOME=/nonexistent`)
  and `--packages` for the Kafka/Postgres jars. Never run the Bronze job
  manually at the same time as Airflow (shared streaming `availableNow`
  checkpoint).
- Flink uses **processing time** (`PROCTIME`) because event-time watermarks
  never fire on a column computed from a nested `ROW`; the ClickHouse sink goes
  through a **Kafka engine + materialized view** because `flink-connector-jdbc`
  has no ClickHouse dialect.
- The `iceberg.views` schema can occasionally disappear (Bronze/Silver/Gold
  tables remain) - recreate the schema and re-run `sql/trino_views.sql`.

## Completed

- Sprint 1: Spark batch Bronze -> Silver -> Gold + dimensions (Airflow
  `batch_iceberg_pipeline`)
- Sprint 2: Flink realtime per-product -> ClickHouse + Feature Store API (:8000)
- Sprint 3: Trino BI views + Superset dashboard
- Sprint 4: ML demo (train + online inference)