# Feature Store & Lakehouse – Real-Time E-Commerce Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)
![Kafka](https://img.shields.io/badge/Kafka-Latest-red.svg)
![Debezium](https://img.shields.io/badge/Debezium-CDC-orange.svg)
![MinIO](https://img.shields.io/badge/MinIO-S3-green.svg)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-Batch-orange.svg)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-Lakehouse-blue.svg)
![Trino](https://img.shields.io/badge/Trino-Query_Engine-purple.svg)
![Apache Flink](https://img.shields.io/badge/Apache_Flink-Streaming-red.svg)
![ClickHouse](https://img.shields.io/badge/ClickHouse-Serving-yellow.svg)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-lightblue.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)

##  Project Overview

**Feature Store & Lakehouse** là kiến trúc kết hợp **CDC (Change Data Capture)** thời gian thực với **Data Lakehouse** trên nền Iceberg, phục vụ cả hai mục tiêu:

| Mục tiêu | Công nghệ | Mô tả |
|----------|-----------|-------|
| **Real-Time Features** | Flink → ClickHouse | Xử lý stream 1-phút window, phục vụ online inference (<100ms) |
| **Batch Features** | Spark → Iceberg → Trino | Lưu trữ historical data, phục vụ training & BI |

**Use Case:** Theo dõi doanh thu real-time, tính toán feature cho ML model (recommendation, fraud detection) và BI dashboard — tất cả từ cùng một nguồn dữ liệu CDC.

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        TRANSACTION SOURCE                            │
│  ┌──────────┐    INSERT/UPDATE     ┌──────────────┐                  │
│  │mock_data │ ──────────────────►  │  PostgreSQL  │                  │
│  └──────────┘                      │  (Source)    │                  │
│                                    └──────┬───────┘                  │
│                                           │ WAL (logical decoding)  │
│                                           ▼                         │
│                              ┌─────────────────────┐                 │
│                              │     Debezium        │                 │
│                              │   (Kafka Connect)   │                 │
│                              └──────────┬──────────┘                 │
│                                         │                            │
│                                         ▼                            │
│                              ┌─────────────────────┐                 │
│                              │   Apache Kafka      │                 │
│                              │   (Message Broker)  │                 │
│                              └──────────┬──────────┘                 │
└─────────────────────────────────────────┼────────────────────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
              ▼                           │                           ▼
┌──────────────────────────┐              │            ┌──────────────────────────┐
│     STREAMING PATH        │              │            │       BATCH PATH          │
│                           │              │            │                           │
│  ┌─────────────────────┐  │              │            │  ┌─────────────────────┐  │
│  │   Apache Flink      │  │              │            │  │  MinIO (Bronze)     │  │
│  │  (Windowing 1-min)  │  │              │            │  │  Raw Parquet        │  │
│  └──────────┬──────────┘  │              │            │  └──────────┬──────────┘  │
│             │              │              │            │             │              │
│             ▼              │              │            │             ▼              │
│  ┌─────────────────────┐   │              │            │  ┌─────────────────────┐  │
│  │  ClickHouse         │   │              │            │  │  PySpark ETL        │  │
│  │  (Serving Layer)    │   │              │            │  │  (Transform)        │  │
│  └──────────┬──────────┘   │              │            │  └──────────┬──────────┘  │
│             │              │              │            │             │              │
│             │              │              │            │             ▼              │
│             │              │              │            │  ┌─────────────────────┐  │
│             │              │              │            │  │  Apache Iceberg     │  │
│             │              │              │            │  │  (Silver / Gold)    │  │
│             │              │              │            │  └──────────┬──────────┘  │
│             │              │              │            │             │              │
│             │              │              │            │             ▼              │
│             │              │              │            │  ┌─────────────────────┐  │
│             │              │              │            │  │  Trino              │  │
│             │              │              │            │  │  (Query Engine)     │  │
│             │              │              │            │  └──────────┬──────────┘  │
│             │              │              │            │             │              │
└─────────────┼──────────────┘              │            └─────────────┼──────────────┘
              │                             │                          │
              ▼                             │                          ▼
┌───────────────────────────────────────────┴──────────────────────────────┐
│                          SERVING LAYER                                    │
│                                                                           │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │
│  │ Feature Store    │    │  Metabase        │    │  Airflow         │     │
│  │ API (FastAPI)    │    │  (BI Dashboard)  │    │  (Orchestrator)  │     │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘     │
└───────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Transaction Source & Streaming (PostgreSQL + Debezium + Kafka)**
   - `mock_data.py` liên tục `INSERT / UPDATE` đơn hàng vào PostgreSQL
   - Debezium đọc WAL, đẩy Raw CDC Events vào Kafka topic `pg.public.orders`

2. **Streaming Path (Flink → ClickHouse)**
   - Flink đọc CDC events từ Kafka
   - 1-min tumbling window aggregation (doanh thu, trạng thái đơn hàng)
   - Ghi kết quả vào ClickHouse (Serving Table) cho real-time query

3. **Batch Path (MinIO → Spark → Iceberg → Trino)**
   - CDC events được dump từ Kafka xuống MinIO Bronze Zone (raw Parquet)
   - PySpark đọc Bronze → làm sạch → ghi Iceberg Silver (dimension joins, dedup)
   - PySpark aggregate → Iceberg Gold (KPIs, features)
   - Trino query trực tiếp Iceberg tables cho BI và batch feature serving

4. **Orchestration (Airflow)**
   - Schedule Spark batch jobs (5-min interval)
   - Trigger Flink window jobs
   - Monitoring & alerting

## Repository Structure

```text
.
├── docker-compose.yml              # Infrastructure (16 services)
├── Dockerfile.flink                # Flink + PyFlink image
├── Dockerfile.spark                # Spark + Iceberg + MinIO image
├── README.md
├── TODO.md
├── requirements.txt
│
├── scripts/
│   ├── mock_data.py                # Python traffic generator (INSERT/UPDATE)
│   ├── register_cdc.sh             # Đăng ký Debezium PostgreSQL connector
│   └── spark-entrypoint.sh         # Spark Master/Worker entrypoint
│
├── flink_jobs/
│   ├── streaming_etl.py            # Flink CDC → ClickHouse streaming
│   └── window_aggregation.py       # Flink 1-min window aggregation (placeholder)
│
├── spark_jobs/
│   ├── bronze_ingestion.py         # Kafka → MinIO Bronze (placeholder)
│   ├── silver_transform.py         # MinIO Bronze → Iceberg Silver (placeholder)
│   └── gold_aggregation.py         # Iceberg Silver → Iceberg Gold (placeholder)
│
├── sql/
│   ├── init_postgres.sql           # PostgreSQL schema + seed data
│   ├── clickhouse/                # ClickHouse serving tables + MVs
│   ├── iceberg_ddl.sql             # Iceberg table definitions (Silver/Gold)
│   └── trino_views.sql             # Trino views trên Iceberg
│
├── configs/
│   ├── kafka_settings.py           # Kafka consumer/producer config
│   ├── spark-defaults.conf         # Spark + Iceberg + MinIO configuration
│   └── trino-catalog/
│       └── iceberg.properties      # Trino Iceberg connector config
│
├── airflow/
│   └── dags/
│       ├── feature_pipeline_dag.py # Main Feature Store pipeline DAG (placeholder)
│       └── batch_iceberg_dag.py    # Daily batch ETL DAG (placeholder)
│
├── feature_store/
│   ├── config.py                   # Feature Group definitions
│   └── api/
│       ├── main.py                 # FastAPI Feature Store API (placeholder)
│       └── requirements.txt
│
└── data/
    ├── minio/                      # MinIO persistent data
    └── iceberg/                    # Iceberg warehouse metadata (local dev)
```

##  Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- Python 3.9+ (local dev)
- 16GB+ RAM khuyến nghị (toàn bộ stack: 16 containers)

### Step 1: Start Infrastructure

> **Thông tin đăng nhập được quản lý tập trung trong file `.env`** (đã gitignore).
> Nếu chưa có, hãy tạo từ bản mẫu: `cp .env.example .env` rồi điền mật khẩu mạnh.

```bash
docker compose up -d
```

Đợi tất cả services healthy (~2-3 phút):

```bash
docker compose ps
```

### Step 2: Verify All Services

| Service          | URL                            | Credentials (lấy từ `.env`)        |
|------------------|--------------------------------|------------------------------------|
| PostgreSQL       | `localhost:5432`               | `$POSTGRES_USER` / `$POSTGRES_PASSWORD` |
| Kafka            | `localhost:9092`               | –                                  |
| Debezium REST    | `http://localhost:8083`        | –                                  |
| MinIO S3 API     | `http://localhost:9000`        | `$MINIO_ROOT_USER` / `$MINIO_ROOT_PASSWORD` |
| MinIO Console    | `http://localhost:9001`        | `$MINIO_ROOT_USER` / `$MINIO_ROOT_PASSWORD` |
| Spark Master UI  | `http://localhost:8080`        | –                                  |
| Spark Worker UI  | `http://localhost:8082`        | –                                  |
| Trino Web UI     | `http://localhost:8084`        | –                                  |
| ClickHouse       | `http://localhost:8123`        | `$CLICKHOUSE_USER` / `$CLICKHOUSE_PASSWORD` |
| Flink Web UI     | `http://localhost:8081`        | –                                  |
| Airflow          | `http://localhost:8085`        | `$AIRFLOW_ADMIN_USER` / `$AIRFLOW_ADMIN_PASSWORD` |
| Metabase         | `http://localhost:3000`        | setup on first run                 |

### Step 3: Register CDC Connector

```bash
bash scripts/register_cdc.sh
```

Kiểm tra connector:

```bash
curl -s http://localhost:8083/connectors/ecommerce-pg-connector/status | python3 -m json.tool
```

### Step 4: Start Mock Traffic Generator

```bash
pip install psycopg2-binary faker
python scripts/mock_data.py --interval 2.0 --update-ratio 0.3
```

### Step 5: Submit Flink Streaming Job

```bash
# Tải Flink Kafka connector
docker compose exec flink-jobmanager wget -P /opt/flink/lib \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

docker compose exec flink-taskmanager wget -P /opt/flink/lib \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

# Submit job
docker compose exec flink-jobmanager \
  /opt/flink/bin/flink run --python /opt/project/flink_jobs/streaming_etl.py
```

### Step 6: Run Spark Batch Jobs (Manual)

```bash
# Bronze: Kafka → MinIO
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/bronze_ingestion.py

# Silver: MinIO Bronze → Iceberg
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/silver_transform.py

# Gold: Iceberg Silver → Iceberg Gold
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/gold_aggregation.py
```

### Step 7: Query Iceberg via Trino

```bash
# Mở Trino CLI
docker compose exec trino trino

# Kiểm tra Iceberg tables
SHOW CATALOGS;           -- Sẽ thấy 'iceberg'
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

# Query feature
SELECT * FROM iceberg.gold.city_revenue_hourly
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

### Step 8: Visualize in Metabase

1. Mở `http://localhost:3000`
2. Complete first-time setup
3. Add ClickHouse và Trino làm data sources
4. Tạo dashboards cho real-time revenue và order trends

##  Data Model

### PostgreSQL (Transactional – Source)

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  categories  │     │  products   │     │      users       │
├─────────────┤     ├─────────────┤     ├──────────────────┤
│ id (PK)     │◄────│ category_id │     │ id (PK)          │
│ name        │     │ name        │     │ full_name        │
│ description │     │ base_price  │     │ email            │
└─────────────┘     │ stock       │     │ phone            │
                    │ is_active   │     │ city             │
                    └─────────────┘     └──────────────────┘
                           │                      │
                           ▼                      ▼
                    ┌─────────────────────────────────────┐
                    │              orders                  │
                    ├─────────────────────────────────────┤
                    │ id (PK), user_id, product_id       │
                    │ quantity, unit_price, total_amount  │
                    │ status (pending→confirmed→shipped   │
                    │         →delivered/cancelled)        │
                    │ ordered_at, updated_at               │
                    └─────────────────────────────────────┘
```

### Iceberg (Lakehouse – Silver & Gold)

| Layer  | Table                          | Mô tả                                |
|--------|--------------------------------|--------------------------------------|
| Silver | `iceberg.silver.orders_clean`  | Cleaned + flattened CDC, dimension joins |
| Gold   | `iceberg.gold.city_revenue_hourly`   | Revenue KPI by city (hourly)    |
| Gold   | `iceberg.gold.category_revenue_hourly` | Revenue KPI by category (hourly) |
| Gold   | `iceberg.gold.product_features_daily` | Product popularity features (daily) |

### ClickHouse (Serving – Real-Time)

| Table / MV                     | Engine              | Mô tả                           |
|--------------------------------|---------------------|---------------------------------|
| `order_events`                 | ReplacingMergeTree  | Raw CDC events (upsert by id)   |
| `revenue_per_minute_mv`        | SummingMergeTree    | Doanh thu theo phút             |
| `order_status_mv`              | AggregatingMergeTree| Trạng thái đơn hàng real-time   |

## 🔧 Configuration

| Variable               | Default                          | Description                        |
|------------------------|----------------------------------|------------------------------------|
| `PG_HOST`              | localhost                        | PostgreSQL host                    |
| `PG_DB`                | ecommerce                        | Database name                      |
| `KAFKA_BOOTSTRAP`      | kafka:9092                       | Kafka bootstrap server             |
| `KAFKA_TOPIC`          | pg.public.orders                 | Debezium topic for orders          |
| `MINIO_ENDPOINT`       | http://minio:9000                | MinIO S3 endpoint                  |
| `ICEBERG_WAREHOUSE`    | s3a://iceberg-warehouse          | Iceberg warehouse location         |
| `CLICKHOUSE_URL`       | jdbc:clickhouse://clickhouse:8123/ecommerce | ClickHouse JDBC    |

## 🧪 Testing CDC Pipeline End-to-End

```bash
# 1. Kiểm tra dữ liệu ban đầu
docker compose exec postgres psql -U admin -d ecommerce -c "SELECT count(*) FROM orders;"

# 2. Bắt đầu traffic generator
python scripts/mock_data.py --interval 1.0 &

# 3. Kiểm tra Kafka topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pg.public.orders \
  --max-messages 2

# 4. Kiểm tra ClickHouse
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(), status FROM order_events GROUP BY status"

# 5. Kiểm tra Iceberg qua Trino
docker compose exec trino trino \
  --execute "SELECT count(*) FROM iceberg.silver.orders_clean"
```


# 3. Kiểm tra Kafka topic đang nhận events
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pg.public.orders \
  --from-beginning --max-messages 3

# 4. Kiểm tra ClickHouse đã nhận dữ liệu
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(), status FROM ecommerce.order_events GROUP BY status"
```

##  Notes

- **Debezium Raw Envelope:** Connector gửi raw CDC envelope (có `before`, `after`, `op`) để Flink tự xử lý logic CDC.
- **Flink Checkpointing:** Được bật mỗi 10 giây để đảm bảo exactly-once semantics và fault-tolerance.
- **Kafka Connector Jar:** Cần tải `flink-sql-connector-kafka` vào `lib/` của cả JobManager và TaskManager.
- **ClickHouse Sink:** Sử dụng HTTP interface để insert, tránh phụ thuộc JDBC driver nặng.
- **Legacy Code:** Các thư mục `notebooks/`, `scraper/`, `spark_jobs/`, `data/` chứa code cũ, có thể giữ lại tham khảo hoặc xoá.
- url
- scraped_at

## 6) Next Steps

Check TODO roadmap in `TODO.md` and start from Sprint 1.
