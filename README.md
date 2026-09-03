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

**Feature Store & Lakehouse** is an architecture that combines **real-time CDC (Change Data Capture)** with an **Iceberg-based Data Lakehouse**, serving two goals:

| Goal | Technology | Description |
|----------|-----------|-------|
| **Real-Time Features** | Flink → ClickHouse | Process 1-minute window streams, serving online inference (<100ms) |
| **Batch Features** | Spark → Iceberg → Trino | Store historical data, serving training & BI |

**Use Case:** Track real-time revenue, compute features for ML models (recommendation, fraud detection) and BI dashboards — all from the same CDC data source.

##  Architecture

![Lakehouse Architecture](./jpg/lakehhouse2.jpg)

### Data Flow

1. **Transaction Source & Streaming (PostgreSQL + Debezium + Kafka)**
   - `mock_data.py` continuously performs `INSERT / UPDATE` on orders in PostgreSQL
   - Debezium reads the WAL and pushes raw CDC events into the Kafka topic `pg.public.orders`

2. **Streaming Path (Flink → ClickHouse)**
   - Flink reads CDC events from Kafka
   - 1-min tumbling window aggregation (revenue, order status)
   - Writes results into ClickHouse (serving table) for real-time queries

3. **Batch Path (MinIO → Spark → Iceberg → Trino)**
   - CDC events are dumped from Kafka into the MinIO Bronze Zone (raw Parquet)
   - PySpark reads Bronze → cleans → writes Iceberg Silver (dimension joins, dedup)
   - PySpark aggregates → Iceberg Gold (KPIs, features)
   - Trino queries Iceberg tables directly for BI and batch feature serving

4. **Orchestration (Airflow)**
   - Schedules Spark batch jobs (5-min interval)
   - Triggers Flink window jobs
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
│   ├── register_cdc.sh             # Register Debezium PostgreSQL connector
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
│   └── trino_views.sql             # Trino views on top of Iceberg
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
- 16GB+ RAM recommended (full stack: 16 containers)

### Step 1: Start Infrastructure

> **Credentials are managed centrally in the `.env` file** (gitignored).
> If it does not exist yet, create it from the template: `cp .env.example .env` and fill in strong passwords.

```bash
docker compose up -d
```

Wait for all services to become healthy (~2-3 minutes):

```bash
docker compose ps
```

### Step 2: Verify All Services

| Service          | URL                            | Credentials (from `.env`)         |
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

Check the connector:

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
# Download the Flink Kafka connector
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
# Open the Trino CLI
docker compose exec trino trino

# Inspect Iceberg tables
SHOW CATALOGS;           -- You should see 'iceberg'
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

# Query feature
SELECT * FROM iceberg.gold.city_revenue_hourly
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

### Step 8: Visualize in Metabase

1. Open `http://localhost:3000`
2. Complete the first-time setup
3. Add ClickHouse and Trino as data sources
4. Create dashboards for real-time revenue and order trends

##  Data Model

### PostgreSQL (Transactional – Source)

![PostgreSQL Database Model](./jpg/database.svg)

### Iceberg (Lakehouse – Silver & Gold)

| Layer  | Table                          | Description                          |
|--------|--------------------------------|--------------------------------------|
| Silver | `iceberg.silver.orders_clean`  | Cleaned + flattened CDC, dimension joins |
| Gold   | `iceberg.gold.city_revenue_hourly`   | Revenue KPI by city (hourly)    |
| Gold   | `iceberg.gold.category_revenue_hourly` | Revenue KPI by category (hourly) |
| Gold   | `iceberg.gold.product_features_daily` | Product popularity features (daily) |

### ClickHouse (Serving – Real-Time)

| Table / MV                     | Engine              | Description                     |
|--------------------------------|---------------------|---------------------------------|
| `order_events`                 | ReplacingMergeTree  | Raw CDC events (upsert by id)   |
| `revenue_per_minute_mv`        | SummingMergeTree    | Revenue per minute              |
| `order_status_mv`              | AggregatingMergeTree| Real-time order status          |

## Configuration

| Variable               | Default                          | Description                        |
|------------------------|----------------------------------|------------------------------------|
| `PG_HOST`              | localhost                        | PostgreSQL host                    |
| `PG_DB`                | ecommerce                        | Database name                      |
| `KAFKA_BOOTSTRAP`      | kafka:9092                       | Kafka bootstrap server             |
| `KAFKA_TOPIC`          | pg.public.orders                 | Debezium topic for orders          |
| `MINIO_ENDPOINT`       | http://minio:9000                | MinIO S3 endpoint                  |
| `ICEBERG_WAREHOUSE`    | s3a://iceberg-datawarehouse      | Iceberg warehouse location         |
| `CLICKHOUSE_URL`       | jdbc:clickhouse://clickhouse:8123/ecommerce | ClickHouse JDBC    |

##  Testing CDC Pipeline End-to-End

```bash
# 1. Check the initial data
docker compose exec postgres psql -U admin -d ecommerce -c "SELECT count(*) FROM orders;"

# 2. Start the traffic generator
python scripts/mock_data.py --interval 1.0 &

# 3. Check the Kafka topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pg.public.orders \
  --max-messages 2

# 4. Check ClickHouse
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(), status FROM order_events GROUP BY status"

# 5. Check Iceberg via Trino
docker compose exec trino trino \
  --execute "SELECT count(*) FROM iceberg.silver.orders_clean"
```


```bash
# 3. Verify the Kafka topic is receiving events
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pg.public.orders \
  --from-beginning --max-messages 3

# 4. Check that ClickHouse has received data
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(), status FROM ecommerce.order_events GROUP BY status"
```

##  Notes

- **Debezium Raw Envelope:** The connector sends the raw CDC envelope (including `before`, `after`, `op`) so that Flink handles the CDC logic itself.
- **Flink Checkpointing:** Enabled every 10 seconds to guarantee exactly-once semantics and fault-tolerance.
- **Kafka Connector Jar:** You need to download `flink-sql-connector-kafka` into `lib/` on both the JobManager and the TaskManager.
- **ClickHouse Sink:** Uses the HTTP interface to insert, avoiding a heavy JDBC driver dependency.
- **Legacy Code:** Folders `notebooks/`, `scraper/`, `spark_jobs/`, `data/` contain old code; keep them for reference or delete them.

##  Next Steps

Check the TODO roadmap in `TODO.md` and start from Sprint 1.
