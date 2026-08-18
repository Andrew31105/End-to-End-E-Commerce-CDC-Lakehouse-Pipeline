# 🗺 Implementation Roadmap – Feature Store & Lakehouse

## Phase 1 – Infrastructure (COMPLETED ✅)

- [x] Docker Compose với 16 services (PostgreSQL, Kafka, Debezium, MinIO, Spark, Trino, Flink, ClickHouse, Airflow, Metabase)
- [x] MinIO buckets auto-init (bronze, silver, gold, iceberg-warehouse)
- [x] PostgreSQL schema + seed data + WAL logical decoding
- [x] Debezium PostgreSQL CDC connector registration script
- [x] ClickHouse serving tables + Materialized Views
- [x] Spark + Iceberg + MinIO configuration
- [x] Trino Iceberg catalog configuration

## Phase 2 – CDC Streaming (COMPLETED ✅)

- [x] Mock data generator (Python Faker: INSERT + UPDATE)
- [x] Kafka topic auto-creation
- [x] Flink streaming ETL job skeleton (Kafka → ClickHouse)

## Phase 3 – Batch Lakehouse (TODO 🔲)

- [ ] **Bronze Ingestion**: Spark job đọc CDC events từ Kafka, ghi raw Parquet vào MinIO Bronze
  - [ ] Parse Debezium envelope (before/after/op)
  - [ ] Add ingestion timestamp, batch_id
  - [ ] Schema evolution handling
- [ ] **Silver Transform**: Spark job đọc từ MinIO Bronze, làm sạch, ghi Iceberg Silver
  - [ ] Flatten CDC records (lấy 'after' payload)
  - [ ] Dedup theo primary key
  - [ ] Join với dimension tables (products, users, categories)
  - [ ] Data quality checks (null ratio, row count, duplicate count)
- [ ] **Gold Aggregation**: Spark job aggregate từ Iceberg Silver → Iceberg Gold
  - [ ] City-level KPIs (avg/median price, order volume)
  - [ ] Category/brand-level KPIs (market share, price trends)
  - [ ] Product popularity features (7d, 30d windows)
  - [ ] Trending score calculation

## Phase 4 – Real-Time Features (TODO 🔲)

- [ ] **Flink Window Aggregation (1-min)**: Tumbling window → ClickHouse
  - [ ] Revenue per minute × category
  - [ ] Order status count per minute
  - [ ] High-value order detection ($ > threshold)
- [ ] **Flink JDBC Sink**: Tối ưu buffer flush cho ClickHouse
- [ ] Flink checkpointing & savepoint configuration

## Phase 5 – Feature Store API (TODO 🔲)

- [ ] FastAPI service với endpoints:
  - [ ] `GET /features/{feature_group}?entity_id=...` – Online feature serving
  - [ ] `POST /features/ingest` – Manual feature ingestion
  - [ ] `GET /health` – Health check
- [ ] ClickHouse connector for real-time features (<100ms)
- [ ] Trino connector for batch features
- [ ] Feature metadata registry (config-driven)

## Phase 6 – Orchestration (TODO 🔲)

- [ ] Airflow DAG: `feature_store_pipeline` (5-min schedule)
  - [ ] Task: Bronze ingestion (Spark)
  - [ ] Task: Silver transform (Spark)
  - [ ] Task: Gold aggregation (Spark)
  - [ ] Task: Flink serving trigger
- [ ] Airflow DAG: `batch_iceberg_full_refresh` (daily)
- [ ] DAG dependencies & retry logic
- [ ] Slack/Email alerting on failure

## Phase 7 – BI & Monitoring (TODO 🔲)

- [ ] Metabase dashboards:
  - [ ] Real-time revenue dashboard (ClickHouse)
  - [ ] Historical trends dashboard (Trino → Iceberg)
  - [ ] Product performance dashboard
- [ ] Pipeline monitoring:
  - [ ] Row count checks per layer
  - [ ] Latency monitoring (CDC → Serving)
  - [ ] Data freshness alerts

## Phase 8 – Production Hardening (TODO 🔲)

- [ ] MinIO retention policies (Bronze TTL)
- [ ] Iceberg table maintenance (compaction, expire snapshots)
- [ ] Spark dynamic resource allocation
- [ ] Flink HA configuration
- [ ] Security: secrets management, network isolation
- [ ] Load testing & performance benchmarks

