-- ============================================================
-- Iceberg DDL – Lakehouse Tables (Bronze / Silver & Gold)
-- Chạy qua Spark SQL hoặc Trino sau khi infrastructure up
-- ============================================================

-- ========================
-- Bronze Layer (Raw CDC)
-- ========================
-- Lưu full CDC envelope từ Kafka (Debezium), giữ _raw_payload gốc.
-- Là Iceberg table để có thể incremental read theo snapshot id.

CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
    before          STRUCT<
                        id: INT,
                        user_id: INT,
                        product_id: INT,
                        quantity: INT,
                        unit_price: DECIMAL(10, 2),
                        total_amount: DECIMAL(10, 2),
                        status: STRING,
                        ordered_at: STRING,
                        updated_at: STRING
                    >,
    after           STRUCT<
                        id: INT,
                        user_id: INT,
                        product_id: INT,    
                        quantity: INT,
                        unit_price: DECIMAL(10, 2),
                        total_amount: DECIMAL(10, 2),
                        status: STRING,
                        ordered_at: STRING,
                        updated_at: STRING
                    >,
    op              STRING,
    debezium_ts_ms  BIGINT,
    kafka_ts        TIMESTAMP,
    _raw_payload    STRING,
    ingestion_ts    TIMESTAMP,
    batch_id        STRING
) USING iceberg
PARTITIONED BY (days(ingestion_ts));

-- ========================
-- Silver Layer (Cleaned)
-- ========================

-- Orders cleaned (flattened CDC + dimension joins)

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_users_hist (
    user_sk       BIGINT,
    user_id       BIGINT,
    full_name     STRING,
    email         STRING,
    phone         STRING,
    city          STRING,
    source_ts     TIMESTAMP,
    valid_from    TIMESTAMP,
    valid_to      TIMESTAMP,
    is_current    BOOLEAN,
    row_hash      STRING,
    cdc_op        STRING,
    ingested_at   TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_categories_hist (
    category_sk   BIGINT,
    category_id   INT,
    category_name STRING,
    description   STRING,
    source_ts     TIMESTAMP,
    valid_from    TIMESTAMP,
    valid_to      TIMESTAMP,
    is_current    BOOLEAN,
    row_hash      STRING,
    cdc_op        STRING,
    ingested_at   TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_products_hist (
    product_sk    BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category_id   INT,
    base_price    DECIMAL(12, 2),
    is_active     BOOLEAN,
    source_ts     TIMESTAMP,
    valid_from    TIMESTAMP,
    valid_to      TIMESTAMP,
    is_current    BOOLEAN,
    row_hash      STRING,
    cdc_op        STRING,
    ingested_at   TIMESTAMP
) USING iceberg;





CREATE TABLE IF NOT EXISTS iceberg.silver.orders_clean (
    order_id        BIGINT,
    user_id         BIGINT,
    product_id      BIGINT,
    product_name    STRING,
    category_id     INT,
    category_name   STRING,
    user_city       STRING,
    quantity        INT,
    unit_price      DECIMAL(12, 2),
    total_amount    DECIMAL(12, 2),
    status          STRING,
    ordered_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    cdc_op          STRING,       -- 'c' = create, 'u' = update, 'd' = delete
    ingested_at     TIMESTAMP,
    product_sk      BIGINT,       -- SCD2: version của product tại ordered_at
    category_sk     BIGINT,       -- SCD2: version của category tại ordered_at
    user_sk         BIGINT,       -- SCD2: version của user tại ordered_at
    dim_match_type  STRING        -- exact | inferred | partial | missing
) USING iceberg
-- months(): 24 tháng lịch sử chỉ tạo ~24 partition. Nếu partition theo NGÀY,
-- backfill tạo ~730 partition và Iceberg mở 1 Parquet writer cho mỗi partition
-- trong cùng 1 task (fanout) -> tràn heap executor.
PARTITIONED BY (months(ordered_at));

-- ========================
-- Gold Layer (Aggregated)
-- ========================

-- Revenue KPI by city (hourly)
CREATE TABLE IF NOT EXISTS iceberg.gold.city_revenue_hourly (
    window_start    TIMESTAMP,
    city            STRING,
    total_revenue   DECIMAL(18, 2),
    order_count     BIGINT,
    avg_order_value DECIMAL(12, 2)
) USING iceberg
PARTITIONED BY (months(window_start));

-- Revenue KPI by category (hourly)
CREATE TABLE IF NOT EXISTS iceberg.gold.category_revenue_hourly (
    window_start    TIMESTAMP,
    category_id     INT,
    category_name   STRING,
    total_revenue   DECIMAL(18, 2),
    order_count     BIGINT
) USING iceberg
PARTITIONED BY (months(window_start));

-- Product popularity features
CREATE TABLE IF NOT EXISTS iceberg.gold.product_features_daily (
    snapshot_date   DATE,
    product_id      BIGINT,
    product_name    STRING,
    category_id     INT,
    order_count_1d  BIGINT,
    order_count_7d  BIGINT,
    order_count_30d BIGINT,
    revenue_1d      DECIMAL(18, 2),
    revenue_7d      DECIMAL(18, 2),
    revenue_30d     DECIMAL(18, 2),
    trending_score  DOUBLE
) USING iceberg
PARTITIONED BY (days(snapshot_date));
