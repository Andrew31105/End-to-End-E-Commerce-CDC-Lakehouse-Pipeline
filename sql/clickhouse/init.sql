-- ============================================================
-- ClickHouse DDL – Serving Tables cho Feature Store
--    Vì entrypoint ClickHouse chạy init.sql trong DB 'default'
--    nếu bảng không được định nghĩa kèm tên database.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce;

-- (1) CDC Events (ReplacingMergeTree – upsert theo primary key)
CREATE TABLE IF NOT EXISTS ecommerce.order_events (
    id              UInt64,
    user_id         UInt64,
    product_id      UInt64,
    quantity        Int32,
    unit_price      Float64,
    total_amount    Float64,
    status          String,
    order_status_clean String DEFAULT '',
    is_high_value   Int32 DEFAULT 0,
    processed_at    DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree()
ORDER BY id;

-- (2) Revenue per minute × category: CHƯA triển khai ở ClickHouse.
--     order_events không có category_id nên không thể tính đúng tại đây;
--     realtime theo product/category do Flink tính (flink_jobs/window_aggregates.sql),
--     batch theo category do Gold (iceberg.gold.category_revenue_hourly) phụ trách.

-- (3) Order status real-time count
--     AggregatingMergeTree BẮT BUỘC dùng hàm -State, nếu dùng count() thường
--     engine sẽ không merge. Đọc bằng: countMerge(status_count).
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.order_status_mv
ENGINE = AggregatingMergeTree()
ORDER BY (window_start, status)
AS SELECT
    toStartOfMinute(processed_at) AS window_start,
    status,
    countState() AS status_count
FROM ecommerce.order_events
GROUP BY window_start, status;

-- (4) Flink 1-min window metrics sink (window_aggregation.py -> Kafka topic,
--     ClickHouse tiêu thụ qua Kafka engine bên dưới)
--     ReplacingMergeTree dedup theo (window_start, window_end)
CREATE TABLE IF NOT EXISTS ecommerce.realtime_metrics_1min (
    window_start DateTime64(3),
    window_end DateTime64(3),
    total_orders UInt64,
    total_products_sold UInt64,
    total_revenue Float64,
    avg_order_value Float64,
    max_order_value Float64,
    min_order_value Float64,
    pending_orders_count UInt64,
    confirmed_orders_count UInt64,
    successful_orders_count UInt64,
    cancelled_orders_count UInt64,
    high_value_orders_count UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (window_start, window_end);

-- (4b) Flink ghi metrics ra Kafka topic 'flink.metrics_1min' (vì flink-connector-jdbc
--      không hỗ trợ ClickHouse). ClickHouse tiêu thụ qua Kafka engine + MV.
CREATE TABLE IF NOT EXISTS ecommerce.realtime_metrics_1min_kafka (
    window_start DateTime64(3),
    window_end DateTime64(3),
    total_orders UInt64,
    total_products_sold UInt64,
    total_revenue Float64,
    avg_order_value Float64,
    max_order_value Float64,
    min_order_value Float64,
    pending_orders_count UInt64,
    confirmed_orders_count UInt64,
    successful_orders_count UInt64,
    cancelled_orders_count UInt64,
    high_value_orders_count UInt64
) ENGINE = Kafka()
SETTINGS kafka_broker_list='kafka:9092',
         kafka_topic_list='flink.metrics_1min',
         kafka_group_name='clickhouse_metrics_1min_grp',
         kafka_format='JSONEachRow',
         kafka_num_consumers=1;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.realtime_metrics_1min_mv
TO ecommerce.realtime_metrics_1min AS
SELECT * FROM ecommerce.realtime_metrics_1min_kafka;

-- (5) Flink per-product 1-min metrics (order_features: entity = product_id)
CREATE TABLE IF NOT EXISTS ecommerce.product_metrics_1min (
    product_id           UInt64,
    window_start         DateTime64(3),
    total_orders_1min    UInt64,
    total_revenue_1min   Float64,
    avg_order_value_1min Float64,
    status_changed_count UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (window_start, product_id);

CREATE TABLE IF NOT EXISTS ecommerce.product_metrics_1min_kafka (
    product_id UInt64, window_start DateTime64(3),
    total_orders_1min UInt64, total_revenue_1min Float64,
    avg_order_value_1min Float64, status_changed_count UInt64
) ENGINE = Kafka()
SETTINGS kafka_broker_list='kafka:9092',
         kafka_topic_list='flink.product_metrics_1min',
         kafka_group_name='clickhouse_product_metrics_grp',
         kafka_format='JSONEachRow',
         kafka_num_consumers=1;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.product_metrics_1min_mv
TO ecommerce.product_metrics_1min AS
SELECT * FROM ecommerce.product_metrics_1min_kafka;
