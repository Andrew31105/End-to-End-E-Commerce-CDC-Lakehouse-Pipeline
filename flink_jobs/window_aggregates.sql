-- 1. SOURCE TABLE (Kafka CDC raw envelope -> append-only)
--    NOTE: đọc envelope Debezium bằng format 'json' thuần (cột after/op) THAY VÌ
--    'debezium-json'. Lý do: debezium-json + PRIMARY KEY sinh changelog upsert,
--    mà Flink KHÔNG gắn watermark cho nguồn upsert -> window event-time không bao
--    giờ fire (watermark giữ -inf). Đọc append-only (không PK) thì watermark chạy.
CREATE TABLE IF NOT EXISTS kafka_orders (
    after  ROW<id BIGINT, user_id BIGINT, product_id BIGINT, quantity INT,
              unit_price DOUBLE, total_amount DOUBLE, status STRING,
              ordered_at STRING, updated_at STRING>,
    before ROW<id BIGINT, user_id BIGINT, product_id BIGINT, quantity INT,
              unit_price DOUBLE, total_amount DOUBLE, status STRING,
              ordered_at STRING, updated_at STRING>,
    op STRING,
    -- Processing time. NOTE: event-time window (computed col từ nested ROW) không
    -- fire được với nguồn json (watermark giữ -inf) nên dùng PROCTIME() cho chắc chắn,
    -- khớp semantics ClickHouse serving (gom theo processed_at/ingestion).
    pt AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
    'properties.group.id' = 'flink_window_metrics_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- 2. SINK TABLE (Kafka -> ClickHouse Kafka engine tiêu thụ, xem sql/clickhouse/init.sql)
--    NOTE: flink-connector-jdbc KHÔNG hỗ trợ ClickHouse (không có dialect) nên sink
--    qua Kafka, ClickHouse dùng bảng Kafka engine + MV để đổ vào realtime_metrics_1min
CREATE TABLE IF NOT EXISTS clickhouse_realtime_metrics_1min (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),

    -- Volume & Revenue Metrics
    total_orders BIGINT,
    total_products_sold BIGINT,
    total_revenue DOUBLE,
    avg_order_value DOUBLE,
    max_order_value DOUBLE,
    min_order_value DOUBLE,

    -- Status Breakdown Metrics
    pending_orders_count BIGINT,
    confirmed_orders_count BIGINT,
    successful_orders_count BIGINT,
    cancelled_orders_count BIGINT,

    -- Business Feature Flags
    high_value_orders_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink.metrics_1min',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
    'properties.group.id' = 'flink_window_metrics_producer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 3. SINK PER-PRODUCT (order_features: entity = product_id) -> Kafka flink.product_metrics_1min
--    ClickHouse tiêu thụ qua product_metrics_1min_kafka + MV (xem sql/clickhouse/init.sql)
CREATE TABLE IF NOT EXISTS product_metrics_sink (
    product_id BIGINT,
    window_start TIMESTAMP(3),
    total_orders_1min BIGINT,
    total_revenue_1min DOUBLE,
    avg_order_value_1min DOUBLE,
    status_changed_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink.product_metrics_1min',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
    'properties.group.id' = 'flink_product_metrics_producer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 3. TRANSFORMATION (Tumbling Window 1 phút – processing time)
INSERT INTO clickhouse_realtime_metrics_1min
SELECT
    TUMBLE_START(pt, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(pt, INTERVAL '1' MINUTE) AS window_end,

    -- Volume & Revenue Metrics
    COUNT(after.id) AS total_orders,
    SUM(after.quantity) AS total_products_sold,
    SUM(after.total_amount) AS total_revenue,
    AVG(after.total_amount) AS avg_order_value,
    MAX(after.total_amount) AS max_order_value,
    MIN(after.total_amount) AS min_order_value,

    -- Status Breakdown Metrics (Conditional Sums)
    SUM(CASE WHEN UPPER(TRIM(after.status)) = 'PENDING' THEN 1 ELSE 0 END) AS pending_orders_count,
    SUM(CASE WHEN UPPER(TRIM(after.status)) = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_orders_count,
    SUM(CASE WHEN UPPER(TRIM(after.status)) IN ('SHIPPED', 'DELIVERED') THEN 1 ELSE 0 END) AS successful_orders_count,
    SUM(CASE WHEN UPPER(TRIM(after.status)) = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders_count,

    -- High-Value Order Flag (Đơn >= 20 triệu VNĐ)
    SUM(CASE WHEN after.total_amount >= 20000000.0 THEN 1 ELSE 0 END) AS high_value_orders_count

FROM kafka_orders
WHERE after IS NOT NULL AND op <> 'd'
  AND after.total_amount > 0 AND after.quantity > 0
GROUP BY
    TUMBLE(pt, INTERVAL '1' MINUTE);

-- 4. TRANSFORMATION – PER-PRODUCT (Tumbling 1 phút, group theo product_id)
--    total_orders/revenue tính trên event TẠO ĐƠN (op='c'); loại cancelled/returned;
--    status_changed_count = số event update mà status thực sự đổi (before.status <> after.status)
INSERT INTO product_metrics_sink
SELECT
    after.product_id                                        AS product_id,
    TUMBLE_START(pt, INTERVAL '1' MINUTE)                   AS window_start,
    SUM(CASE WHEN op = 'c' THEN 1 ELSE 0 END)               AS total_orders_1min,
    COALESCE(SUM(CASE WHEN op = 'c'
                      AND UPPER(TRIM(after.status)) NOT IN ('CANCELLED','RETURNED')
                      THEN after.total_amount ELSE 0 END), 0) AS total_revenue_1min,
    COALESCE(SUM(CASE WHEN op = 'c'
                      AND UPPER(TRIM(after.status)) NOT IN ('CANCELLED','RETURNED')
                      THEN after.total_amount ELSE 0 END), 0)
        / NULLIF(SUM(CASE WHEN op = 'c' THEN 1 ELSE 0 END), 0) AS avg_order_value_1min,
    SUM(CASE WHEN op = 'u' AND before IS NOT NULL
              AND before.status <> after.status
             THEN 1 ELSE 0 END)                              AS status_changed_count
FROM kafka_orders
WHERE after IS NOT NULL AND op <> 'd' AND after.product_id IS NOT NULL
GROUP BY after.product_id, TUMBLE(pt, INTERVAL '1' MINUTE);