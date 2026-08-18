-- 1. SOURCE TABLE (Kafka CDC + Watermark)
CREATE TABLE IF NOT EXISTS kafka_orders (
    id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DOUBLE,
    total_amount DOUBLE,
    status STRING,
    ordered_at STRING,
    updated_at STRING,
    
    -- Event Time & Watermark (Trễ tối đa 5s)
    event_time AS TO_TIMESTAMP(ordered_at),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
    'properties.group.id' = 'flink_window_metrics_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- 2. SINK TABLE (ClickHouse JDBC - Bổ sung thêm các cột chỉ số)
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
    high_value_orders_count BIGINT,
    
    PRIMARY KEY (window_start, window_end) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}',
    'table-name' = 'realtime_metrics_1min',
    'username' = '{CLICKHOUSE_USER}',
    'password' = '{CLICKHOUSE_PASSWORD}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s'
);

-- 3. TRANSFORMATION (Tumbling Window 1 phút nâng cao)
INSERT INTO clickhouse_realtime_metrics_1min
SELECT 
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    
    -- Volume & Revenue Metrics
    COUNT(id) AS total_orders,
    SUM(quantity) AS total_products_sold,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    MAX(total_amount) AS max_order_value,
    MIN(total_amount) AS min_order_value,
    
    -- Status Breakdown Metrics (Conditional Sums)
    SUM(CASE WHEN UPPER(TRIM(status)) = 'PENDING' THEN 1 ELSE 0 END) AS pending_orders_count,
    SUM(CASE WHEN UPPER(TRIM(status)) = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_orders_count,
    SUM(CASE WHEN UPPER(TRIM(status)) IN ('SHIPPED', 'DELIVERED') THEN 1 ELSE 0 END) AS successful_orders_count,
    SUM(CASE WHEN UPPER(TRIM(status)) = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders_count,
    
    -- High-Value Order Flag (Đơn >= 20 triệu VNĐ)
    SUM(CASE WHEN total_amount >= 20000000.0 THEN 1 ELSE 0 END) AS high_value_orders_count

FROM kafka_orders
WHERE total_amount > 0 AND quantity > 0
GROUP BY 
    TUMBLE(event_time, INTERVAL '1' MINUTE);