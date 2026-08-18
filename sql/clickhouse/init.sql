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

-- (2) Revenue per minute × category (Materialized View)
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.revenue_per_minute_mv
ENGINE = SummingMergeTree()
ORDER BY (window_start, category_id)
AS SELECT
    toStartOfMinute(processed_at) AS window_start,
    0 AS category_id,  -- TODO: join với product dimension
    sum(total_amount) AS total_revenue,
    count() AS order_count, 
    avg(total_amount) AS avg_order_value
FROM ecommerce.order_events
WHERE status NOT IN ('cancelled', 'returned')
GROUP BY window_start;

-- (3) Order status real-time count
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.order_status_mv
ENGINE = AggregatingMergeTree()
ORDER BY (window_start, status)
AS SELECT
    toStartOfMinute(processed_at) AS window_start,
    status,
    count() AS status_count
FROM ecommerce.order_events
GROUP BY window_start, status;
