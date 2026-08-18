-- ============================================================
-- Trino Views – Query Layer trên Iceberg
-- Chạy qua Trino CLI hoặc JDBC sau khi infrastructure up
-- ============================================================

-- View: Doanh thu real-time theo thành phố (1 giờ gần nhất)
CREATE OR REPLACE VIEW iceberg.views.city_revenue_latest AS
SELECT
    city,
    SUM(total_revenue) AS total_revenue_1h,
    SUM(order_count) AS total_orders_1h,
    AVG(avg_order_value) AS avg_order_value_1h
FROM iceberg.gold.city_revenue_hourly
WHERE window_start >= DATE_TRUNC('hour', CURRENT_TIMESTAMP - INTERVAL '1' HOUR)
GROUP BY city;

-- View: Top 10 sản phẩm trending
CREATE OR REPLACE VIEW iceberg.views.top_trending_products AS
SELECT
    product_id,
    product_name,
    category_id,
    order_count_7d,
    revenue_7d,
    trending_score
FROM iceberg.gold.product_features_daily
WHERE snapshot_date = CURRENT_DATE
ORDER BY trending_score DESC
LIMIT 10;

-- View: Doanh thu theo danh mục (daily)
CREATE OR REPLACE VIEW iceberg.views.category_daily_revenue AS
SELECT
    DATE(window_start) AS report_date,
    category_name,
    SUM(total_revenue) AS daily_revenue,
    SUM(order_count) AS daily_orders
FROM iceberg.gold.category_revenue_hourly
WHERE DATE(window_start) = CURRENT_DATE
GROUP BY DATE(window_start), category_name
ORDER BY daily_revenue DESC;
