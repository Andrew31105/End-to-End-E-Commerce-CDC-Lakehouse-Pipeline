

-- ============================================================
-- PATTERN A – KPI snapshot (card dashboard "hiện tại")
-- ============================================================

-- A1: Doanh thu theo thành phố – bucket giờ khép lại gần nhất
--     AOV tính lại = tổng doanh thu / tổng đơn (KHÔNG lấy AVG của avg_order_value)
CREATE OR REPLACE VIEW iceberg.views.city_revenue_latest AS
SELECT
    city,
    SUM(total_revenue) AS total_revenue_1h,
    SUM(order_count)   AS total_orders_1h,
    CASE WHEN SUM(order_count) = 0 THEN 0
         ELSE SUM(total_revenue) / SUM(order_count)
    END                AS avg_order_value_1h
FROM iceberg.gold.city_revenue_hourly
WHERE window_start = (SELECT MAX(window_start) FROM iceberg.gold.city_revenue_hourly)
GROUP BY city;

-- A2: Doanh thu theo danh mục – bucket giờ gần nhất
CREATE OR REPLACE VIEW iceberg.views.category_revenue_latest AS
SELECT
    category_id,
    category_name,
    SUM(total_revenue) AS total_revenue_1h,
    SUM(order_count)   AS total_orders_1h
FROM iceberg.gold.category_revenue_hourly
WHERE window_start = (SELECT MAX(window_start) FROM iceberg.gold.category_revenue_hourly)
GROUP BY category_id, category_name;

-- ============================================================
-- PATTERN B – Daily rollup (biểu đồ xu hướng theo ngày)
-- ============================================================

-- B1: Doanh thu theo thành phố, theo ngày
CREATE OR REPLACE VIEW iceberg.views.city_daily_revenue AS
SELECT
    CAST(window_start AS DATE) AS report_date,
    city,
    SUM(total_revenue) AS daily_revenue,
    SUM(order_count)   AS daily_orders,
    CASE WHEN SUM(order_count) = 0 THEN 0
         ELSE SUM(total_revenue) / SUM(order_count)
    END                AS daily_avg_order_value
FROM iceberg.gold.city_revenue_hourly
GROUP BY CAST(window_start AS DATE), city;

-- B2: Doanh thu theo danh mục, theo ngày
CREATE OR REPLACE VIEW iceberg.views.category_daily_revenue AS
SELECT
    CAST(window_start AS DATE) AS report_date,
    category_id,
    category_name,
    SUM(total_revenue) AS daily_revenue,
    SUM(order_count)   AS daily_orders
FROM iceberg.gold.category_revenue_hourly
GROUP BY CAST(window_start AS DATE), category_id, category_name;

-- B3: Tổng doanh thu toàn hệ thống theo ngày (1 dòng/ngày – headline + trend)
CREATE OR REPLACE VIEW iceberg.views.daily_revenue_summary AS
SELECT
    CAST(window_start AS DATE) AS report_date,
    SUM(total_revenue) AS total_revenue,
    SUM(order_count)   AS total_orders
FROM iceberg.gold.city_revenue_hourly
GROUP BY CAST(window_start AS DATE);

-- ============================================================
-- PATTERN C – Ranking sản phẩm
-- ============================================================

-- C1: Top 10 sản phẩm trending theo snapshot mới nhất
CREATE OR REPLACE VIEW iceberg.views.top_trending_products AS
SELECT
    product_id,
    product_name,
    category_id,
    order_count_1d,
    order_count_7d,
    order_count_30d,
    revenue_1d,
    revenue_7d,
    revenue_30d,
    trending_score
FROM iceberg.gold.product_features_daily
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM iceberg.gold.product_features_daily)
ORDER BY trending_score DESC
LIMIT 10;
