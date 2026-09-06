#!/usr/bin/env bash
# ============================================================
# ensure_bi_views.sh – Tạo lại schema + BI views cho Superset
# ------------------------------------------------------------
# DÙNG SAU MỖI LẦN `docker compose down -v` (hoặc khi dashboard
# Superset báo "Trino Error" / "Schema views does not exist").
#
# Lý do: batch Spark chỉ tạo bảng bronze/silver/gold, KHÔNG tạo
# schema iceberg.views (6 BI view) – cái đó nằm trong sql/trino_views.sql
# và phải chạy tay (hoặc script này) sau khi stack khởi động lại.
#
# Cách dùng (từ thư mục gốc repo):
#   bash scripts/ensure_bi_views.sh
# ============================================================
set -euo pipefail

echo "[BI] Đảm bảo schema iceberg.views ..."
docker compose exec -T trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.views"

echo "[BI] Tạo/Cập nhật 6 BI views từ sql/trino_views.sql ..."
docker compose exec -T trino trino < sql/trino_views.sql

echo "[BI] Xong. Kiểm tra nhanh:"
docker compose exec -T trino trino --execute "SHOW TABLES FROM iceberg.views"
