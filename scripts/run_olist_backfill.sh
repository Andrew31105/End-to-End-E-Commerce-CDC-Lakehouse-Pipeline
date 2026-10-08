#!/usr/bin/env bash
# ============================================================
# run_olist_backfill.sh – Chạy trọn pipeline nạp dataset Olist
# ============================================================
#   1/5  Dataset/*.csv            → data/olist/prepared/*.parquet  (DuckDB)
#   2/5  dim_*.parquet            → PostgreSQL (categories/products/users)
#   3/5  orders_resolved.parquet  → data/olist/orders_backfill.parquet (~1M)
#   4/5  orders_backfill.parquet  → iceberg.bronze.orders_raw        (Spark)
#   5/5  trigger DAG batch_iceberg_pipeline → Silver + Gold
#
# Cách dùng:
#   ./scripts/run_olist_backfill.sh                       # chạy full 1 triệu đơn
#   SAMPLE=5000 TARGET=20000 ./scripts/run_olist_backfill.sh   # chạy thử nhanh
#   SKIP_DAG=1 ./scripts/run_olist_backfill.sh            # không trigger Airflow
#
# Biến môi trường: DATASET_DIR, PREPARED_DIR, BACKFILL_PARQUET, TARGET, FX,
#                  SAMPLE, SKIP_DAG, PYTHON
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Thư mục mount cho Spark phải tồn tại TRƯỚC docker compose up, nếu không Docker
# sẽ tạo nó với quyền root và bước chuẩn hóa dữ liệu không ghi được Parquet.
mkdir -p data/olist

DATASET_DIR="${DATASET_DIR:-Dataset}"
PREPARED_DIR="${PREPARED_DIR:-data/olist/prepared}"
BACKFILL_PARQUET="${BACKFILL_PARQUET:-data/olist/orders_backfill.parquet}"
TARGET="${TARGET:-1000000}"
FX="${FX:-5000}"
SAMPLE="${SAMPLE:-}"        # SAMPLE=5000 → chỉ lấy 5000 đơn gốc (chạy thử)
SKIP_DAG="${SKIP_DAG:-0}"

# ---- tìm python có duckdb + pandas + psycopg2 ----
PY="${PYTHON:-}"
if [ -z "${PY}" ]; then
    for candidate in "$HOME/anaconda3/envs/crypto/bin/python" "$(command -v python3 || true)"; do
        if [ -n "${candidate}" ] && [ -x "${candidate}" ] \
           && "${candidate}" -c "import duckdb, pandas, psycopg2" 2>/dev/null; then
            PY="${candidate}"
            break
        fi
    done
fi
if [ -z "${PY}" ]; then
    echo "Không tìm thấy Python có duckdb/pandas/psycopg2."
    echo "Gợi ý: conda run -n crypto pip install duckdb  rồi đặt PYTHON=/đường/dẫn/python"
    exit 1
fi
echo "Python: ${PY}"

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# ---------------------------------------------------------------- 1/5
step "1/5  Chuẩn hóa CSV Olist → Parquet"
# shellcheck disable=SC2086
"${PY}" scripts/olist_prepare.py \
    --src "${DATASET_DIR}" \
    --out "${PREPARED_DIR}" \
    --fx "${FX}" \
    ${SAMPLE:+--sample "${SAMPLE}"}

# ---------------------------------------------------------------- 2/5
step "2/5  Nạp dimension vào PostgreSQL"
"${PY}" scripts/olist_load_postgres.py --prepared "${PREPARED_DIR}"

# ---------------------------------------------------------------- 3/5
step "3/5  Nhân bản lên ${TARGET} đơn hàng"
# shellcheck disable=SC2086
"${PY}" scripts/olist_amplify.py \
    --src "${PREPARED_DIR}/orders_resolved.parquet" \
    --out "${BACKFILL_PARQUET}" \
    --target "${TARGET}" \
    ${SAMPLE:+--base-limit "${SAMPLE}"}

# ---------------------------------------------------------------- 4/5
step "4/5  Backfill Parquet → iceberg.bronze.orders_raw"
# Áp dụng volume ./data/olist (thêm trong docker-compose.yml)
docker compose up -d spark-master spark-worker
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/backfill_olist.py

# ---------------------------------------------------------------- 5/5
if [ "${SKIP_DAG}" = "1" ]; then
    step "5/5  Bỏ qua trigger DAG (SKIP_DAG=1)"
else
    step "5/5  Trigger batch_iceberg_pipeline (Silver + Gold)"
    docker exec airflow-scheduler airflow dags unpause batch_iceberg_pipeline || true
    docker exec airflow-scheduler airflow dags trigger batch_iceberg_pipeline
fi

cat <<'EOF'

============================================================
Xong. Kiểm tra kết quả:

  # Trino (http://localhost:8084)
  SELECT batch_id, count(*) FROM iceberg.bronze.orders_raw GROUP BY 1;
  SELECT count(*) FROM iceberg.silver.orders_clean;
  SELECT min(ordered_at), max(ordered_at) FROM iceberg.silver.orders_clean;

  # Backfill chạy xong Silver/Gold cần vài phút (1 triệu dòng trên 1 worker).
  # Theo dõi: http://localhost:8085 (Airflow) | http://localhost:8080 (Spark UI)
============================================================
EOF
