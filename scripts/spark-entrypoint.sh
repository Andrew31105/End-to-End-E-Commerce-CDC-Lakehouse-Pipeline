#!/usr/bin/env bash
# ============================================================
# spark-entrypoint.sh – Khởi động Spark Master hoặc Worker
# ============================================================
set -e

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    /opt/spark/sbin/start-master.sh \
        --host "${SPARK_MASTER_HOST:-0.0.0.0}" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker, connecting to ${SPARK_MASTER_URL}..."
    /opt/spark/sbin/start-worker.sh "${SPARK_MASTER_URL}"
else
    echo "SPARK_MODE must be 'master' or 'worker'"
    exit 1
fi

# Giữ container chạy
tail -f /opt/spark/logs/*.out
