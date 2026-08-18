#!/usr/bin/env bash
# ============================================================
# register_cdc.sh – Đăng ký Debezium PostgreSQL Connector
# Tối ưu hóa đặc biệt cho Apache Flink (Raw Debezium Payload)
# ============================================================

set -euo pipefail

# Nạp biến môi trường từ .env (chạy từ thư mục gốc dự án)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "${SCRIPT_DIR}/../.env" ]; then
    set -a
    . "${SCRIPT_DIR}/../.env"
    set +a
fi

DEBEZIUM_URL="${DEBEZIUM_URL:-http://localhost:8083}"

echo " Đang chờ Debezium Connect (Port 8083) khởi động..."
until curl -s "${DEBEZIUM_URL}/connectors" > /dev/null 2>&1; do
    sleep 2
done
echo " Debezium Connect đã sẵn sàng!"

# Xoá connector cũ nếu tồn tại
curl -s -X DELETE "${DEBEZIUM_URL}/connectors/ecommerce-pg-connector" > /dev/null 2>&1 || true

echo "Đang đăng ký PostgreSQL CDC connector cho Flink..."

PAYLOAD=$(cat <<EOF
{
  "name": "ecommerce-pg-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "${POSTGRES_USER}",
    "database.password": "${POSTGRES_PASSWORD}",
    "database.dbname": "${POSTGRES_DB}",
    "database.server.name": "pg",
    "topic.prefix": "pg",
    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "slot.name": "flink_debezium_slot",
    "publication.autocreate.mode": "filtered",
    "decimal.handling.mode": "double",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
EOF
)

curl -s -X POST "${DEBEZIUM_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}" | python3 -m json.tool

echo ""
echo "Cấu hình hoàn tất! Debezium đang đẩy Raw CDC Envelope vào Kafka."