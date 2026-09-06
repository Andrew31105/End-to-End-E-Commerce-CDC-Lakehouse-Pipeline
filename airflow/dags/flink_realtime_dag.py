"""
============================================================================
 airflow/dags/flink_realtime_dag.py
 Đảm bảo job realtime Flink luôn chạy (ensure-running, self-healing)
============================================================================
 Cơ chế (Airflow không có Flink CLI):
   Task dùng docker CLI (docker.sock host, xem docker-compose airflow-scheduler)
   để exec VÀO container flink-jobmanager:
       docker exec flink-jobmanager /opt/flink/bin/flink ...

 Mỗi chu kỳ:
   1. `flink list` -> nếu ĐÃ có job realtime đang RUNNING  -> bỏ qua (idempotent)
   2. nếu CHƯA có -> submit: `flink run -d --python window_aggregation.py`

 Lịch mỗi 2 phút => tự phục hồi khi job/container bị chết/restart.
 (Job streaming chạy dài hạn, KHÔNG được submit trùng -> bắt buộc phải check trước.)
============================================================================
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}

# Job name Flink sinh ra có chứa tên 2 sink:
#   insert-into_..._clickhouse_realtime_metrics_1min,..._product_metrics_sink
ENSURE_FLINK = r"""
set -e
if docker exec flink-jobmanager /opt/flink/bin/flink list 2>/dev/null \
     | grep "clickhouse_realtime_metrics_1min" | grep -q "RUNNING"; then
  echo "[Flink] job đã đang chạy -> bỏ qua submit"
else
  echo "[Flink] chưa thấy job -> submit window_aggregation.py"
  docker exec flink-jobmanager /opt/flink/bin/flink run -d \
    --python /opt/project/flink_jobs/window_aggregation.py
fi
"""

with DAG(
    dag_id="flink_realtime_ensure_running",
    description="Ensure Flink realtime window job is running (check every 2 min)",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/2 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,          # tránh 2 lần kiểm tra chồng nhau
    tags=["flink", "realtime", "airflow"],
) as dag:
    ensure_flink = BashOperator(
        task_id="ensure_flink_job",
        bash_command=ENSURE_FLINK,
        execution_timeout=timedelta(minutes=5),
    )
