"""
============================================================================
 airflow/dags/batch_iceberg_dag.py
 Airflow DAG – Batch ETL toàn phần vào Iceberg (daily full refresh)
============================================================================
 (PLACEHOLDER – Code triển khai sau)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="batch_iceberg_full_refresh",
    default_args=default_args,
    description="Daily full batch ETL into Iceberg tables",
    schedule_interval="@daily",
    catchup=False,
    tags=["batch", "iceberg"],
)

full_batch = BashOperator(
    task_id="full_batch_etl",
    bash_command="echo '[Airflow] Running full batch ETL...'",
    dag=dag,
)
