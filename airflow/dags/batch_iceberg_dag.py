from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Jar Kafka connector + Postgres JDBC đã bake sẵn trong image spark
# (Dockerfile.spark) nên KHÔNG dùng `--packages`: nếu dùng, các task chạy song song
# sẽ cùng resolve vào một Ivy cache dùng chung và race nhau.
EXEC = ('docker exec spark-master /opt/spark/bin/spark-submit '
        '--master spark://spark-master:7077 /opt/spark/jobs/')

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_iceberg_pipeline",
    description="Bronze -> Silver -> Gold, Iceberg lakehouse batch pipeline (spark-master)",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,          
    max_active_runs=1,      
    tags=["spark", "iceberg", "lakehouse", "airflow"],
) as dag:

    # ------------------------------------------------------------------
    # 1. Bronze ingestion (Kafka CDC -> Bronze) – độc lập với dims
    # ------------------------------------------------------------------
    bronze_orders = BashOperator(
        task_id="bronze_orders",
        bash_command=f"{EXEC}bronze_ingestion.py",
        execution_timeout=timedelta(minutes=15),
    )

    # ------------------------------------------------------------------
    # 2. Silver dimensions (Postgres -> dim_*) – độc lập với bronze
    # ------------------------------------------------------------------
    dim_snapshot = BashOperator(
        task_id="dim_snapshot",
        bash_command=f"{EXEC}dim_snapshot.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ------------------------------------------------------------------
    # 3. Silver orders (Bronze -> orders_clean, join dims)
    # ------------------------------------------------------------------
    silver_orders = BashOperator(
        task_id="silver_orders",
        bash_command=f"{EXEC}silver_transform.py",
        execution_timeout=timedelta(minutes=30),
    )

    # ------------------------------------------------------------------
    # 4. Gold aggregation (Silver -> Gold)
    # ------------------------------------------------------------------
    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command=f"{EXEC}gold_aggregation.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ------------------------------------------------------------------
    # Thứ tự phụ thuộc
    # ------------------------------------------------------------------
    [bronze_orders, dim_snapshot] >> silver_orders >> gold_aggregation