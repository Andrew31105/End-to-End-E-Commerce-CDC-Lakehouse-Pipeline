from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

EXEC = ('docker exec spark-master /opt/spark/bin/spark-submit '
        '--conf spark.jars.ivy=/tmp/.ivy2 '
        '--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,'
        'org.postgresql:postgresql:42.7.4" '
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
    # 2b. Bronze CDC dimension (users/categories/products)
    #     Chuẩn bị cho phase B: dim_scd2 đọc từ Bronze thay vì JDBC.
    # ------------------------------------------------------------------
    bronze_dims = BashOperator(
        task_id="bronze_dims",
        bash_command=f"{EXEC}dim_history_bronze.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ------------------------------------------------------------------
    # 2c. SCD Type 2 cho dimension (Postgres snapshot -> dim_*_hist)
    # ------------------------------------------------------------------
    dim_scd2 = BashOperator(
        task_id="dim_scd2",
        bash_command=f"{EXEC}dim_scd2.py",
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
    [bronze_orders, bronze_dims, dim_snapshot, dim_scd2] >> silver_orders \
        >> gold_aggregation