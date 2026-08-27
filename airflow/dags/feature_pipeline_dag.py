# """
# ============================================================================
#  airflow/dags/feature_pipeline_dag.py
#  Airflow DAG – Orchestrate toàn bộ Feature Store Pipeline
# ============================================================================
#    2. Dimension Snapshot (PostgreSQL → Iceberg Silver)
#    3. Silver Transform (MinIO → Iceberg)
#    4. Gold Aggregation (Iceberg → Feature Tables)
#    5. Flink Window Job (Iceberg/Trino → ClickHouse Serving)
#  Lịch chạy: mỗi 5 phút
#  Pipeline:"""
# ============================================================================
#  airflow/dags/feature_pipeline_dag.py
#  Airflow DAG – Orchestrate toàn bộ Feature Store Pipeline
# ============================================================================
#    2. Dimension Snapshot (PostgreSQL → Iceberg Silver)
#    3. Silver Transform (MinIO → Iceberg)
#    4. Gold Aggregation (Iceberg → Feature Tables)
#    5. Flink Window Job (Iceberg/Trino → ClickHouse Serving)
#  Lịch chạy: mỗi 5 phút
#  Pipeline:
# snapshot_task = BashOperator(
#     task_id="dimension_snapshot",
#     bash_command="echo '[Airflow] Running dimension snapshot...'; python /opt/project/spark_jobs/dim_snapshot.py",
#     dag=dag,
# )

# # TODO: Task 3 – Silver Transform (Spark job)
#    1. Bronze Ingestion (Kafka → MinIO)
#    2. Silver Transform (MinIO → Iceberg)
#    3. Gold Aggregation (Iceberg → Feature Tables)
#    4. Flink Window Job (Iceberg/Trino → ClickHouse Serving)
# ============================================================================
#  (PLACEHOLDER – Code triển khai sau)
# # TODO: Task 4 – Gold Aggregation (Spark job)
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator


# # TODO: Task 5 – Trigger Flink window job
#     "owner": "data-team",
#     "depends_on_past": False,
#     "start_date": datetime(2026, 8, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=2),
# }

# [bronze_task, snapshot_task] >> silver_task >> gold_task >> serving_task
#     dag_id="feature_store_pipeline",
#     default_args=default_args,
#     description="Feature Store Lakehouse Pipeline: Bronze → Silver → Gold → Serving",
#     schedule_interval="*/5 * * * *",  # Mỗi 5 phút
#     catchup=False,
#     tags=["feature-store", "lakehouse"],
# )

# # TODO: Task 1 – Bronze Ingestion (Spark job)
# bronze_task = BashOperator(
#     task_id="bronze_ingestion",
#     bash_command="echo '[Airflow] Running Bronze ingestion...'",
#     dag=dag,
# )

# # TODO: Task 2 – Silver Transform (Spark job)
# silver_task = BashOperator(
#     task_id="silver_transform",
#     bash_command="echo '[Airflow] Running Silver transform...'",
#     dag=dag,
# )

# # TODO: Task 3 – Gold Aggregation (Spark job)
# gold_task = BashOperator(
#     task_id="gold_aggregation",
#     bash_command="echo '[Airflow] Running Gold aggregation...'",
#     dag=dag,
# )

# # TODO: Task 4 – Trigger Flink window job
# serving_task = BashOperator(
#     task_id="flink_serving",
#     bash_command="echo '[Airflow] Triggering Flink serving job...'",
#     dag=dag,
# )

# # Pipeline DAG
# bronze_task >> silver_task >> gold_task >> serving_task

# snapshot_task = BashOperator(
#     task_id="dimension_snapshot",
#     bash_command="echo '[Airflow] Running dimension snapshot...'; python /opt/project/spark_jobs/dim_snapshot.py",
#     dag=dag,
# )

# # TODO: Task 3 – Silver Transform (Spark job)
#    1. Bronze Ingestion (Kafka → MinIO)
#    2. Silver Transform (MinIO → Iceberg)
#    3. Gold Aggregation (Iceberg → Feature Tables)
#    4. Flink Window Job (Iceberg/Trino → ClickHouse Serving)
# ============================================================================
#  (PLACEHOLDER – Code triển khai sau)
# # TODO: Task 4 – Gold Aggregation (Spark job)
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator


# # TODO: Task 5 – Trigger Flink window job
#     "owner": "data-team",
#     "depends_on_past": False,
#     "start_date": datetime(2026, 8, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=2),
# }

# [bronze_task, snapshot_task] >> silver_task >> gold_task >> serving_task
#     dag_id="feature_store_pipeline",
#     default_args=default_args,
#     description="Feature Store Lakehouse Pipeline: Bronze → Silver → Gold → Serving",
#     schedule_interval="*/5 * * * *",  # Mỗi 5 phút
#     catchup=False,
#     tags=["feature-store", "lakehouse"],
# )

# # TODO: Task 1 – Bronze Ingestion (Spark job)
# bronze_task = BashOperator(
#     task_id="bronze_ingestion",
#     bash_command="echo '[Airflow] Running Bronze ingestion...'",
#     dag=dag,
# )

# # TODO: Task 2 – Silver Transform (Spark job)
# silver_task = BashOperator(
#     task_id="silver_transform",
#     bash_command="echo '[Airflow] Running Silver transform...'",
#     dag=dag,
# )

# # TODO: Task 3 – Gold Aggregation (Spark job)
# gold_task = BashOperator(
#     task_id="gold_aggregation",
#     bash_command="echo '[Airflow] Running Gold aggregation...'",
#     dag=dag,
# )

# # TODO: Task 4 – Trigger Flink window job
# serving_task = BashOperator(
#     task_id="flink_serving",
#     bash_command="echo '[Airflow] Triggering Flink serving job...'",
#     dag=dag,
# )

# # Pipeline DAG
# bronze_task >> silver_task >> gold_task >> serving_task
