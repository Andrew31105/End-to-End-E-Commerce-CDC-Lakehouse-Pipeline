"""Shared pytest fixtures for local Spark unit tests."""
from __future__ import annotations

import os
import sys
import time

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPARK_JOBS_ROOT = os.path.join(PROJECT_ROOT, "spark_jobs")
for path in (PROJECT_ROOT, SPARK_JOBS_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A small local Spark session; no Kafka, Iceberg or Docker required."""
    # PySpark serializes naive Python datetimes with the process timezone.
    # Match production's UTC Spark session so boundary values (especially the
    # SCD2 9999-12-31 sentinel) are deterministic on every developer machine.
    os.environ["TZ"] = "UTC"
    time.tzset()
    session = (
        SparkSession.builder.master("local[2]")
        .appName("second-project-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
