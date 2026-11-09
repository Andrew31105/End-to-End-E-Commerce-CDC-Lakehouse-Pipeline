"""Entry point for incremental Bronze CDC SCD Type 2 dimensions.

Run with::

    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark/jobs/dim_scd2.py

The implementation lives in ``spark_jobs/scd2`` so this stable entry point can
remain referenced by Airflow while the engine stays independently testable.
"""
from __future__ import annotations

import argparse
import os
import sys

JOBS_ROOT = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(JOBS_ROOT, ".."))
for path in (PROJECT_ROOT, JOBS_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

from feature_store.config import get_spark_session
from scd2.config import DIMENSIONS
from scd2.engine import apply_cdc_scd2


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze CDC to Silver SCD2")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read/normalize pending Bronze events without MERGE or watermark",
    )
    args = parser.parse_args()

    spark = get_spark_session("Silver SCD2 Dimensions")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        for spec in DIMENSIONS:
            apply_cdc_scd2(spark, spec, dry_run=args.dry_run)
        mode = "dry-run" if args.dry_run else "apply"
        print(f"[SCD2 CDC] Completed (mode={mode}).")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
