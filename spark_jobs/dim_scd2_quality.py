"""CLI for read-only data-quality checks on real SCD2 Iceberg tables.

Run all dimensions with::

    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/dim_scd2_quality.py

The job never writes to dimension tables or advances CDC watermarks.
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
from scd2.quality import run_scd2_quality_checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only quality checks for SCD2 Iceberg dimensions"
    )
    parser.add_argument(
        "--dimension",
        choices=("all", *(spec.name for spec in DIMENSIONS)),
        default="all",
        help="Check one dimension or all configured dimensions",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected_specs = (
        DIMENSIONS
        if args.dimension == "all"
        else tuple(spec for spec in DIMENSIONS if spec.name == args.dimension)
    )

    spark = get_spark_session("SCD2 Data Quality")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        run_scd2_quality_checks(spark, selected_specs)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
