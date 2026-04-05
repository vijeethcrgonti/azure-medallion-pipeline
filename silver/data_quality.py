"""
data_quality.py
Great Expectations-based data quality checks for Silver layer tables.
Raises on critical failures; logs warnings for soft checks.
"""

import argparse
import logging
import os
import sys

import great_expectations as ge
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ORDERS_EXPECTATIONS = [
    {"expectation": "expect_column_values_to_not_be_null", "column": "order_id", "critical": True},
    {"expectation": "expect_column_values_to_not_be_null", "column": "customer_id_masked", "critical": True},
    {"expectation": "expect_column_values_to_be_between", "column": "order_amount", "min_value": 0.01, "critical": True},
    {"expectation": "expect_column_values_to_not_be_null", "column": "order_date", "critical": True},
    {"expectation": "expect_column_values_to_be_in_set", "column": "status",
     "value_set": ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"], "critical": False},
    {"expectation": "expect_column_values_to_be_between", "column": "quantity",
     "min_value": 1, "max_value": 10000, "critical": False},
]


def run_checks(spark: SparkSession, delta_path: str, date: str) -> bool:
    df = (
        spark.read.format("delta").load(delta_path)
        .filter(f"_partition_date = '{date}'")
        .toPandas()
    )
    ge_df = ge.from_pandas(df)
    failures = []

    for check in ORDERS_EXPECTATIONS:
        col = check["column"]
        exp = check["expectation"]

        if exp == "expect_column_values_to_not_be_null":
            result = ge_df.expect_column_values_to_not_be_null(col)
        elif exp == "expect_column_values_to_be_between":
            result = ge_df.expect_column_values_to_be_between(
                col, min_value=check.get("min_value"), max_value=check.get("max_value")
            )
        elif exp == "expect_column_values_to_be_in_set":
            result = ge_df.expect_column_values_to_be_in_set(col, check["value_set"])
        else:
            continue

        if not result["success"]:
            msg = f"FAILED [{col}] {exp} — unexpected_count={result['result'].get('unexpected_count')}"
            if check["critical"]:
                logger.error(msg)
                failures.append(msg)
            else:
                logger.warning(msg)
        else:
            logger.info(f"PASSED [{col}] {exp}")

    if failures:
        logger.error(f"{len(failures)} critical checks failed")
        return False

    logger.info("All critical checks passed")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=["silver", "gold"])
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("data-quality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    account = os.environ["ADLS_ACCOUNT_NAME"]
    container = os.environ["ADLS_CONTAINER_NAME"]
    delta_path = f"abfss://{container}@{account}.dfs.core.windows.net/delta/{args.layer}/orders"

    passed = run_checks(spark, delta_path, args.date)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
