"""
clean_orders.py
Reads Bronze Delta table, applies data quality rules, deduplication,
PII masking, and writes cleansed records to Silver Delta layer.
"""

import argparse
import hashlib
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-orders-cleaner")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, delta_path: str, date: str) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(delta_path)
        .filter(F.col("_partition_date") == date)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the most recent record per order_id."""
    window = Window.partitionBy("order_id").orderBy(F.col("created_at").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def apply_quality_filters(df: DataFrame) -> DataFrame:
    """Drop records that violate hard data quality rules."""
    before = df.count()
    df_clean = (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_amount") > 0)
        .filter(F.col("order_date").isNotNull())
    )
    after = df_clean.count()
    logger.warning(f"Quality filter dropped {before - after} records")
    return df_clean


def mask_pii(df: DataFrame) -> DataFrame:
    """SHA-256 hash customer_id for PII compliance."""
    mask_udf = F.udf(lambda val: hashlib.sha256(val.encode()).hexdigest() if val else None)
    return df.withColumn("customer_id_masked", mask_udf(F.col("customer_id"))).drop("customer_id")


def standardize(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("order_amount", F.round(F.col("order_amount"), 2))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
    )


def write_silver(df: DataFrame, delta_path: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_partition_date = '{df.first()['_partition_date']}'")
        .partitionBy("order_year", "order_month")
        .save(delta_path)
    )
    logger.info(f"Silver write complete: {df.count()} records")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    spark = get_spark()
    account = os.environ["ADLS_ACCOUNT_NAME"]
    container = os.environ["ADLS_CONTAINER_NAME"]
    base = f"abfss://{container}@{account}.dfs.core.windows.net"

    df = read_bronze(spark, f"{base}/delta/bronze/orders", args.date)
    df = deduplicate(df)
    df = apply_quality_filters(df)
    df = mask_pii(df)
    df = standardize(df)
    write_silver(df, f"{base}/delta/silver/orders")


if __name__ == "__main__":
    main()
