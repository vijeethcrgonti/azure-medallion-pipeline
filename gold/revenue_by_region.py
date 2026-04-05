"""
revenue_by_region.py
Aggregates Silver orders with store dimension to produce daily revenue by region.
Writes to Gold Delta layer, partitioned by order_year/order_month.
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold-revenue-by-region")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_revenue(spark: SparkSession, base_path: str, date: str):
    orders = (
        spark.read.format("delta")
        .load(f"{base_path}/delta/silver/orders")
        .filter(F.col("_partition_date") == date)
    )

    stores = (
        spark.read.format("delta")
        .load(f"{base_path}/delta/silver/stores")
    )

    revenue = (
        orders.join(stores, on="store_id", how="left")
        .groupBy(
            F.col("order_date").cast("date").alias("order_date"),
            F.col("region"),
            F.col("store_id"),
        )
        .agg(
            F.sum("order_amount").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id_masked").alias("unique_customers"),
        )
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("_processed_at", F.current_timestamp())
    )

    logger.info(f"Writing {revenue.count()} region-day rows to Gold")

    (
        revenue.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"order_date = '{date}'")
        .partitionBy("order_year", "order_month")
        .save(f"{base_path}/delta/gold/revenue_by_region")
    )

    logger.info("Gold revenue write complete")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    spark = get_spark()
    account = os.environ["ADLS_ACCOUNT_NAME"]
    container = os.environ["ADLS_CONTAINER_NAME"]
    base = f"abfss://{container}@{account}.dfs.core.windows.net"

    build_revenue(spark, base, args.date)


if __name__ == "__main__":
    main()
