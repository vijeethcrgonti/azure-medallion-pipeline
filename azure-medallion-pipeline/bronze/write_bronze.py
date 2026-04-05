"""
write_bronze.py
Reads raw Parquet files from ADLS landing zone and writes them as Delta Lake tables
in the Bronze layer. Applies minimal transformation — schema coercion only.
"""

import argparse
import logging
import os

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DecimalType(10, 2), nullable=True),
    StructField("order_amount", DecimalType(12, 2), nullable=True),
    StructField("order_date", TimestampType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-writer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def write_bronze_orders(spark: SparkSession, date: str, adls_account: str, container: str):
    source_path = (
        f"abfss://{container}@{adls_account}.dfs.core.windows.net"
        f"/bronze/orders/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
    )
    delta_path = (
        f"abfss://{container}@{adls_account}.dfs.core.windows.net/delta/bronze/orders"
    )

    logger.info(f"Reading raw parquet from: {source_path}")
    df = (
        spark.read
        .schema(ORDERS_SCHEMA)
        .parquet(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_partition_date", F.lit(date).cast("date"))
    )

    logger.info(f"Writing {df.count()} records to Bronze Delta table")

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_partition_date")
        .option("mergeSchema", "false")
        .save(delta_path)
    )

    logger.info("Bronze write complete")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=["orders", "products"])
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark()
    adls_account = os.environ["ADLS_ACCOUNT_NAME"]
    container = os.environ["ADLS_CONTAINER_NAME"]

    if args.source == "orders":
        write_bronze_orders(spark, args.date, adls_account, container)
    else:
        raise NotImplementedError(f"Source not implemented: {args.source}")


if __name__ == "__main__":
    main()
