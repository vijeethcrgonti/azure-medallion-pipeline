"""
ingest_orders.py
Pulls order records from PostgreSQL and lands them into ADLS Gen2 Bronze container.
Triggered by Azure Data Factory on daily schedule.
"""

import argparse
import logging
import os

import pandas as pd
import psycopg2
from azure.storage.filedatalake import DataLakeServiceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_pg_connection(config: dict):
    return psycopg2.connect(
        host=config["pg_host"],
        port=config["pg_port"],
        dbname=config["pg_db"],
        user=config["pg_user"],
        password=config["pg_password"],
    )


def extract_orders(conn, date: str) -> pd.DataFrame:
    query = """
        SELECT
            order_id,
            customer_id,
            store_id,
            product_id,
            quantity,
            unit_price,
            order_amount,
            order_date,
            status,
            created_at
        FROM orders
        WHERE DATE(order_date) = %(date)s
    """
    logger.info(f"Extracting orders for date: {date}")
    df = pd.read_sql(query, conn, params={"date": date})
    logger.info(f"Extracted {len(df)} records")
    return df


def upload_to_adls(df: pd.DataFrame, account_name: str, container: str, date: str):
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=None,  # DefaultAzureCredential (managed identity) in prod
    )
    fs_client = service_client.get_file_system_client(container)
    partition_path = f"bronze/orders/year={date[:4]}/month={date[5:7]}/day={date[8:10]}"
    file_name = f"orders_{date.replace('-', '')}.parquet"
    file_path = f"{partition_path}/{file_name}"

    file_client = fs_client.get_file_client(file_path)
    file_client.upload_data(df.to_parquet(index=False), overwrite=True)
    logger.info(f"Uploaded {len(df)} records -> {file_path}")
    return file_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    config = {
        "pg_host": os.environ["PG_HOST"],
        "pg_port": os.environ.get("PG_PORT", 5432),
        "pg_db": os.environ["PG_DB"],
        "pg_user": os.environ["PG_USER"],
        "pg_password": os.environ["PG_PASSWORD"],
    }

    conn = get_pg_connection(config)
    try:
        df = extract_orders(conn, args.date)
        upload_to_adls(df, os.environ["ADLS_ACCOUNT_NAME"], os.environ["ADLS_CONTAINER_NAME"], args.date)
        logger.info("Ingestion complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
