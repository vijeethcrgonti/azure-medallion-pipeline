"""
test_silver_transforms.py
Unit tests for Silver layer transformation logic using PySpark local mode.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from silver.clean_orders import apply_quality_filters, deduplicate, mask_pii, standardize


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-silver-transforms")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_orders(spark):
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_amount", DecimalType(12, 2)),
        StructField("order_date", TimestampType()),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
    ])
    data = [
        ("ORD-001", "CUST-A", 150.00, "2024-01-15 10:00:00", "completed", "2024-01-15 10:00:00"),
        ("ORD-001", "CUST-A", 150.00, "2024-01-15 10:00:00", "completed", "2024-01-15 10:05:00"),  # duplicate
        ("ORD-002", None,     200.00, "2024-01-15 11:00:00", "pending",   "2024-01-15 11:00:00"),  # null customer
        ("ORD-003", "CUST-B", -10.00, "2024-01-15 12:00:00", "refunded",  "2024-01-15 12:00:00"),  # negative amount
        ("ORD-004", "CUST-C", 300.00, "2024-01-15 13:00:00", "PENDING",   "2024-01-15 13:00:00"),
    ]
    return spark.createDataFrame(data, schema)


class TestDeduplicate:
    def test_removes_duplicate_order_ids(self, sample_orders):
        result = deduplicate(sample_orders)
        order_ids = [row["order_id"] for row in result.collect()]
        assert len(order_ids) == len(set(order_ids)), "Duplicate order_ids remain after dedup"

    def test_keeps_latest_record(self, spark):
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("created_at", TimestampType()),
            StructField("status", StringType()),
        ])
        data = [
            ("ORD-X", "2024-01-01 08:00:00", "PENDING"),
            ("ORD-X", "2024-01-01 09:00:00", "COMPLETED"),
        ]
        df = spark.createDataFrame(data, schema)
        result = deduplicate(df).collect()
        assert len(result) == 1
        assert result[0]["status"] == "COMPLETED"


class TestQualityFilters:
    def test_drops_null_customer_id(self, sample_orders):
        deduped = deduplicate(sample_orders)
        result = apply_quality_filters(deduped)
        null_count = result.filter(F.col("customer_id").isNull()).count()
        assert null_count == 0

    def test_drops_negative_order_amount(self, sample_orders):
        deduped = deduplicate(sample_orders)
        result = apply_quality_filters(deduped)
        neg_count = result.filter(F.col("order_amount") <= 0).count()
        assert neg_count == 0


class TestMaskPii:
    def test_customer_id_removed(self, sample_orders):
        result = mask_pii(sample_orders)
        assert "customer_id" not in result.columns

    def test_masked_column_exists(self, sample_orders):
        result = mask_pii(sample_orders)
        assert "customer_id_masked" in result.columns

    def test_mask_is_deterministic(self, sample_orders):
        result = mask_pii(sample_orders)
        masks = {row["customer_id_masked"] for row in result.filter("order_id = 'ORD-001'").collect()}
        assert len(masks) == 1


class TestStandardize:
    def test_status_uppercased(self, sample_orders):
        result = standardize(sample_orders)
        for row in result.collect():
            assert row["status"] == row["status"].upper()

    def test_year_month_columns_added(self, sample_orders):
        result = standardize(sample_orders)
        assert "order_year" in result.columns
        assert "order_month" in result.columns
