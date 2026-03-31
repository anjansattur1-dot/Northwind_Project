import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-northwind") \
        .master("local[*]") \
        .getOrCreate()

def test_clean_orders_casting(spark):
    df = spark.createDataFrame(
        [
            ("1", "101", "5", "20.5"),
            ("2", "102", "10", "15.0")
        ],
        ["order_id", "product_id", "quantity", "unit_price"]
    )

    cleaned = df \
        .withColumn("order_id", col("order_id").cast(IntegerType())) \
        .withColumn("quantity", col("quantity").cast(IntegerType()))

    assert cleaned.schema["order_id"].dataType == IntegerType()
    assert cleaned.schema["quantity"].dataType == IntegerType()


def test_clean_trim_strings(spark):
    df = spark.createDataFrame(
        [(" ALFKI ", " London ")],
        ["customer_id", "city"]
    )

    cleaned = df \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("city", trim(col("city")))

    row = cleaned.collect()[0]

    assert row["customer_id"] == "ALFKI"
    assert row["city"] == "London"


def test_clean_remove_nulls(spark):
    df = spark.createDataFrame(
        [
            (1, 5),
            (2, None)
        ],
        ["order_id", "quantity"]
    )

    cleaned = df.dropna()

    assert cleaned.count() == 1


def test_clean_remove_duplicates(spark):
    df = spark.createDataFrame(
        [
            (1, 5),
            (1, 5)
        ],
        ["order_id", "quantity"]
    )

    cleaned = df.dropDuplicates()

    assert cleaned.count() == 1
