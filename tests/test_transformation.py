import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-northwind") \
        .master("local[*]") \
        .getOrCreate()


def test_total_price_calculation(spark):
    df = spark.createDataFrame(
        [
            (1, 10, 5.0),
            (2, 2, 20.0)
        ],
        ["order_id", "quantity", "unit_price"]
    )

    result = df.withColumn("total_price", col("quantity") * col("unit_price"))

    rows = result.collect()

    assert rows[0]["total_price"] == 50.0
    assert rows[1]["total_price"] == 40.0


def test_sales_aggregation_by_customer(spark):
    df = spark.createDataFrame(
        [
            ("C1", 100.0),
            ("C1", 50.0),
            ("C2", 200.0)
        ],
        ["customer_id", "total_price"]
    )

    result = df.groupBy("customer_id") \
        .agg(spark_sum("total_price").alias("total_spend"))

    output = {row["customer_id"]: row["total_spend"] for row in result.collect()}

    assert output["C1"] == 150.0
    assert output["C2"] == 200.0


def test_filter_valid_orders(spark):
    df = spark.createDataFrame(
        [
            (1, 50.0),
            (2, -10.0)
        ],
        ["order_id", "total_price"]
    )

    valid = df.filter(col("total_price") > 0)

    assert valid.count() == 1
