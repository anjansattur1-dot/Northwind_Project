import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-northwind") \
        .master("local[*]") \
        .getOrCreate()


def test_full_load_no_null_ids(spark):
    data = [
        (1, 101, 5, 20.0),
        (None, 102, 10, 15.0)
    ]

    columns = ["order_id", "product_id", "quantity", "unit_price"]

    df = spark.createDataFrame(data, columns)

    valid_df = df.filter(df.order_id.isNotNull())

    assert valid_df.count() == 1
