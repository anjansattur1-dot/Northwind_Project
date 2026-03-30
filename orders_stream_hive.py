from pyspark.sql import SparkSession

SILVER_PATH = "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/anjan_project/silver/orders_stream"
HIVE_DB = "anjandb"
HIVE_TABLE = "orders_stream"

spark = (
    SparkSession.builder
    .appName("OrdersStreamSilverToHive")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(HIVE_DB))

silver_df = spark.read.parquet(SILVER_PATH)

silver_df.write.mode("overwrite").format("hive").saveAsTable("{}.{}".format(HIVE_DB, HIVE_TABLE))

print("Loaded Hive table: {}.{}".format(HIVE_DB, HIVE_TABLE))
spark.sql(
    "SELECT * FROM {}.{} ORDER BY order_id DESC LIMIT 20").format(HIVE_DB,HIVE_TABLE
).show(truncate=False)
