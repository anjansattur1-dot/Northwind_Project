from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, current_timestamp

BRONZE_PATH = "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/anjan_project/bronze/orders_stream"

SILVER_PATH = "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/anjan_project/silver/orders_stream"

spark = (
    SparkSession.builder
    .appName("OrdersStreamBronzeToSilver")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.parquet(BRONZE_PATH)

silver_df = (
    bronze_df
    .withColumn("customer_id", upper(trim(col("customer_id"))))
    .withColumn("silver_load_timestamp", current_timestamp())
    .dropDuplicates(["order_id"])
)

silver_df.write.mode("append").parquet(SILVER_PATH)

print("Silver load completed.")
print("Silver count =", silver_df.count())
