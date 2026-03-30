
#importing libraries
from pyspark.sql import SparkSession
from logger_util import setup_logger

logger = setup_logger("full_load_csv_jar")
# spark session
spark = SparkSession.builder.appName("Postgres_to_HDFS").getOrCreate()
logger.info("Connecting to PostgreSQL")

# connecting to sql db
jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"

properties = {
  "user": "admin",
  "password": "admin123",
  "driver": "org.postgresql.Driver"
}
logger.info("Reading source tables")



#reading from sql db
categories_df= spark.read.jdbc(url=jdbc_url,table="anjan.categories",properties=properties)
customers_df = spark.read.jdbc(url=jdbc_url,table="anjan.customers",properties=properties)
employees_df = spark.read.jdbc(url=jdbc_url,table="anjan.employees",properties=properties)
shippers_df = spark.read.jdbc(url=jdbc_url,table="anjan.shippers",properties=properties)
products_df = spark.read.jdbc(url=jdbc_url,table="anjan.products",properties=properties)
ord_details_df = spark.read.jdbc(url=jdbc_url,table="anjan.order_details",properties=properties)
orders_df = spark.read.jdbc(url=jdbc_url,table="anjan.orders",properties=properties)


logger.info("Writing bronze parquet outputs")
#writing as parqet files to hadoop bronze folder
categories_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/categories")
customers_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/customers")
employees_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/employees")
shippers_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/shippers")
products_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/products")
ord_details_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/order_details")
orders_df.write.mode("overwrite").parquet("/tmp/anjan_project/bronze/orders")
logger.info("Full load completed successfully")
spark.stop()
