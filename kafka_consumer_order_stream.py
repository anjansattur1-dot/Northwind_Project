import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, current_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("required_date", StringType(), True),
    StructField("shipped_date", StringType(), True),
    StructField("shipper_id", IntegerType(), True),
    StructField("freight", DoubleType(), True)
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("KafkaOrdersConsumerToHDFS")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from topic: orders")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .withColumn("data", from_json(col("value").cast("string"), ORDER_SCHEMA))
        .select("data.*")
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("required_date", to_date(col("required_date"), "yyyy-MM-dd"))
        .withColumn("shipped_date", to_date(col("shipped_date"), "yyyy-MM-dd"))
        .withColumn(
            "delivery_status",
            when(col("shipped_date").isNull(), "PENDING")
            .when(col("shipped_date") <= col("required_date"), "ON_TIME")
            .otherwise("DELAYED")
        )
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    query = (
        parsed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "hdfs:///tmp/anjan_project/bronze/orders_stream")
        .option("checkpointLocation", "hdfs:///tmp/anjan_project/checkpoints/orders_bronze_stream")
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("Streaming orders to HDFS path: /tmp/anjan_project/bronze/orders_stream")
    query.awaitTermination()


if __name__ == "__main__":
    main()
