from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round, trim, upper, lower, regexp_replace,
    to_date, current_timestamp, when
)
from logger_util import setup_logger

logger = setup_logger("bronze_to_silver_all_tables")

spark = SparkSession.builder \
    .appName("Spark_Bronze_to_Silver_ETL") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    logger.info("Bronze to Silver ETL with Hive registration started")

    spark.sql("CREATE DATABASE IF NOT EXISTS anjandb")
    spark.sql("USE anjandb")
    logger.info("Hive database ready: anjandb")

    # ================================
    # Read Bronze Data
    # ================================
    logger.info("Reading bronze data")

    categories_df = spark.read.parquet("/tmp/anjan_project/bronze/categories")
    order_details_df = spark.read.parquet("/tmp/anjan_project/bronze/order_details")
    employees_df = spark.read.parquet("/tmp/anjan_project/bronze/employees")
    customers_df = spark.read.parquet("/tmp/anjan_project/bronze/customers")
    products_df = spark.read.parquet("/tmp/anjan_project/bronze/products")
    orders_df = spark.read.parquet("/tmp/anjan_project/bronze/orders")
    shippers_df = spark.read.parquet("/tmp/anjan_project/bronze/shippers")

    logger.info(f"categories bronze count: {categories_df.count()}")
    logger.info(f"order_details bronze count: {order_details_df.count()}")
    logger.info(f"employees bronze count: {employees_df.count()}")
    logger.info(f"customers bronze count: {customers_df.count()}")
    logger.info(f"products bronze count: {products_df.count()}")
    logger.info(f"orders bronze count: {orders_df.count()}")
    logger.info(f"shippers bronze count: {shippers_df.count()}")

    # ================================
    # ORDER DETAILS
    # ================================
    logger.info("Transforming order_details")

    order_details_df = order_details_df \
        .withColumnRenamed("orderID", "order_id") \
        .withColumnRenamed("productID", "product_id") \
        .withColumnRenamed("unitPrice", "unit_price") \
        .withColumn("order_id", col("order_id").cast("int")) \
        .withColumn("product_id", col("product_id").cast("int")) \
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("discount", round(col("discount").cast("decimal(5,2)"), 2)) \
        .withColumn("total_price", round(col("unit_price") * col("quantity"), 2)) \
        .dropDuplicates() \
        .fillna({"order_id": 0, "product_id": 0})

    logger.info(f"order_details silver count: {order_details_df.count()}")

    # ================================
    # CATEGORIES
    # ================================
    logger.info("Transforming categories")

    categories_df = categories_df \
        .withColumnRenamed("categoryID", "category_id") \
        .withColumnRenamed("categoryName", "category_name") \
        .withColumn("category_id", col("category_id").cast("int")) \
        .withColumn("category_name", trim(col("category_name")).cast("string")) \
        .withColumn("description", trim(col("description")).cast("string"))

    logger.info(f"categories silver count: {categories_df.count()}")

    # ================================
    # EMPLOYEES
    # ================================
    logger.info("Transforming employees")

    employees_df = employees_df \
        .withColumnRenamed("employeeID", "employee_id") \
        .withColumnRenamed("employeeName", "employee_name") \
        .withColumnRenamed("reportsTo", "manager_id") \
        .withColumn("employee_id", col("employee_id").cast("int")) \
        .withColumn("employee_name", col("employee_name").cast("string")) \
        .withColumn("title", col("title").cast("string")) \
        .withColumn("city", col("city").cast("string")) \
        .withColumn("country", col("country").cast("string")) \
        .withColumn("manager_id", col("manager_id").cast("int")) \
        .dropDuplicates() \
        .fillna({"employee_id": 0, "manager_id": 0})

    logger.info(f"employees silver count: {employees_df.count()}")

    # ================================
    # CUSTOMERS
    # ================================
    logger.info("Transforming customers")

    customers_df = customers_df \
        .withColumnRenamed("customerID", "customer_id") \
        .withColumnRenamed("companyName", "company") \
        .withColumnRenamed("contactName", "contact_name") \
        .withColumnRenamed("contactTitle", "contact_title") \
        .withColumn("customer_id", col("customer_id").cast("string")) \
        .withColumn("company", col("company").cast("string")) \
        .withColumn("contact_name", col("contact_name").cast("string")) \
        .withColumn("contact_title", col("contact_title").cast("string")) \
        .withColumn("city", col("city").cast("string")) \
        .withColumn("country", col("country").cast("string"))

    for c in ["company", "contact_name", "city"]:
        customers_df = customers_df.withColumn(
            c,
            upper(trim(regexp_replace(col(c), "�", "")))
        )

    customers_df = customers_df.dropDuplicates().fillna({"customer_id": "UNKNOWN"})

    logger.info(f"customers silver count: {customers_df.count()}")

    # ================================
    # PRODUCTS
    # ================================
    logger.info("Transforming products")

    products_df = products_df \
        .withColumnRenamed("productID", "product_id") \
        .withColumnRenamed("productName", "product_name") \
        .withColumnRenamed("quantityPerUnit", "quantity_per_unit") \
        .withColumnRenamed("unitPrice", "unit_price") \
        .withColumnRenamed("categoryID", "category_id")

    products_df = products_df.withColumn(
        "product_name", upper(trim(regexp_replace(col("product_name"), "�", "")))
    ).withColumn(
        "quantity_per_unit", lower(trim(col("quantity_per_unit")))
    ).withColumn(
        "quantity_per_unit", regexp_replace("quantity_per_unit", "-", " x ")
    ).withColumn(
        "quantity_per_unit", regexp_replace("quantity_per_unit", "\\s+", " ")
    )

    products_df = products_df \
        .withColumn("product_id", col("product_id").cast("int")) \
        .withColumn("product_name", trim(col("product_name")).cast("string")) \
        .withColumn("quantity_per_unit", trim(col("quantity_per_unit")).cast("string")) \
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
        .withColumn("discontinued", col("discontinued").cast("int")) \
        .withColumn("category_id", col("category_id").cast("int")) \
        .dropDuplicates() \
        .fillna({"product_id": 0})

    logger.info(f"products silver count: {products_df.count()}")

    # ================================
    # SHIPPERS
    # ================================
    logger.info("Transforming shippers")

    shippers_df = shippers_df \
        .withColumnRenamed("shipperID", "shipper_id") \
        .withColumnRenamed("companyName", "shipping_company_name") \
        .withColumn("shipper_id", col("shipper_id").cast("int")) \
        .withColumn("shipping_company_name", col("shipping_company_name").cast("string")) \
        .dropDuplicates() \
        .fillna({"shipper_id": 0})

    logger.info(f"shippers silver count: {shippers_df.count()}")

    # ================================
    # ORDERS
    # ================================
    logger.info("Transforming orders")

    orders_df = orders_df \
        .withColumnRenamed("orderID", "order_id") \
        .withColumnRenamed("customerID", "customer_id") \
        .withColumnRenamed("employeeID", "employee_id") \
        .withColumnRenamed("orderDate", "order_date") \
        .withColumnRenamed("requiredDate", "required_date") \
        .withColumnRenamed("shippedDate", "shipped_date") \
        .withColumnRenamed("shipperID", "shipper_id")

    orders_df = orders_df \
        .withColumn("order_id", col("order_id").cast("int")) \
        .withColumn("customer_id", col("customer_id").cast("string")) \
        .withColumn("employee_id", col("employee_id").cast("int")) \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
        .withColumn("required_date", to_date(col("required_date"), "yyyy-MM-dd")) \
        .withColumn("shipped_date", to_date(col("shipped_date"), "yyyy-MM-dd")) \
        .withColumn("shipper_id", col("shipper_id").cast("int")) \
        .withColumn("freight", col("freight").cast("decimal(10,2)")) \
        .withColumn(
            "delivery_status",
            when(col("shipped_date").isNull(), "PENDING")
            .when(col("shipped_date") <= col("required_date"), "ON_TIME")
            .otherwise("DELAYED")
        ) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .dropDuplicates() \
        .fillna({"order_id": 0})

    logger.info(f"orders silver count: {orders_df.count()}")

    # ================================
    # Write Silver parquet to HDFS
    # ================================
    logger.info("Writing silver parquet files to HDFS")

    categories_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/categories")
    order_details_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/order_details")
    employees_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/employees")
    customers_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/customers")
    products_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/products")
    orders_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/orders")
    shippers_df.write.mode("overwrite").parquet("/tmp/anjan_project/silver/shippers")

    logger.info("Silver parquet files written successfully")

    # ================================
    # Drop and recreate Hive external tables
    # ================================
    logger.info("Dropping and recreating Hive external silver tables")

    spark.sql("DROP TABLE IF EXISTS anjandb.silver_categories")
    spark.sql("DROP TABLE IF EXISTS anjandb.silver_order_details")
    spark.sql("DROP TABLE IF EXISTS anjandb.silver_employees")
    spark.sql("DROP TABLE IF EXISTS anjandb.silver_customers")
    spark.sql("DROP TABLE IF EXISTS anjandb.silver_products")
    spark.sql("DROP TABLE IF EXISTS anjandb.silver_orders")
    spark.sql("DROP TABLE IF EXISTS anjandb.silver_shippers")

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_categories (
            category_id INT,
            category_name STRING,
            description STRING
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/categories'
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_order_details (
            order_id INT,
            product_id INT,
            unit_price DECIMAL(10,2),
            quantity INT,
            discount DECIMAL(5,2),
            total_price DECIMAL(10,2)
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/order_details'
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_employees (
            employee_id INT,
            employee_name STRING,
            title STRING,
            city STRING,
            country STRING,
            manager_id INT
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/employees'
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_customers (
            customer_id STRING,
            company STRING,
            contact_name STRING,
            contact_title STRING,
            city STRING,
            country STRING
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/customers'
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_products (
            product_id INT,
            product_name STRING,
            quantity_per_unit STRING,
            unit_price DECIMAL(10,2),
            discontinued INT,
            category_id INT
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/products'
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_orders (
            order_id INT,
            customer_id STRING,
            employee_id INT,
            order_date DATE,
            required_date DATE,
            shipped_date DATE,
            shipper_id INT,
            freight DECIMAL(10,2),
            delivery_status STRING,
            ingestion_timestamp TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/orders'
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE anjandb.silver_shippers (
            shipper_id INT,
            shipping_company_name STRING
        )
        STORED AS PARQUET
        LOCATION '/tmp/anjan_project/silver/shippers'
    """)

    logger.info("All Hive silver tables created successfully")

    spark.sql("SHOW TABLES IN anjandb").show(truncate=False)

    logger.info("Bronze to Silver ETL with Hive registration completed successfully")

except Exception as e:
    logger.exception(f"Bronze to Silver ETL with Hive registration failed: {str(e)}")
    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")
