from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    round as spark_round,
    year,
    month,
    countDistinct,
    count,
    concat_ws,
    lpad
)
from logger_util import setup_logger

logger = setup_logger("silver_to_gold_hive")

spark = SparkSession.builder \
    .appName("Northwind_Gold_Insights_To_Hive") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020") \
    .config("hive.metastore.uris", "thrift://ip-172-31-6-42.eu-west-2.compute.internal:9083") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 8)

TARGET_DB = "anjandb"

try:
    logger.info("Silver to Gold Hive job started")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    spark.sql(f"USE {TARGET_DB}")
    logger.info(f"Hive database ready: {TARGET_DB}")

    # ==========================================
    # Read Silver Hive Tables
    # ==========================================
    logger.info("Reading silver tables from Hive")

    orders_df = spark.table(f"{TARGET_DB}.silver_orders")
    order_details_df = spark.table(f"{TARGET_DB}.silver_order_details")
    customers_df = spark.table(f"{TARGET_DB}.silver_customers")
    products_df = spark.table(f"{TARGET_DB}.silver_products")
    employees_df = spark.table(f"{TARGET_DB}.silver_employees")
    shippers_df = spark.table(f"{TARGET_DB}.silver_shippers")

    logger.info(f"silver_orders count: {orders_df.count()}")
    logger.info(f"silver_order_details count: {order_details_df.count()}")
    logger.info(f"silver_customers count: {customers_df.count()}")
    logger.info(f"silver_products count: {products_df.count()}")
    logger.info(f"silver_employees count: {employees_df.count()}")
    logger.info(f"silver_shippers count: {shippers_df.count()}")

    # ==========================================
    # Select Required Columns
    # ==========================================
    logger.info("Selecting required columns")

    orders_sel = orders_df.select(
        "order_id",
        "customer_id",
        "employee_id",
        "shipper_id",
        "order_date",
        "delivery_status",
        "freight"
    )

    order_details_sel = order_details_df.select(
        "order_id",
        "product_id",
        col("unit_price").alias("sales_unit_price"),
        "quantity",
        "discount"
    )

    customers_sel = customers_df.select(
        "customer_id",
        "company",
        "contact_name",
        col("city").alias("customer_city"),
        col("country").alias("customer_country")
    )

    products_sel = products_df.select(
        "product_id",
        "product_name",
        "category_id"
    )

    employees_sel = employees_df.select(
        "employee_id",
        "employee_name",
        "title",
        col("city").alias("employee_city"),
        col("country").alias("employee_country")
    )

    shippers_sel = shippers_df.select(
        "shipper_id",
        "shipping_company_name"
    )

    # ==========================================
    # Base Joined DataFrame
    # ==========================================
    logger.info("Creating joined base dataframe")

    base_df = order_details_sel \
        .join(orders_sel, on="order_id", how="inner") \
        .join(customers_sel, on="customer_id", how="left") \
        .join(products_sel, on="product_id", how="left") \
        .join(employees_sel, on="employee_id", how="left") \
        .join(shippers_sel, on="shipper_id", how="left") \
        .withColumn(
            "net_sales",
            spark_round(col("sales_unit_price") * col("quantity") * (1 - col("discount")), 2)
        )

    logger.info(f"base_df count: {base_df.count()}")

    # ==========================================
    # Insight 1: Top Customers by Total Spend
    # ==========================================
    logger.info("Creating report_top_customers")

    top_customers_df = base_df.groupBy(
        "customer_id", "company", "contact_name", "customer_city", "customer_country"
    ).agg(
        spark_round(spark_sum("net_sales"), 2).alias("total_spend")
    ).orderBy(col("total_spend").desc()).limit(10)

    # ==========================================
    # Insight 2: Top Selling Products
    # ==========================================
    logger.info("Creating report_top_products")

    top_products_df = base_df.groupBy(
        "product_id", "product_name", "category_id"
    ).agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_round(spark_sum("net_sales"), 2).alias("total_product_revenue")
    ).orderBy(col("total_product_revenue").desc()).limit(10)

    # ==========================================
    # Insight 3: Sales by Employee
    # ==========================================
    logger.info("Creating report_sales_by_employee")

    sales_by_employee_df = base_df.groupBy(
        "employee_id", "employee_name", "title", "employee_city", "employee_country"
    ).agg(
        spark_round(spark_sum("net_sales"), 2).alias("total_sales"),
        countDistinct("order_id").alias("total_orders_handled")
    ).orderBy(col("total_sales").desc())

    # ==========================================
    # Insight 4: Monthly Sales Trend
    # ==========================================
    logger.info("Creating report_monthly_sales_trend")

    monthly_sales_trend_df = base_df \
        .withColumn("sales_year", year(col("order_date"))) \
        .withColumn("sales_month", month(col("order_date"))) \
        .withColumn(
            "sales_year_month",
            concat_ws(
                "-",
                col("sales_year").cast("string"),
                lpad(col("sales_month").cast("string"), 2, "0")
            )
        ) \
        .groupBy("sales_year", "sales_month", "sales_year_month") \
        .agg(
            spark_round(spark_sum("net_sales"), 2).alias("monthly_sales")
        ) \
        .orderBy("sales_year", "sales_month")

    # ==========================================
    # Insight 5: Sales by Shipping Company
    # ==========================================
    logger.info("Creating report_sales_by_shipping_company")

    sales_by_shipping_company_df = base_df.groupBy(
        "shipper_id", "shipping_company_name"
    ).agg(
        spark_round(spark_sum("net_sales"), 2).alias("total_sales"),
        countDistinct("order_id").alias("total_orders")
    ).orderBy(col("total_sales").desc())

    # ==========================================
    # Insight 6: Orders by Delivery Status
    # ==========================================
    logger.info("Creating report_orders_by_delivery_status")

    orders_by_delivery_status_df = orders_sel.groupBy(
        "delivery_status"
    ).agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("freight"), 2).alias("total_freight")
    ).orderBy(col("total_orders").desc())

    # ==========================================
    # KPI Summary Table
    # ==========================================
    logger.info("Creating report_kpi_summary")

    kpi_summary_df = base_df.agg(
        spark_round(spark_sum("net_sales"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("product_id").alias("total_products"),
        countDistinct("employee_id").alias("total_employees")
    )

    # ==========================================
    # Helper to write Hive tables
    # ==========================================
    def write_hive_table(df, table_name, hdfs_path):
        full_table_name = f"{TARGET_DB}.{table_name}"
        logger.info(f"Writing Hive table: {full_table_name} at {hdfs_path}")

        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("path", hdfs_path) \
            .saveAsTable(full_table_name)

        logger.info(f"Successfully wrote Hive table: {full_table_name}")

    # ==========================================
    # Write Gold Insight Tables to Hive
    # ==========================================
    write_hive_table(
        top_customers_df,
        "report_top_customers",
        "/tmp/anjan_project/gold/report_top_customers"
    )

    write_hive_table(
        top_products_df,
        "report_top_products",
        "/tmp/anjan_project/gold/report_top_products"
    )

    write_hive_table(
        sales_by_employee_df,
        "report_sales_by_employee",
        "/tmp/anjan_project/gold/report_sales_by_employee"
    )

    write_hive_table(
        monthly_sales_trend_df,
        "report_monthly_sales_trend",
        "/tmp/anjan_project/gold/report_monthly_sales_trend"
    )

    write_hive_table(
        sales_by_shipping_company_df,
        "report_sales_by_shipping_company",
        "/tmp/anjan_project/gold/report_sales_by_shipping_company"
    )

    write_hive_table(
        orders_by_delivery_status_df,
        "report_orders_by_delivery_status",
        "/tmp/anjan_project/gold/report_orders_by_delivery_status"
    )

    write_hive_table(
        kpi_summary_df,
        "report_kpi_summary",
        "/tmp/anjan_project/gold/report_kpi_summary"
    )

    spark.sql(f"SHOW TABLES IN {TARGET_DB}").show(truncate=False)

    logger.info("Silver to Gold Hive job completed successfully")

except Exception as e:
    logger.exception(f"Silver to Gold Hive job failed: {str(e)}")
    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")
