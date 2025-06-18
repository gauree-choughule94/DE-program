from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from datetime import datetime
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def process_products_to_silver(spark, bronze_base_path, silver_base_path, processing_date_str):
    year, month, day = processing_date_str.split('-')
    raw_path = f"{bronze_base_path}/mysql/products/{year}/{month}/{day}"
    silver_path = f"{silver_base_path}/products_cleaned/processed_date={processing_date_str}"

    logger.info(f"Reading raw products from: {raw_path}")
    df = spark.read.parquet(raw_path)

    df_cleaned = df.withColumn("name", trim(col("name"))) \
                   .withColumn("category", lower(trim(col("category")))) \
                   .withColumn("price", col("price").cast("decimal(10,2)")) \
                   .withColumn("created_at", col("created_at").cast("timestamp"))

    df_silver = df_cleaned.select(
        col("id").cast("int"),
        col("name").cast("string"),
        col("price"),
        col("category").cast("string"),
        col("created_at")
    )

    logger.info(f"Writing cleaned products to: {silver_path}")
    df_silver.write.mode("overwrite").parquet(silver_path)
    logger.info("Products processing completed.")


def process_orders_to_silver(spark, bronze_base_path, silver_base_path, processing_date_str):
    year, month, day = processing_date_str.split('-')
    raw_path = f"{bronze_base_path}/mysql/orders/{year}/{month}/{day}"
    silver_path = f"{silver_base_path}/orders_cleaned/processed_date={processing_date_str}"

    logger.info(f"Reading raw orders from: {raw_path}")
    df = spark.read.parquet(raw_path)

    df_cleaned = df.select(
        col("id").cast("int"),
        col("customer_id").cast("int"),
        col("product_id").cast("int"),
        col("quantity").cast("int"),
        col("total_price").cast("decimal(10,2)"),
        col("order_date").cast("timestamp"),
        col("last_updated").cast("timestamp")
    )

    logger.info(f"Writing cleaned orders to: {silver_path}")
    df_cleaned.write.mode("overwrite").parquet(silver_path)
    logger.info("Orders processing completed.")


def process_customers_to_silver(spark, bronze_base_path, silver_base_path, processing_date_str):
    year, month, day = processing_date_str.split('-')
    raw_path = f"{bronze_base_path}/mysql/customers/{year}/{month}/{day}"
    silver_path = f"{silver_base_path}/customers_cleaned/processed_date={processing_date_str}"

    logger.info(f"Reading raw customers from: {raw_path}")
    df = spark.read.parquet(raw_path)

    df_cleaned = df.withColumn("name", trim(col("name"))) \
                   .withColumn("email", trim(lower(col("email")))) \
                   .withColumn("address", trim(col("address"))) \
                   .select(
                       col("id").cast("int"),
                       col("name").cast("string"),
                       col("email").cast("string"),
                       col("address").cast("string"),
                       col("created_at").cast("timestamp"),
                       col("last_updated").cast("timestamp")
                   )

    logger.info(f"Writing cleaned customers to: {silver_path}")
    df_cleaned.write.mode("overwrite").parquet(silver_path)
    logger.info("Customers processing completed.")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        logger.error("Usage: silver_processing.py <bronze_base_path> <silver_base_path> <processing_date YYYY-MM-DD> <table_name>")
        logger.error("Example: silver_processing.py /opt/data_lake/bronze /opt/data_lake/silver 2024-07-01 products")
        sys.exit(-1)

    bronze_path_arg = sys.argv[1]
    silver_path_arg = sys.argv[2]
    date_arg = sys.argv[3]
    table_arg = sys.argv[4].lower()

    spark = SparkSession.builder.appName("SilverLayerProcessing").getOrCreate()

    try:
        if table_arg == 'products':
            process_products_to_silver(spark, bronze_path_arg, silver_path_arg, date_arg)
        elif table_arg == 'orders':
            process_orders_to_silver(spark, bronze_path_arg, silver_path_arg, date_arg)
        elif table_arg == 'customers':
            process_customers_to_silver(spark, bronze_path_arg, silver_path_arg, date_arg)
        else:
            logger.error(f"Unsupported table name: {table_arg}. Choose from 'products', 'orders', or 'customers'.")
            sys.exit(-1)
    finally:
        spark.stop()
