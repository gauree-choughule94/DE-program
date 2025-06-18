from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, sum as spark_sum
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def create_product_summary(spark, silver_base_path, gold_base_path, processing_date_str):
    input_path = f"{silver_base_path}/products_cleaned/processed_date={processing_date_str}"
    output_path = f"{gold_base_path}/products_summary/report_date={processing_date_str}"

    logger.info(f"Processing products summary from: {input_path}")
    df = spark.read.parquet(input_path)

    summary_df = df.groupBy("category").agg(
        count("id").alias("total_products"),
        avg("price").alias("avg_price"),
        spark_max("price").alias("max_price")
    )

    summary_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written product summary to: {output_path}")


def create_orders_summary(spark, silver_base_path, gold_base_path, processing_date_str):
    input_path = f"{silver_base_path}/orders_cleaned/processed_date={processing_date_str}"
    output_path = f"{gold_base_path}/orders_summary/report_date={processing_date_str}"

    logger.info(f"Processing orders summary from: {input_path}")
    df = spark.read.parquet(input_path)

    from pyspark.sql.functions import row_number
    from pyspark.sql.window import Window

    summary_df = df.groupBy("customer_id").agg(
        count("id").alias("total_orders"),
        spark_sum("total_price").alias("total_spent")
    )

    window_spec = Window.orderBy(col("total_spent").desc())
    summary_df = summary_df.withColumn("rank_by_spend", row_number().over(window_spec))

    summary_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written orders summary to: {output_path}")


def create_customers_summary(spark, silver_base_path, gold_base_path, processing_date_str):
    input_path = f"{silver_base_path}/customers_cleaned/processed_date={processing_date_str}"
    output_path = f"{gold_base_path}/customers_summary/report_date={processing_date_str}"

    logger.info(f"Processing customers summary from: {input_path}")
    df = spark.read.parquet(input_path)

    summary_df = df.groupBy("address").agg(
        count("id").alias("total_customers")
    )

    summary_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written customers summary to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        logger.error("Usage: gold_aggregation.py <silver_base_path> <gold_base_path> <processing_date YYYY-MM-DD> <table_name>")
        sys.exit(1)

    silver_path = sys.argv[1]
    gold_path = sys.argv[2]
    processing_date = sys.argv[3]
    table_name = sys.argv[4].lower()

    spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()

    if table_name == "products":
        create_product_summary(spark, silver_path, gold_path, processing_date)
    elif table_name == "orders":
        from pyspark.sql.functions import row_number  # only needed for orders
        create_orders_summary(spark, silver_path, gold_path, processing_date)
    elif table_name == "customers":
        create_customers_summary(spark, silver_path, gold_path, processing_date)
    else:
        logger.error(f"Invalid table name: {table_name}")
        sys.exit(1)

    spark.stop()
    logger.info("Gold aggregation completed successfully.")