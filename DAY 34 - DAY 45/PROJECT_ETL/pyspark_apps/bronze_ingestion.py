from pyspark.sql import SparkSession
from datetime import datetime
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def ingest_table_to_bronze(spark, jdbc_url, connection_props, table_name, bronze_base_path):
    """Ingests the MySQL 'products' table to the Bronze layer."""
    logger.info(f"Starting ingestion for table: {table_name}")
    try:
        # Load from MySQL table
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_props
        )

        # Add ingestion date
        today = datetime.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        output_path = f"{bronze_base_path}/mysql/{table_name}/{year}/{month}/{day}"

        logger.info(f"Writing to path: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Successfully ingested table '{table_name}' to Bronze at: {output_path}")

    except Exception as e:
        logger.error(f"Failed to ingest table '{table_name}': {e}")
        raise


if __name__ == "__main__":
    if len(sys.argv) != 6:
        logger.error("Usage: bronze_ingestion.py <jdbc_url> <user> <password> <table_name> <bronze_base_path>")
        sys.exit(1)

    jdbc_url_arg = sys.argv[1]
    user_arg = sys.argv[2]
    password_arg = sys.argv[3]
    table_name_arg = sys.argv[4]
    bronze_base_path_arg = sys.argv[5]

    spark = SparkSession.builder \
        .appName(f"Ingest_{table_name_arg}_To_Bronze") \
        .getOrCreate()

    connection_properties = {
        "user": user_arg,
        "password": password_arg,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    ingest_table_to_bronze(spark, jdbc_url_arg, connection_properties, table_name_arg, bronze_base_path_arg)

    spark.stop()
