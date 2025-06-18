from pyspark.sql import SparkSession
import sys

def load_gold_table_to_postgres(spark, gold_table_path, pg_jdbc_url, connection_properties, target_table_name):
    """Loads a Gold Parquet table to a PostgreSQL table."""
    print(f"Reading Gold data from: {gold_table_path}")
    try:
        df_gold = spark.read.parquet(gold_table_path)

        print(f"Writing data to PostgreSQL table: {target_table_name}")
        df_gold.write \
            .mode("overwrite").jdbc(url=pg_jdbc_url, table=target_table_name, properties=connection_properties) 
        # Change to "append" if needed

        print(f"Successfully loaded data to {target_table_name}")

    except Exception as e:
        print(f"Error loading data to PostgreSQL table {target_table_name}: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: load_to_warehouse.py <gold_data_path> <pg_jdbc_url> <user> <password> <target_schema.target_table> <processing_date YYYY-MM-DD>")
        sys.exit(-1)

    gold_data_path_arg = sys.argv[1]  # Example: /opt/data_lake/gold/sales_daily_summary/report_date=2024-01-01
    pg_jdbc_url_arg = sys.argv[2]     # Example: jdbc:postgresql://postgres_dw:5432/ecommerce_warehouse
    pg_user_arg = sys.argv[3]
    pg_password_arg = sys.argv[4]
    target_table_arg = sys.argv[5]    # Example: facts.sales_daily_summary
    date_arg = sys.argv[6]            # Not directly used, but good for logging

    spark = SparkSession.builder.appName(f"LoadToWarehouse_{target_table_arg}").getOrCreate()

    connection_properties = {
        "user": pg_user_arg,
        "password": pg_password_arg,
        "driver": "org.postgresql.Driver"
    }

    load_gold_table_to_postgres(spark, gold_data_path_arg, pg_jdbc_url_arg, connection_properties, target_table_arg)

    spark.stop()
