# # import pymysql
# # import pandas as pd
# # import json
# # import sys
# # import traceback
# # from datetime import datetime, timezone
# # import os

# # LAST_RUN_FILE = "/opt/airflow/data/last_run.txt"

# # def get_last_run_timestamp():
# #     if not os.path.exists(LAST_RUN_FILE):
# #         with open(LAST_RUN_FILE, "w") as f:
# #             f.write("2025-01-01T00:00:00+00:00")  # Use ISO format with UTC offset
# #     with open(LAST_RUN_FILE, "r") as f:
# #         last_run = f.read().strip()
# #     print(f"[INFO] Last run timestamp: {last_run}")
# #     return last_run

# # def update_last_run_timestamp():
# #     now = datetime.now(timezone.utc).isoformat()
# #     with open(LAST_RUN_FILE, "w") as f:
# #         f.write(now)
# #     print(f"[INFO] Updated last_run.txt with timestamp: {now}")
# #     return now  # Return it so you can reuse below

# # def extract_mysql(output_path):
# #     try:
# #         last_updated = get_last_run_timestamp()
# #         print(f"[INFO] Starting MySQL extraction with last_updated={last_updated}")
        
# #         with open('/opt/airflow/config/db_config.json') as f:
# #             config = json.load(f)
# #         print(f"[INFO] Loaded DB config: host={config['host']}, db={config['database']}")

# #         conn = pymysql.connect(
# #             host=config['host'],
# #             user=config['user'],
# #             password=config['password'],
# #             database=config['database'],
# #             port=config['port']
# #         )
# #         print("[INFO] MySQL connection established.")

# #         query = f"SELECT * FROM users WHERE updated_at > '{last_updated}'"
# #         print(f"[INFO] Executing query: {query}")

# #         df = pd.read_sql(query, conn)
# #         print(f"[INFO] Query executed. Rows fetched: {len(df)}")

# #         if not df.empty:
# #             df.to_csv(output_path, index=False)
# #             print(f"[INFO] Data written to {output_path}")
# #         else:
# #             print("[INFO] No new or updated records found.")

# #         conn.close()
# #         print("[INFO] MySQL connection closed.")

# #         new_timestamp = update_last_run_timestamp()
# #         return new_timestamp

# #     except Exception as e:
# #         print("[ERROR] An error occurred during MySQL extraction.")
# #         traceback.print_exc()
# #         sys.exit(2)

# # if __name__ == "__main__":
# #     if len(sys.argv) != 2:
# #         print("[ERROR] Invalid number of arguments. Usage: script.py <output_path>")
# #         sys.exit(1)

# #     output_path = sys.argv[1]
# #     new_timestamp = extract_mysql(output_path)
# #     print(f"[RESULT] {new_timestamp}")

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from datetime import datetime, timezone
# import os

# LAST_RUN_FILE = "/opt/airflow/data/last_run.txt"

# def get_last_run():
#     if not os.path.exists(LAST_RUN_FILE):
#         with open(LAST_RUN_FILE, "w") as f:
#             f.write("2025-01-01 00:00:00")
#     with open(LAST_RUN_FILE, "r") as f:
#         return f.read().strip()

# def update_last_run():
#     now = datetime.now(timezone.utc).isoformat()
#     with open(LAST_RUN_FILE, "w") as f:
#         f.write(now)
#     return now

# def main():
#     spark = SparkSession.builder \
#         .appName("IncrementalETL") \
#         .master("spark://spark_incre:7077") \
#         .getOrCreate()

#     last_run = get_last_run()
#     print(f"[INFO] Last run timestamp: {last_run}")

#     jdbc_url = "jdbc:mysql://mysql:3306/airflow"

#     connection_properties = {
#         "user": "root",
#         "password": "airflow_pass",
#         "driver": "com.mysql.cj.jdbc.Driver"
#     }

#     query = f"(SELECT * FROM users WHERE updated_at > '{last_run}') as users_incremental"

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

#     count = df.count()
#     print(f"[INFO] Rows fetched: {count}")

#     if count > 0:
#         output_path = "/opt/airflow/data/mysql_data.csv"

#         df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
#         print(f"[INFO] Data written to {output_path}")
#         # update_last_run()
#         timestamp = update_last_run()
#         print(f"[RESULT] {timestamp}")

#         print(f"[INFO] Updated last run timestamp.")

#     spark.stop()

# if __name__ == "__main__":
#     main()
# extract_mysql.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timezone
import sys

def update_last_run():
    now = datetime.now(timezone.utc).isoformat()
    return now

def main(last_run):
    spark = SparkSession.builder \
        .appName("IncrementalETL") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print(f"[INFO] Last run timestamp: {last_run}")

    jdbc_url = "jdbc:mysql://mysql:3306/airflow"

    connection_properties = {
        "user": "root",
        "password": "airflow_pass",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    query = f"(SELECT * FROM users WHERE updated_at > '{last_run}') as users_incremental"

    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

    count = df.count()
    print(f"[INFO] Rows fetched: {count}")

    if count > 0:
        output_path = "/opt/airflow/data/mysql_data.csv"
        df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"[INFO] Data written to {output_path}")

        timestamp = update_last_run()
        print(f"[RESULT] {timestamp}")
        print(f"[INFO] Updated last run timestamp.")

    spark.stop()

if __name__ == "__main__":
    # Get timestamp from CLI, or use fallback default
    last_run_arg = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01T00:00:00"
    main(last_run_arg)
