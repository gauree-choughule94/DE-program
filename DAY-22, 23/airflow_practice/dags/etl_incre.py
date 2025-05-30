# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime, timedelta
# # import mysql.connector
# # import requests
# # import subprocess
# # import os

# # default_args = {
# #     'owner': 'airflow',
# #     'start_date': datetime(2024, 1, 1),
# #     'retries': 1,
# #     'retry_delay': timedelta(seconds=10)
# # }

# # dag = DAG(
# #     dag_id='incremental_api_spark_etl',
# #     default_args=default_args,
# #     schedule_interval='*/1 * * * *',  # every minute
# #     catchup=False,
# #     description='Incremental loading from API using Spark and storing in MySQL'
# # )

# # # MYSQL_CONFIG = {
# # #     "host": "localhost",
# # #     "user": "root",
# # #     "password": "airflow_pass",
# # #     "database": "airflow"
# # # }
# # MYSQL_CONFIG = {
# #     "host": "mysql",    # running the Python code inside a Docker container but then use port 3306
# #     # "host": "localhost",
# #     "user": "root",
# #     "password": "airflow_pass",
# #     "database": "airflow",
# #     "port": 3306
# # }


# # API_URL = "https://dummyjson.com/products"

# # def get_last_id():
# #     conn = mysql.connector.connect(**MYSQL_CONFIG)
# #     cursor = conn.cursor()
# #     cursor.execute("CREATE TABLE IF NOT EXISTS product_meta (id INT PRIMARY KEY)")
# #     cursor.execute("SELECT MAX(id) FROM product_meta")
# #     result = cursor.fetchone()[0]
# #     conn.close()
# #     return result if result else 0

# # def fetch_data(**context):
# #     last_id = get_last_id()
# #     print(f"Fetching data after ID: {last_id}")
# #     response = requests.get(API_URL)
# #     data = response.json()["products"]
# #     new_data = [item for item in data if item["id"] > last_id]
    
# #     filepath = "/opt/airflow/dags/tmp/products.json"
# #     os.makedirs(os.path.dirname(filepath), exist_ok=True)
# #     with open(filepath, "w") as f:
# #         import json
# #         json.dump(new_data, f)
    
# #     context['ti'].xcom_push(key="latest_id", value=max([d["id"] for d in new_data], default=last_id))

# # def run_spark_job(**context):
# #     subprocess.run(["/opt/bitnami/spark/bin/spark-submit", "/app/process_data.py"], check=True)

# # def update_last_id(**context):
# #     latest_id = context['ti'].xcom_pull(key="latest_id")
# #     if latest_id:
# #         conn = mysql.connector.connect(**MYSQL_CONFIG)
# #         cursor = conn.cursor()
# #         cursor.execute("INSERT IGNORE INTO product_meta (id) VALUES (%s)", (latest_id,))
# #         conn.commit()
# #         conn.close()

# # fetch = PythonOperator(
# #     task_id='fetch_data',
# #     python_callable=fetch_data,
# #     provide_context=True,
# #     dag=dag,
# # )

# # process = PythonOperator(
# #     task_id='spark_process',
# #     python_callable=run_spark_job,
# #     provide_context=True,
# #     dag=dag,
# # )

# # update = PythonOperator(
# #     task_id='update_id',
# #     python_callable=update_last_id,
# #     provide_context=True,
# #     dag=dag,
# # )

# # fetch >> process >> update
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import psycopg2
# import requests
# import subprocess
# import os
# import json

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(seconds=10)
# }

# dag = DAG(
#     dag_id='incremental_api_spark_etl_postgres',
#     default_args=default_args,
#     schedule_interval='*/1 * * * *',  # every minute
#     catchup=False,
#     description='Incremental loading from API using Spark and storing in PostgreSQL'
# )

# POSTGRES_CONFIG = {
#     "host": "postgres",    # container name from docker-compose
#     "user": "airflow",
#     "password": "airflow_pass",
#     "dbname": "airflow",
#     "port": 5432
# }

# API_URL = "https://dummyjson.com/products"

# def get_last_id():
#     conn = psycopg2.connect(**POSTGRES_CONFIG)
#     cursor = conn.cursor()
#     cursor.execute("CREATE TABLE IF NOT EXISTS product_meta (id INT PRIMARY KEY)")
#     cursor.execute("SELECT MAX(id) FROM product_meta")
#     result = cursor.fetchone()[0]
#     conn.close()
#     return result if result else 0

# def fetch_data(**context):
#     last_id = get_last_id()
#     print(f"Fetching data after ID: {last_id}")
#     response = requests.get(API_URL)
#     data = response.json()["products"]
#     new_data = [item for item in data if item["id"] > last_id]

#     filepath = "/opt/airflow/dags/tmp/products.json"
#     os.makedirs(os.path.dirname(filepath), exist_ok=True)
#     with open(filepath, "w") as f:
#         json.dump(new_data, f)

#     context['ti'].xcom_push(key="latest_id", value=max([d["id"] for d in new_data], default=last_id))

# def run_spark_job(**context):
#     subprocess.run(["/opt/bitnami/spark/bin/spark-submit", "/app/process_data.py"], check=True)

# def update_last_id(**context):
#     latest_id = context['ti'].xcom_pull(key="latest_id")
#     if latest_id:
#         conn = psycopg2.connect(**POSTGRES_CONFIG)
#         cursor = conn.cursor()
#         cursor.execute("""
#             INSERT INTO product_meta (id) VALUES (%s)
#             ON CONFLICT (id) DO NOTHING
#         """, (latest_id,))
#         conn.commit()
#         conn.close()

# fetch = PythonOperator(
#     task_id='fetch_data',
#     python_callable=fetch_data,
#     provide_context=True,
#     dag=dag,
# )

# process = PythonOperator(
#     task_id='spark_process',
#     python_callable=run_spark_job,
#     provide_context=True,
#     dag=dag,
# )

# update = PythonOperator(
#     task_id='update_id',
#     python_callable=update_last_id,
#     provide_context=True,
#     dag=dag,
# )

# fetch >> process >> update
