# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable
# from datetime import datetime, timedelta
# import subprocess

# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2)
# }

# def get_last_updated():
#     return Variable.get("last_updated", default_var="2000-01-01T00:00:00")

# def update_last_updated(ts):
#     Variable.set("last_updated", ts)

# def extract_mysql():
#     last_updated = get_last_updated()
#     subprocess.run([
#         "python3", "/opt/airflow/script/extract_mysql.py", 
#         last_updated, "/opt/airflow/data/mysql_data.csv"
#     ], check=True)

# def extract_api():
#     last_updated = get_last_updated()
#     subprocess.run([
#         "python3", "/opt/airflow/script/extract_api.py", 
#         last_updated, "/opt/airflow/data/api_data.csv"
#     ], check=True)

# def transform_and_load():
#     subprocess.run([
#         "python3", "/opt/airflow/script/transform_load.py",
#         "/opt/airflow/data/mysql_data.csv",
#         "/opt/airflow/data/api_data.csv",
#         "/opt/airflow/data/output.parquet"
#     ], check=True)
#     update_last_updated(datetime.now().isoformat())

# with DAG("incremental_etl_mysql",
#          schedule_interval=timedelta(minutes=1),
#          start_date=datetime(2023, 1, 1),
#          catchup=False,
#          default_args=default_args) as dag:

#     t1 = PythonOperator(task_id="extract_mysql", python_callable=extract_mysql)
#     t2 = PythonOperator(task_id="extract_api", python_callable=extract_api)
#     t3 = PythonOperator(task_id="transform_and_load", python_callable=transform_and_load)

#     t1 >> t2 >> t3


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import subprocess
import re

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def get_last_updated():
    value = Variable.get("last_updated", default_var="2025-01-01T00:00:00")
    print(f"[INFO] Last updated timestamp from Variable: {value}")
    return value


def extract_mysql():
    last_updated = get_last_updated()
    output_path = "/opt/airflow/data/mysql_data.csv"

    print(f"[INFO] Running extract_mysql.py with args: {last_updated}, {output_path}")

    # result = subprocess.run([
    #     "python3", "/opt/airflow/script/extract_mysql.py", 
    #     output_path
    # ], capture_output=True, text=True)
    result = subprocess.run([
    "spark-submit", "/opt/airflow/script/extract_mysql.py", last_updated
], capture_output=True, text=True)

    print("[INFO] Script stdout:")
    print(result.stdout)

    print("[INFO] Script stderr:")
    print(result.stderr)

    if result.returncode != 0:
        print("[ERROR] extract_mysql.py failed.")
        raise subprocess.CalledProcessError(result.returncode, result.args)

    # Extract the timestamp from stdout
    match = re.search(r"\[RESULT\]\s*(\d{4}-\d{2}-\d{2}T[^\s]+)", result.stdout)
    if match:
        new_timestamp = match.group(1)
        Variable.set("last_updated", new_timestamp)
        print(f"[INFO] Updated Airflow Variable 'last_updated' to {new_timestamp}")
    else:
        print("[WARNING] No timestamp found in extract_mysql.py output. Variable not updated.")
  

def extract_api():
    last_updated = get_last_updated()
    print(f"[INFO] Running extract_api.py with args: {last_updated}")

    result1 = subprocess.run([
        "python3", "/opt/airflow/script/extract_api.py", 
        last_updated, "/opt/airflow/data/api_data.csv"
    ], capture_output=True, text=True, check=True)

    print("[INFO] Script stdout:")
    print(result1.stdout)

    print("[INFO] Script stderr:")
    print(result1.stderr)

    if result1.returncode != 0:
        print("[ERROR] extract_api.py failed.")
        raise subprocess.CalledProcessError(result1.returncode, result1.args)

with DAG("incremental_etl",
         schedule_interval=timedelta(hours=1),
         start_date=datetime(2023, 1, 1),
         catchup=False,
         default_args=default_args) as dag:

    t1 = PythonOperator(task_id="extract_mysql", python_callable=extract_mysql)

    t2 = PythonOperator(task_id="extract_api", python_callable=extract_api)

    t1 >> t2 


