from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello_world():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='simple_hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 5, 15),
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id='say_hello',
        python_callable=hello_world,
    )
