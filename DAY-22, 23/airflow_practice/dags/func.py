from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def sum_of_squares():
    l1 = [1, 2, 3, 4, 5]
    result = sum(i**2 for i in l1)
    print('sum of squares:', result)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='sum_of_squares_dag',
    default_args=default_args,
    description='A DAG that calculates the sum of squares of a list of numbers',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=["example"],
) as dag:
    
    # Define the task
    task1 = PythonOperator(
        task_id='calculate_sum_of_squares',
        python_callable=sum_of_squares,
    )
