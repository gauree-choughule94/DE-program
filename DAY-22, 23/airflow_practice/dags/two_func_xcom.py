from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from typing import List

# Function to generate a list of numbers and push to XCom
def generate_numbers(**context):
    numbers = [1, 2, 3, 4, 5]
    context['ti'].xcom_push(key='number_list', value=numbers)
    print("Generated numbers:", numbers)

# Function to retrieve numbers and calculate sum of squares
def compute_sum_of_squares(**context):
    numbers: List[int] = context['ti'].xcom_pull(key='number_list', task_ids='generate_numbers_task')
    result = sum(i**2 for i in numbers)
    print("Sum of squares:", result)

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='sum_of_squares_dependent_dag',
    default_args=default_args,
    description='A DAG with two dependent Python tasks',
    schedule_interval=timedelta(minutes=1),
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:

    task1 = PythonOperator(
        task_id='generate_numbers_task',
        python_callable=generate_numbers,
        provide_context=True,
    )

    task2 = PythonOperator(
        task_id='compute_sum_of_squares_task',
        python_callable=compute_sum_of_squares,
        provide_context=True,
    )

    task1 >> task2  # Define task dependency


# “Pull the list of numbers that was saved under the key 'number_list' by the task with ID 
# 'generate_numbers_task' and store it in a variable named numbers, which should be a list of integers.”