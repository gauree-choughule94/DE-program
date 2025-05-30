from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import List

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='sum_of_squares_taskflow_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
)
def sum_of_squares_dag():

    @task
    def generate_numbers() -> List[int]:
        numbers = [1, 2, 3, 4, 5]
        print("Generated numbers:", numbers)
        return numbers

    @task
    def compute_sum_of_squares(numbers: List[int]):
        result = sum(i ** 2 for i in numbers)
        print("Sum of squares:", result)

    nums = generate_numbers()
    compute_sum_of_squares(nums)

dag = sum_of_squares_dag()
