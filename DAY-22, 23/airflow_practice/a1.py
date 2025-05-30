from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'gauree',
    'retries':1,
    'retry_delay': timedelta(minutes=2),
}

# def say_hi():
#     print('hi')
def task1(**context):
    nums = [1,2,3,4,5]
    context['ti'].xcom_push(key='tasss', value=nums)
    print(nums)

def task2(**context):
    nums = context['ti'].xcom_pull(key='tasss', task_ids='numbers_1')
    result = sum(i**2 for i in nums)
    print(result)

with DAG(
    start_date= days_ago(1),
    schedule_interval= timedelta(minutes=1),
    default_args= default_args, 
    dag_id= 'my_dag',
) as dag:

    t1 = PythonOperator(
        task_id= 'numbers_1',
        python_callable=task1,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id= 'numbers_2',
        python_callable=task2,
        provide_context=True
    )
     
    t1 >> t2