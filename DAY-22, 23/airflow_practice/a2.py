from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

default_args = {
    'owner': 'gauree',
    'retries':1,
    'retry_delay': timedelta(minutes=2),
}

# def say_hi():
#     print('hello')

def t1(**context):
    nuum = [1,2,3,4,5]
    context['ti'].xcom_push(key='heeyy', value=nuum)
    print(nuum)

def t2(**context):
    re = context['ti'].xcom_pull(key='heeyy', task_ids = 'tt1')
    resu = sum(i**2 for i in re)
    print(resu)

with DAG(
    dag_id='hello',
    start_date = days_ago(1),
    schedule_interval=timedelta(minutes=1)

)as dag:
    
    task1 =  PythonOperator(
        task_id='tt1',
        python_callable=t1,
        provide_context=True
    )

    task2 =  PythonOperator(
        task_id='tt2',
        python_callable=t2,
        provide_context=True
    )

    task1 >> task2



@dag(
    dag_id='hello',
    start_date = days_ago(1),
    schedule_interval=timedelta(minutes=1)
)

def my_task():
    
    @task
    def t1():
        nuum = [1,2,3,4,5]
        print(nuum)
        return nuum

    @task
    def t2(nuum):
        res = sum(i**2 for i in nuum)
        print(res)

    l1 = t1()
    t2(l1)

dag = my_task()






    task1 =  PythonOperator(
        task_id='tt1',
        python_callable=t1,
        provide_context=True
    )

    task2 =  PythonOperator(
        task_id='tt2',
        python_callable=t2,
        provide_context=True
    )














# def say_hi():
#     print('hi')
# def task1(**context):
#     nums = [1,2,3,4,5]
#     context['ti'].xcom_push(key='tasss', value=nums)
#     print(nums)

# def task2(**context):
#     nums = context['ti'].xcom_pull(key='tasss', task_ids='numbers_1')
#     result = sum(i**2 for i in nums)
#     print(result)

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

@dag(
    start_date= days_ago(1),
    schedule_interval= timedelta(minutes=1),
    default_args= default_args, 
    dag_id= 'my_dag',
)

def my_task():

    @task
    def task1():
        nums = [1,2,3,4,5]
        print(nums)
        return nums

    @task
    def task2(nums):
        result = sum(i**2 for i in nums)
        print(result)

    v1 = task1()
    task2(v1)

dag = my_task()