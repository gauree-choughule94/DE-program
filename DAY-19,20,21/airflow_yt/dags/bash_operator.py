from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "gauree",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

with DAG (
    dag_id= "bash_operator_3tasks",
    default_args= default_args,
    description= "bash_operator",
    start_date= datetime(2025, 5, 12, 5, 15),
    schedule_interval='@hourly'
) as dag:
    t1 = BashOperator(
        task_id='first_tasks',
        bash_command='echo hi, hello, good morning!'
    )

    t2 = BashOperator(
        task_id='second_tasks',
        bash_command='echo this is my second tasks'
    )

    last_empty_task = EmptyOperator(
        task_id='run_this_at_last'
    )

    t1 >> [t2, last_empty_task]

