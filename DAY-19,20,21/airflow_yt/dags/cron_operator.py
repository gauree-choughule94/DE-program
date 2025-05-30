from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "gauree",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id= "bash_operator_3tasks",
    default_args= default_args,
    description= "bash_operator",
    start_date= datetime(2025, 5, 12, 5, 15),
    schedule_interval='@hourly'
)as dag:
    t1=BashOperator(
        task_id='cron_job',
        bash_command='my_cron_job'
    )

    t1