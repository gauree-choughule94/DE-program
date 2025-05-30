from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'gauree',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_backfill_v02',
    default_args=default_args,
    start_date=datetime(2025, 5, 12),
    schedule_interval='@daily',
    catchup=True
    # do False, True
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )