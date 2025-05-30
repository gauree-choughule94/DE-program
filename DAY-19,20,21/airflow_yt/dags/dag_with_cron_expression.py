from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'gauree',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_cron_expression_vc1",
    start_date=datetime(2025, 5, 12),
    # description='Our first dag using python operator',
    # schedule_interval='0 3 * * Tue-Fri'
    schedule_interval='* * * * *'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo dag with cron expression!"
    )
    
    task1