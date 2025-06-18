from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='kafka_to_bronze_streaming',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_spark_streaming = BashOperator(
        task_id='run_streaming_job',
        bash_command="""
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/pyspark_apps/spark_streaming_kafka_to_bronze.py \
        kafka:9092 \
        clickstream_events \
        /opt/data_lake/bronze/events
        """
    )
