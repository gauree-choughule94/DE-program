from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

MYSQL_TABLES = ["orders", "customers", "products"]
SILVER_TABLES = ["orders", "customers", "products"]
GOLD_TABLES = ["products", "orders", "customers"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_spark_submit(cmd):
    subprocess.run(cmd, shell=True, check=True)

with DAG(
    dag_id='ecommerce_pipeline_python_operator',
    default_args=default_args,
    description='End-to-end pipeline: Bronze → Silver → Gold → DWH',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'silver', 'gold', 'dwh'],
) as dag:

    start = EmptyOperator(task_id='start_pipeline')

    # Bronze ingestion tasks
    bronze_tasks = []
    for table in MYSQL_TABLES:
        cmd = f"""
            spark-submit \
              --jars /opt/pyspark_apps/jars/mysql-connector-j-8.3.0.jar \
              /opt/pyspark_apps/bronze_ingestion.py \
              jdbc:mysql://mysql_source:3306/ecommerce_db \
              ecomuser \
              ecompassword \
              {table} \
              /opt/data_lake/bronze
        """
        task = PythonOperator(
            task_id=f'bronze_ingest_{table}',
            python_callable=run_spark_submit,
            op_args=[cmd],
        )
        bronze_tasks.append(task)

    bronze_complete = EmptyOperator(task_id='bronze_ingestion_complete')

    # Silver layer processing for each table
    silver_tasks = []
    for table in SILVER_TABLES:
        def silver_process(ds, table=table, **kwargs):
            cmd = f"""
                spark-submit \
                  /opt/pyspark_apps/silver_processing.py \
                  /opt/data_lake/bronze \
                  /opt/data_lake/silver \
                  {ds} \
                  {table}
            """
            run_spark_submit(cmd)

        silver_task = PythonOperator(
            task_id=f'silver_process_{table}',
            python_callable=silver_process,
            provide_context=True,
        )
        silver_tasks.append(silver_task)

    silver_complete = EmptyOperator(task_id='silver_processing_complete')

    # Gold Aggregation for each table
    gold_tasks = []
    for table in GOLD_TABLES:
        def gold_aggregate(ds, table=table, **kwargs):
            cmd = f"""
                spark-submit \
                  /opt/pyspark_apps/gold_aggregation.py \
                  /opt/data_lake/silver \
                  /opt/data_lake/gold \
                  {ds} \
                  {table}
            """
            run_spark_submit(cmd)

        gold_task = PythonOperator(
            task_id=f'gold_aggregate_{table}',
            python_callable=gold_aggregate,
            provide_context=True,
        )
        gold_tasks.append(gold_task)

    gold_complete = EmptyOperator(task_id='gold_processing_complete')

    # Load to DWH for 3 Gold tables
    def load_to_dwh(ds, **kwargs):
        tables = [
            {
                "path": f"/opt/data_lake/gold/products_summary/report_date={ds}",
                "target_table": "facts.products"
            },
            {
                "path": f"/opt/data_lake/gold/customers_summary/report_date={ds}",
                "target_table": "facts.customers"
            },
            {
                "path": f"/opt/data_lake/gold/orders_summary/report_date={ds}",
                "target_table": "facts.orders"
            }
        ]

        for table in tables:
            cmd = f"""
                spark-submit \
                --packages org.postgresql:postgresql:42.6.0 \
                /opt/pyspark_apps/load_to_warehouse.py \
                {table["path"]} \
                jdbc:postgresql://postgres_dw:5432/ecommerce_warehouse \
                dw_user \
                dw_password \
                {table["target_table"]} \
                {ds}
            """
            run_spark_submit(cmd)

    load_gold_products_to_dwh = PythonOperator(
        task_id='load_all_gold_tables_to_postgres',
        python_callable=load_to_dwh,
        provide_context=True,
    )

    dwh_load_complete = EmptyOperator(task_id='dwh_load_complete')
    end = EmptyOperator(task_id='end_pipeline')

    # DAG Dependencies
    start >> bronze_tasks >> bronze_complete
    bronze_complete >> silver_tasks >> silver_complete
    # start >> silver_tasks >> silver_complete
    silver_complete >> gold_tasks >> gold_complete
    gold_complete >> load_gold_products_to_dwh >> dwh_load_complete >> end
