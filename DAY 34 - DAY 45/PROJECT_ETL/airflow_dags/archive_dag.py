from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import shutil

# Archival function
def archive_old_data(base_path, archive_path, retention_days, table_name, include_mysql_dir=True):
    """Archives data older than retention_days for a given table."""
    if include_mysql_dir:
        source_table_path = os.path.join(base_path, "mysql", table_name)
        archive_table_path = os.path.join(archive_path, "mysql", table_name)
    else:
        source_table_path = os.path.join(base_path, table_name)
        archive_table_path = os.path.join(archive_path, table_name)

    os.makedirs(archive_table_path, exist_ok=True)
    cutoff_date = datetime.now() - timedelta(days=retention_days)

    for partition_dir in os.listdir(source_table_path):
        partition_path = os.path.join(source_table_path, partition_dir)
        if not os.path.isdir(partition_path):
            continue

        # Extract date from partition dir like 'processed_date=2025-06-05' or 'report_date=2025-06-05'
        if "=" in partition_dir:
            try:
                _, date_str = partition_dir.split("=")
                partition_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                print(f"Skipping invalid partition directory: {partition_dir}")
                continue
        else:
            print(f"Skipping non-partitioned folder: {partition_dir}")
            continue

        # Check if this date is old enough to archive
        if partition_date <= cutoff_date:
            dest_path = os.path.join(archive_table_path, partition_dir)

            # Clean up existing archive path if it exists
            if os.path.exists(dest_path):
                print(f"Removing existing archive folder: {dest_path}")
                shutil.rmtree(dest_path)

            print(f"Archiving {partition_path} -> {dest_path}")
            shutil.move(partition_path, dest_path)

            # Extra cleanup if source still exists
            if os.path.exists(partition_path):
                print(f"Force removing leftover path: {partition_path}")
                shutil.rmtree(partition_path)

    print(f"Archiving completed for {table_name}.")


# DAG Configuration
ARCHIVE_BASE_PATH = "/opt/data_lake/archive"
BRONZE_BASE_PATH = "/opt/data_lake/bronze"
SILVER_BASE_PATH = "/opt/data_lake/silver"
GOLD_BASE_PATH = "/opt/data_lake/gold"

RETENTION_DAYS_BRONZE = 0
RETENTION_DAYS_SILVER = 0
RETENTION_DAYS_GOLD = 0             # = 365  => Archive data older than 1 year

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_archival_pipeline_all_layers',
    default_args=default_args,
    description='Daily Data Archival for Bronze, Silver, and Gold Layers',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'archive'],
) as dag:

    start = EmptyOperator(task_id='start_archival')

    TABLES = ['orders', 'customers', 'products']

    # Bronze archival tasks
    archive_bronze_tasks = [
        PythonOperator(
            task_id=f'archive_bronze_{table}',
            python_callable=archive_old_data,
            op_kwargs={
                'base_path': BRONZE_BASE_PATH,
                'archive_path': os.path.join(ARCHIVE_BASE_PATH, 'bronze'),
                'retention_days': RETENTION_DAYS_BRONZE,
                'table_name': table,
                'include_mysql_dir': True,
            }
        ) for table in TABLES
    ]

    # Silver archival tasks (table names are suffixed with _cleaned)
    archive_silver_tasks = [
        PythonOperator(
            task_id=f'archive_silver_{table}',
            python_callable=archive_old_data,
            op_kwargs={
                'base_path': SILVER_BASE_PATH,
                'archive_path': os.path.join(ARCHIVE_BASE_PATH, 'silver'),
                'retention_days': RETENTION_DAYS_SILVER,
                'table_name': f'{table}_cleaned',
                'include_mysql_dir': False,
            }
        ) for table in TABLES
    ]

    # Gold archival tasks (table names are suffixed with _summary or similar)
    archive_gold_tasks = [
        PythonOperator(
            task_id=f'archive_gold_{table}',
            python_callable=archive_old_data,
            op_kwargs={
                'base_path': GOLD_BASE_PATH,
                'archive_path': os.path.join(ARCHIVE_BASE_PATH, 'gold'),
                'retention_days': RETENTION_DAYS_GOLD,
                'table_name': f'{table}_summary' if table != 'products' else 'product_category_summary',
                'include_mysql_dir': False,
            }
        ) for table in TABLES
    ]

    end = EmptyOperator(task_id='end_archival')

    # DAG Dependencies
    start >> archive_bronze_tasks >> end
    start >> archive_silver_tasks >> end
    start >> archive_gold_tasks >> end
