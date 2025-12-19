from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.job2_cleaner import clean_and_store_data

default_args = {
    'owner': 'gdacs_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 19),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hourly_gdacs_cleaner',
    default_args=default_args,
    description='Ежечасная очистка данных GDACS из Kafka в SQLite',
    schedule_interval='@hourly',
    catchup=False,
    tags=['gdacs', 'cleaning', 'batch'],
)

clean_task = PythonOperator(
    task_id='clean_and_store_gdacs_data',
    python_callable=clean_and_store_data,
    dag=dag,
)

clean_task