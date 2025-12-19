from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.job1_producer import run_producer

default_args = {
    'owner': 'data-team-person1',
    'description': 'GDACS API → Kafka continuous data ingestion',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    'job1_api_ingestion',
    default_args=default_args,
    description='Continuous ingestion of GDACS events from API to Kafka',
    schedule_interval=timedelta(minutes=1),  
    catchup=False,  
    tags=['ingestion', 'api', 'kafka', 'gdacs'],
)

fetch_and_send_task = PythonOperator(
    task_id='fetch_from_api_and_send_to_kafka',
    python_callable=run_producer,
    op_kwargs={'once': True},
    dag=dag,
    queue='default',
)
