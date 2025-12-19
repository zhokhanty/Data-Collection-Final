from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.job3_analytics import run_analytics

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='job3_daily_summary',
    default_args=default_args,
    description='Daily summary of GDACS events',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['analytics']
) as dag:

    t3 = PythonOperator(
        task_id='run_daily_analytics',
        python_callable=run_analytics
    )

    t3