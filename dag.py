from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email': ['medajyoshna28@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval="*/1 * * * *",
)

runs_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_etl,
    dag=dag,
)

runs_etl

