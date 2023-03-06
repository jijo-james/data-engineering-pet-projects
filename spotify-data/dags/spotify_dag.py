from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from spotify_etl import run_spotify_etl

default_args = {
    'name': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 6),
    'email': ['airflow@sample.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args = default_args,
    description = 'Spotify DAG with ETL process',
    schedule_interval = timedelta(days=1),
)

def check_function():
    print("spotify_dag is running")

run_etl = PythonOperator(
    *,
    python_callable= run_spotify_etl,
    dag= dag,
)

run_etl