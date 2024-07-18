#!/usr/bin/python3
"""
The scheduler for running the files
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# Import the other functions from the files


default_args = {
    'owner': 'angelo',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'data_pipeline',
    default_args=default_args,
    description="A binance data pipeline",
    # Running everyday
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
    )

    ingest_task
