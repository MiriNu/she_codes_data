from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ETL.api_imports.zoom import import_zoom

def call_import():
    return 0

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':'datetime(2021,06,01)',
    'retries':3
}
with DAG(
    dag_id='zoom_import',
    default_args=default_args,
    catchup=False,
    schedule_interval='@weekly'
) as dag:

    t1 = PythonOperator(
        task_id='import_zoom_branches',
        python_callable=call_import
    )

    t2 = PythonOperator(
        task_id='import_zoom_classes',
        python_callable=call_import
    )

t1 >> t2