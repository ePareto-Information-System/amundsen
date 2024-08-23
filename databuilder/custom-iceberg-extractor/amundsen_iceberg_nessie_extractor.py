# dags/amundsen_iceberg_nessie_extractor.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def run_amundsen_extractor():
    from run_data_builder_job import main
    main()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
}

dag = DAG('amundsen_iceberg_nessie_extractor', default_args=default_args, schedule_interval='@daily')

t1 = PythonOperator(
    task_id='extract_metadata',
    python_callable=run_amundsen_extractor,
    dag=dag,
)
