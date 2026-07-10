from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/extraction')

from cdc_extractor import run as run_extraction

default_args = {
    'owner': 'robi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='cdc_health_pipeline',
    default_args=default_args,
    description='Weekly CDC NNDSS ingestion and transformation pipeline',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cdc', 'health', 'bigquery']
) as dag:

    extract_and_load = PythonOperator(
        task_id='extract_and_load_to_bigquery',
        python_callable=run_extraction
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir /opt/airflow/dbt_project'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir /opt/airflow/dbt_project'
    )

    extract_and_load >> dbt_run >> dbt_test
