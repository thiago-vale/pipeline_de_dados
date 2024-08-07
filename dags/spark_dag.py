from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/src')
import source_to_landing

default_args = {
    'owner': 'Thiago',
    'depends_on_past': True,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_etl_dag',
    default_args=default_args,
    description='A simple DAG to run Spark ETL script',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

def run_etl():
    etl = source_to_landing.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

source_to_land = PythonOperator(
                    task_id='source_to_landing',
                    python_callable=run_etl,
                    dag=dag,
                )

source_to_land 