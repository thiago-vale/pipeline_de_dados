from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Thiago',
    'depends_on_past': False,
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
    schedule_interval=timedelta(days=1),
)

source_to_landing = BashOperator(
    task_id='source_to_landing',
    bash_command='python /home/thiago/Documentos/GitHub/pipeline_de_dados/src/source_to_landing.py',
    dag=dag,
)

landing_to_bronze = BashOperator(
    task_id='source_to_landing',
    bash_command='python /home/thiago/Documentos/GitHub/pipeline_de_dados/src/landing_to_bronze.py',
    dag=dag,
)

source_to_landing >> landing_to_bronze