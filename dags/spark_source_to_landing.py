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

run_etl = BashOperator(
    task_id='run_etl_script',
    bash_command='python /home/thiago/Documentos/GitHub/pipeline_de_dados/src/spark_etl.py',
    dag=dag,
)

run_etl