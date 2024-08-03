from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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

source_to_landing = BashOperator(
    task_id='source_to_landing',
    bash_command='python3 /home/thiago/Documentos/GitHub/pipeline_de_dados/src/00_source_to_landing.py',
    dag=dag,
)

landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',
    bash_command='python3 /home/thiago/Documentos/GitHub/pipeline_de_dados/src/01_landing_to_bronze.py',
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='python3 /home/thiago/Documentos/GitHub/pipeline_de_dados/src/02_bronze_to_silver.py',
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command='python3 /home/thiago/Documentos/GitHub/pipeline_de_dados/src/03_silver_to_gold.py',
    dag=dag,
)

source_to_landing >> landing_to_bronze >> bronze_to_silver >> silver_to_gold