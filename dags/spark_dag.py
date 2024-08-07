from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Adicione o caminho do seu script ETL
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/src')

# Importe o módulo que contém a classe ETL e a função run_etl
import source_to_landing
import landing_to_bronze

# Defina os argumentos padrão para a DAG
default_args = {
    'owner': 'Thiago',
    'depends_on_past': True,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crie a DAG
dag = DAG(
    'spark_dag',
    default_args=default_args,
    description='A simple DAG to run Spark ETL script',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

# Defina a função run_etl
def run_etl1():
    etl = source_to_landing.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

# Crie o operador Python
source_to_land = PythonOperator(
                                task_id='source_to_landing',
                                python_callable=run_etl1,
                                dag=dag,
                                )
# Defina a função run_etl
def run_etl2():
    etl = landing_to_bronze.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

# Crie o operador Python
land_to_bronze = PythonOperator(
                                task_id='land_to_bronze',
                                python_callable=run_etl2,
                                dag=dag,
                                )

# Defina a ordem das tarefas na DAG
source_to_land >> land_to_bronze