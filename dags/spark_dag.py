from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Adicione o caminho do seu script ETL
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/src')

# Importe o módulo que contém a classe ETL e a função run_etl
import source_to_landing.train
import source_to_landing.test
import source_to_landing.store
import landing_to_bronze.train
import landing_to_bronze.test
import landing_to_bronze.store
import bronze_to_silver.store_sales
import silver_to_gold.store_sales

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

def source_to_landing_train():
    etl = source_to_landing.train.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def source_to_landing_test():
    etl = source_to_landing.train.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def source_to_landing_store():
    etl = source_to_landing.train.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def landing_to_bronze_train():
    etl = landing_to_bronze.train.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def landing_to_bronze_test():
    etl = landing_to_bronze.test.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def landing_to_bronze_store():
    etl = landing_to_bronze.test.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def bronze_to_silver_store_sales():
    etl = bronze_to_silver.store_sales.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

def silver_to_gold_store_sales():
    etl = silver_to_gold.store_sales.ETL()
    data = etl.extract()
    transformed_data = etl.transform(data)
    etl.load(transformed_data)

source_to_landing_train_0 = PythonOperator(
                                task_id='source_to_landing_train',
                                python_callable=source_to_landing_train,
                                dag=dag,
                                )

source_to_landing_test_0 = PythonOperator(
                                task_id='source_to_landing_test',
                                python_callable=source_to_landing_test,
                                dag=dag,
                                )

source_to_landing_store_0 = PythonOperator(
                                task_id='source_to_landing_store',
                                python_callable=source_to_landing_store,
                                dag=dag,
                                )

landing_to_bronze_train_01 = PythonOperator(
                                task_id='landing_to_bronze_train',
                                python_callable=landing_to_bronze_train,
                                dag=dag,
                                )

landing_to_bronze_test_01 = PythonOperator(
                                task_id='landing_to_bronze_test',
                                python_callable=landing_to_bronze_test,
                                dag=dag,
                                )

landing_to_bronze_store_01 = PythonOperator(
                                task_id='landing_to_bronze_store',
                                python_callable=landing_to_bronze_store,
                                dag=dag,
                                )

bronze_to_silver_store_sales_01 = PythonOperator(
                                task_id='bronze_to_silver_store_sales',
                                python_callable=bronze_to_silver_store_sales,
                                dag=dag,
                                )

silver_to_gold_store_sales_01 = PythonOperator(
                                task_id='silver_to_gold_store_sales',
                                python_callable=silver_to_gold_store_sales,
                                dag=dag,
                                )

[ source_to_landing_train_0 >> landing_to_bronze_train_01 ,
source_to_landing_test_0 >> landing_to_bronze_test_01 ,
source_to_landing_store_0 >> landing_to_bronze_store_01 ] >> bronze_to_silver_store_sales_01 >> silver_to_gold_store_sales_01
