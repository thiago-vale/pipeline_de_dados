from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extract():
    df = pd.read_csv('../data/raw/train.csv')
    return df.to_dict()

def transform(ti):
    data = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame(data)
    df = df.drop_duplicates()
    return df.to_dict()

def load(ti):
    data = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(data)
    df.to_csv('../data/trusted/train.csv', index=False)

default_args = {
    'owner' : 'Thiago',
    'depends_on_past' : False,
    'start_date' : datetime(2024,6,13),
    'retries' : 3
}

with DAG( 'etl',
         default_args=default_args,
         description='DAG de ETL Simples',
         schedule_interval='@day',
         catchup=False) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    extract >> transform >> load
