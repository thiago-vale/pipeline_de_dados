from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3

def extract():
    df = pd.read_csv('../data/raw/train.csv')
    return df.to_dict()

def transform(ti):
    data = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame(data)
    return df.to_dict()

def load(ti):
    data = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(data)
    csv_buffer = df.to_parquet(index=False)

    s3 = boto3.client('s3')
    try:
        response = s3.put_object(
            Bucket='datalake-test-thiago',
            Key='00-landing/pandas/train.csv',
            Body=csv_buffer.encode('utf-8')
        )
        print(f'Arquivo train.csv salvo com sucesso no S3.')
    except Exception as e:
        print(f'Erro ao salvar arquivo train.csv no S3: {e}')
        raise e



default_args = {
    'owner' : 'Thiago',
    'depends_on_past' : False,
    'start_date' : datetime(2024,6,13),
    'retries' : 3
}

with DAG( 'source_to_landing',
         default_args=default_args,
         description='pipeline source to landing',
         schedule_interval='3 * * * *',
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
