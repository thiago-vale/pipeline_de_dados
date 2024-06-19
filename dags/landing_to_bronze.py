from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3

def extract():
    s3 = boto3.client('s3')  # Cria um cliente S3

    obj = s3.get_object(Bucket='datalake-test-thiago', Key='00-landing/train.csv')
    df = pd.read_csv(obj['Body'])

    return df.to_dict()

def transform(ti):
    data = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame(data)
    df = df.drop_duplicates()
    return df.to_dict()

def load(ti):
    data = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(data)
    csv_buffer = df.to_parquet(index=False)

    s3 = boto3.client('s3')
    try:
        response = s3.put_object(
            Bucket='datalake-test-thiago',
            Key='01-bronze/pandas/train.parquet',
            Body=csv_buffer
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

with DAG( 'landing_to_bronze',
         default_args=default_args,
         description='pipeline slanding to bronze',
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
