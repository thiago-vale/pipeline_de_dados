import boto3
from spark_config import SparkConfig

class Read():

    def __init__(self):
        self.spark = SparkConfig().spark_config()

    def aws_credentials(self):
        try:
            # Cria uma sessão boto3
            session = boto3.Session()

            # Obtém as credenciais da sessão
            credentials = session.get_credentials()
            current_credentials = credentials.get_frozen_credentials()

            return current_credentials.access_key, current_credentials.secret_key

        except Exception as e:
            print(e)

    def read_parquet(self,path):

        df = self.spark.read.format('parquet').load(path)
        
        return df

    def read_delta(self,path):

        df = self.spark.read.format('delta').load(path)

        return df