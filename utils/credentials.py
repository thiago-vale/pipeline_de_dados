import boto3
import json

class Read():

    def __init__(self):
        self.session = boto3.Session()
        self.secrets_client = self.session.client('secretsmanager')


    def local_aws_credentials(self):
        try:
            # Obtém as credenciais da sessão
            credentials = self.session.get_credentials()
            current_credentials = credentials.get_frozen_credentials()

            return current_credentials.access_key, current_credentials.secret_key

        except Exception as e:
            print(e)

    
    def remote_aws_credentials(self,secret_name):

        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            secret = response['SecretString']
            secret_dict = json.loads(secret)
            return secret_dict

        except Exception as e:
            print(e)