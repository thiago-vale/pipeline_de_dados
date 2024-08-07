import boto3
import json

class Read():

    """
    A class to read credentials either locally or remote.

    Attributes:
        session (boto3.Session): A boto3 session object.
        secrets_client (boto3.client): A boto3 client for AWS Secrets Manager.
    """

    def __init__(self):
        """
        Initializes the Read class, setting up session and secrets client.
        """
        self.session = boto3.Session()
        self.secrets_client = self.session.client('secretsmanager')


    def local_aws_credentials(self):
        """
        Retrieves AWS credentials from the local session.

        Returns:
            tuple: A tuple containing the access key and secret key.

        Raises:
            Exception: If there is an error retrieving the credentials.
        """
        
        try:
            # Obtém as credenciais da sessão
            credentials = self.session.get_credentials()
            current_credentials = credentials.get_frozen_credentials()

            return current_credentials.access_key, current_credentials.secret_key

        except Exception as e:
            print(e)

    
    def remote_aws_credentials(self,secret_name):
        """
        Retrieves AWS credentials from AWS Secrets Manager.

        Args:
            secret_name (str): The name of the secret in AWS Secrets Manager.

        Returns:
            dict: A dictionary containing the secret.

        Raises:
            Exception: If there is an error retrieving the secret.
        """
        
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            secret = response['SecretString']
            secret_dict = json.loads(secret)
            return secret_dict

        except Exception as e:
            print(e)