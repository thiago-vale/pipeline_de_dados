import boto3

class Read():

    def __init__(self):
        self.session = boto3.Session()

    def aws_credentials(self):
        try:
            # Obtém as credenciais da sessão
            credentials = self.session.get_credentials()
            current_credentials = credentials.get_frozen_credentials()

            return current_credentials.access_key, current_credentials.secret_key

        except Exception as e:
            print(e)