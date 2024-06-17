import boto3

class Read():

    def __init__(self):
        pass

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