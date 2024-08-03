import boto3
from spark_config import SparkConfig

class Write():
        
    def __init__(self):
        pass

    def write_parquet(self,data,mode,path):

        if mode == 'full':
            data.write.format('parquet').mode('overwrite').save(path)
        elif mode == 'incremen':
            data.write.format('parquet').mode('append').save(path)
        else:
            print('mode not found')

    def write_delta(self,data,mode,path):

        if mode == 'full':
            data.write.format('delta').mode('overwrite').save(path)
        elif mode == 'incremen':
            data.write.format('delta').mode('append').save(path)
        else:
            print('mode not found')