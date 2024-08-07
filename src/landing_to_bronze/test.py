import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load

class ETL(Base):

    def __init__(self):
        self.extractor = Extract()
        self.loader = Load()

    def extract(self):
        df = self.extractor.parquet('s3a://datalake-test-thiago/00-landing/spark/test')
        print("extract")
        return df

    def transform(self,df):
        return df

    def load(self,df):
        self.loader.parquet(df, 'full', 's3a://datalake-test-thiago/01-bronze/spark/test')
        print("load")