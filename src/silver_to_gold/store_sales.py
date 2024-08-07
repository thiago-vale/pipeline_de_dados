import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load

import inflection
from pyspark.sql.functions import col, when, isnan, month, year, weekofyear,\
      lit, split, array_contains, to_date, dayofmonth, date_format, expr
from pyspark.sql.types import IntegerType, DateType

class ETL(Base):

    def __init__(self):
        self.extractor = Extract()
        self.loader = Load()

    def extract(self):
        df = self.extractor.delta('s3a://datalake-test-thiago/02-silver/delta/store_sales/')
        print("extract")
        return df

    def transform(self,df):
        df = df.dropDuplicates()
        return df

    def load(self,df):
        self.loader.delta(df, 'full', 's3a://datalake-test-thiago/03-gold/delta/store_sales/')
        print("load")