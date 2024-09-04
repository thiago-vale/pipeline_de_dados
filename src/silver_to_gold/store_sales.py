import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load

import inflection
from pyspark.sql.functions import col, when, isnan, month, year, weekofyear,\
      lit, split, array_contains, to_date, dayofmonth, date_format, expr
from pyspark.sql.types import IntegerType, DateType
from sparkmeasure import StageMetrics

class ETL(Base):

    def __init__(self):
        self.extractor = Extract('silver_to_gold_store_sales')
        self.loader = Load()
        self.metrcis = StageMetrics(self.extractor.session())

    def extract(self):
        self.metrcis.begin()
        df = self.extractor.delta('s3a://datalake-test-thiago/02-silver/delta/store_sales/')
        print("extract")
        return df

    def transform(self,df):
        df = df.dropDuplicates()
        return df

    def load(self,df):
        self.loader.delta(df, 'full', 's3a://datalake-test-thiago/03-gold/delta/store_sales/')
        print("load")

        df.sparkSession.sql("OPTIMIZE delta.`s3a://datalake-test-thiago/03-gold/delta/store_sales/`")
        
        self.metrcis.end()
        self.metrcis.print_report()

        metrics = "s3a://datalake-test-thiago/99-logs/metrics/silver_to_gold/store_sales/"
        
        df_stage_metrics = self.metrcis.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

        df_aggregated_metrics = self.metrcis.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")