import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load
from sparkmeasure import StageMetrics

class ETL(Base):

    def __init__(self):
        self.extractor = Extract('landing_to_bronze_train')
        self.loader = Load()
        self.metrics = StageMetrics(self.extractor.session())
    
    def extract(self):
        self.metrics.begin()
        df = self.extractor.parquet('s3a://datalake-test-thiago/00-landing/spark/train')
        print("extract")
        return df

    def transform(self,df):
        return df

    def load(self,df):
        self.loader.parquet(df, 'full', 's3a://datalake-test-thiago/01-bronze/spark/train')
        print("load")

        self.metrics.end()
        self.metrics.print_report()

        metrics = ".../metrics/landing_to_bronze/train/"
        
        df_stage_metrics = self.metrics.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

        df_aggregated_metrics = self.metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")