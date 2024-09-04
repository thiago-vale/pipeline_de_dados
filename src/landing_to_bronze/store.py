import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load
from sparkmeasure import StageMetrics

class ETL(Base):

    def __init__(self):
        self.extractor = Extract('landing_to_bronze_store')
        self.loader = Load()
        self.metrcis = StageMetrics(self.extractor.session())
    
    def extract(self):
        self.metrcis.begin()
        df = self.extractor.parquet('s3a://datalake-test-thiago/00-landing/spark/store')
        print("extract")
        return df

    def transform(self,df):
        return df

    def load(self,df):
        self.loader.parquet(df, 'full', 's3a://datalake-test-thiago/01-bronze/spark/store')
        print("load")

        self.metrcis.end()
        self.metrcis.print_report()

        metrics = "s3a://datalake-test-thiago/99-logs/metrics/landing_to_bronze/store/"
        
        df_stage_metrics = self.metrcis.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

        df_aggregated_metrics = self.metrcis.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")