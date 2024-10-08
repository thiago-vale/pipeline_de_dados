import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load
from sparkmeasure import StageMetrics

class ETL(Base):

    def __init__(self):
        self.extractor = Extract('source_to_landing_store')
        self.loader = Load()
        self.metrics = StageMetrics(self.extractor.session())

    def extract(self):
        self.metrics.begin()
        df = self.extractor.csv("/home/thiago/Documentos/GitHub/pipeline_de_dados/data/raw/store.csv")
        print("extract")
        return df

    def transform(self,df):
        return df

    def load(self,df):
        self.loader.parquet(df, 'full', 's3a://datalake-test-thiago/00-landing/spark/store')
        print("load")
        self.metrics.end()
        self.metrics.print_report()

        metrics = ".../metrics/source_to_landing/store/"
        
        df_stage_metrics = self.metrics.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

        df_aggregated_metrics = self.metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")