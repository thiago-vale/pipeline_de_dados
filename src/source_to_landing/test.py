import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from etl import Base
from extract import Extract
from load import Load
from sparkmeasure import StageMetrics

class ETL(Base):

    def __init__(self):
        self.metrcis.begin()
        self.extractor = Extract()
        self.loader = Load()
        self.metrcis = StageMetrics(self.extractor.session())


    def extract(self):
        df = self.extractor.csv("/home/thiago/Documentos/GitHub/pipeline_de_dados/data/raw/test.csv")
        print("extract")
        return df

    def transform(self,df):
        return df

    def load(self,df):
        self.loader.parquet(df, 'full', 's3a://datalake-test-thiago/00-landing/spark/test')
        print("load")
        metrics = "/home/thiago/Documentos/GitHub/pipeline_de_dados/metrics/source_to_landing/test/"
        df_stage_metrics = self.metrcis.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

        df_aggregated_metrics = self.metrcis.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")