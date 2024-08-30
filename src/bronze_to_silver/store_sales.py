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
        self.extractor = Extract('bronze_to_silver_store_sales')
        self.loader = Load()
        self.metrcis = StageMetrics(self.extractor.session())

    def extract(self):
        self.metrcis.begin()
        df_train = self.extractor.parquet('s3a://datalake-test-thiago/01-bronze/spark/train')
        df_store = self.extractor.parquet('s3a://datalake-test-thiago/01-bronze/spark/store')
        df = df_train.join(df_store, on="Store", how="left")
        print("extract")
        return df

    def transform(self,df):
        df = df.dropDuplicates()
        snakecase = lambda x: inflection.underscore(x)
        cols_old = df.columns
        cols_new = list(map(snakecase, cols_old))
        for old_col, new_col in zip(cols_old, cols_new):
            df = df.withColumnRenamed(old_col, new_col)

        df = df.withColumn("competition_distance", when(isnan(col("competition_distance")), 200000.0).otherwise(col("competition_distance")))
        df = df.withColumn("competition_open_since_month", 
                     when(isnan(col("competition_open_since_month")), month(col("date")))
                     .otherwise(col("competition_open_since_month")))
        df = df.withColumn("competition_open_since_year", 
                     when(isnan(col("competition_open_since_year")), year(col("date")))
                     .otherwise(col("competition_open_since_year")))
        df = df.withColumn("promo2_since_week", 
                     when(isnan(col("promo2_since_week")), weekofyear(col("date")))
                     .otherwise(col("promo2_since_week")))
        df = df.withColumn("promo2_since_year", 
                     when(isnan(col("promo2_since_year")), year(col("date")))
                     .otherwise(col("promo2_since_year")))
        month_map = {1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Out', 11: 'Nov', 12: 'Dec'}
        month_map_expr = when(col("date").isNotNull(), col("date").cast("date")).cast(IntegerType())
        for month_num, month_str in month_map.items():
            month_map_expr = when(month(col("date")) == month_num, lit(month_str)).otherwise(month_map_expr)
        df = df.withColumn("month_map", month_map_expr)
        df = df.withColumn("promo_interval", when(col("promo_interval").isNull(), lit("")).otherwise(col("promo_interval")))
        df = df.withColumn("is_promo", 
                            when(col("promo_interval") == "", lit(0))
                            .otherwise(when(array_contains(split(col("promo_interval"), ","), col("month_map")), lit(1)).otherwise(lit(0))))

        df = df.withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))
        df = df.withColumn('competition_open_since_month', col('competition_open_since_month').cast(IntegerType()))
        df = df.withColumn('competition_open_since_year', col('competition_open_since_year').cast(IntegerType()))
        df = df.withColumn('promo2_since_week', col('promo2_since_week').cast(IntegerType()))
        df = df.withColumn('promo2_since_year', col('promo2_since_year').cast(IntegerType()))

        df = df.withColumn('year', year(col('date')))
        df = df.withColumn('month', month(col('date')))
        df = df.withColumn('day', dayofmonth(col('date')))
        df = df.withColumn('week_of_year', weekofyear(col('date')))
        df = df.withColumn('year_week', date_format(col('date'), 'yyyy-ww'))
        df = df.withColumn('competition_since', expr("make_date(competition_open_since_year, competition_open_since_month, 1)"))
        df = df.withColumn('competition_time_month', ((col('date').cast("long") - col('competition_since').cast("long")) / 30).cast(IntegerType()))
        df = df.withColumn('promo_since', expr("date_add(make_date(promo2_since_year, 1, 1), (promo2_since_week - 1) * 7 - 1)"))
        df = df.withColumn('promo_time_week', ((col('date').cast("long") - col('promo_since').cast("long")) / 7).cast(IntegerType()))
        df = df.withColumn('assortment', when(col('assortment') == 'a', 'basic')
                   .when(col('assortment') == 'b', 'extra')
                   .otherwise('extended'))
        df = df.withColumn('state_holiday', when(col('state_holiday') == 'a', 'public_holiday')
                   .when(col('state_holiday') == 'b', 'easter_holiday')
                   .when(col('state_holiday') == 'c', 'christmas')
                   .otherwise('regular_day'))

        print("Data Transform successfully")
        return df

    def load(self,df):
        self.loader.delta(df, 'full', 's3a://datalake-test-thiago/02-silver/delta/store_sales/')
        print("load")
        
        self.metrcis.end()
        self.metrcis.print_report()
        metrics = "/home/thiago/Documentos/GitHub/pipeline_de_dados/metrics/bronze_to_silver/store_sales/"
        df_stage_metrics = self.metrcis.create_stagemetrics_DF("PerfStageMetrics")
        df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

        df_aggregated_metrics = self.metrcis.aggregate_stagemetrics_DF("PerfStageMetrics")
        df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")