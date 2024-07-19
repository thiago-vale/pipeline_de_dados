import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from spark_config import SparkConfig
from pyspark.sql import functions as F
import logging

def run_etl():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Inicializa a SparkSession
        spark_config = SparkConfig()
        spark = spark_config.spark_config()
        logger.info("Spark session created")

        # Carregar dados
        df = spark.read.format('parquet').load('s3a://datalake-test-thiago/02-silver/spark/train')
        logger.info("Data loaded successfully")

        # Trnaformar Dados

        df = df.withColumn('Age_Group',
                        F.when(F.col('Age') < 25, '18-24')
                            .when(F.col('Age').between(25, 34), '25-34')
                            .when(F.col('Age').between(35, 44), '35-44')
                            .when(F.col('Age').between(45, 54), '45-54')
                            .when(F.col('Age') >= 55, '55+')
                            .otherwise('Unknown'))
        
        df = df.withColumn('Vehicle_Age',
                   F.when(F.col('Vehicle_Age') == '> 2 Years', 'over_2_years')
                    .when(F.col('Vehicle_Age') == '1-2 Year', 'between_1_2_year')
                    .when(F.col('Vehicle_Age') == '< 1 Year', 'below_1_year')
                    .otherwise(F.col('Vehicle_Age')))

        df = df.withColumn('Vehicle_Damage',
                        F.when(F.col('Vehicle_Damage') == 'Yes', 1)
                            .when(F.col('Vehicle_Damage') == 'No', 0)
                            .otherwise(F.col('Vehicle_Damage'))
                            .cast('integer'))

        # Salvar dados no S3
        df.write.format('parquet').mode('overwrite').save('s3a://datalake-test-thiago/03-gold/spark/train')
        logger.info("Data saved to S3 successfully")

        # Finaliza a SparkSession
        spark.stop()
        logger.info("Spark session stopped")

    except Exception as e:
        logger.error(f"Error during ETL process: {e}")
        raise

if __name__ == "__main__":
    run_etl()