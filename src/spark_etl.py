import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from spark_config import SparkConfig
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
        df = spark.read.csv("/home/thiago/Documentos/GitHub/pipeline_de_dados/data/raw/train.csv", header=True, inferSchema=True)
        logger.info("Data loaded successfully")

        # Salvar dados no S3
        df.write.format('parquet').mode('overwrite').save('s3a://datalake-test-thiago/00-landing/spark/train')
        logger.info("Data saved to S3 successfully")

        # Finaliza a SparkSession
        spark.stop()
        logger.info("Spark session stopped")

    except Exception as e:
        logger.error(f"Error during ETL process: {e}")
        raise

if __name__ == "__main__":
    run_etl()

