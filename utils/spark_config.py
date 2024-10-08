from pyspark.sql import SparkSession
from pyspark import SparkConf
from delta import *
from delta.tables import *
import os
import sys
sys.path.append('/home/thiago/Documentos/GitHub/pipeline_de_dados/utils')

from credentials import Read


class SparkConfig():
    """
    A class to configure and create a SparkSession with Delta Lake integration.
    """

    def __init__(self):
        """
        Initializes the SparkConfig class, setting up the credentials reader.
        """
        self.read = Read()

    def spark_config(self,appname):

        """
        Configures and creates a SparkSession with Delta Lake support.

        Args:
            appname (str): The App Name.

        Returns:
            SparkSession: A configured SparkSession object.
            
        """
        
        aws_key , aws_pass = self.read.local_aws_credentials()

        config = {
            'aws_access_key_id': aws_key,
            'aws_secret_access_key': aws_pass,
            'bucket': 'datalake-test-thiago',
        }
        
        
        spark_jars = '''
            /home/thiago/Documentos/GitHub/pipeline_de_dados/utils/spark_jars/aws-java-sdk-1.7.4.jar,
            /home/thiago/Documentos/GitHub/pipeline_de_dados/utils/spark_jars/hadoop-aws-2.7.7.jar,
            /home/thiago/Documentos/GitHub/pipeline_de_dados/utils/spark_jars/jets3t-0.9.4.jar,
            /home/thiago/Documentos/GitHub/pipeline_de_dados/utils/spark_jars/spark-measure_2.12-0.24.jar
            '''

        conf = (
                SparkConf()
                .set('spark.sql.repl.eagerEval.enabled', True)
                .set('spark.sql.execution.arrow.pyspark.enabled', True)
                .set('spark.sql.session.timeZone', 'UTC')
                .set('spark.sql.parquet.int96RebaseModeInRead', 'LEGACY')
                .set('spark.sql.parquet.int96RebaseModeInWrite', 'LEGACY')
                .set('spark.sql.parquet.datetimeRebaseModeInRead', 'LEGACY')
                .set('spark.sql.parquet.datetimeRebaseModeInWrite', 'LEGACY')
                .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .set('spark.network.timeout', '100000000')
                .set('spark.executor.heartbeatInterval', '100000000')
                .set('spark.executor.memory', '12G')
                .set('spark.driver.memory', '4G')
                .set('spark.memory.offHeap.enabled', 'true')
                .set('spark.memory.offHeap.size', '4G' )
                .set('spark.sql.autoBroadcastJoinThreshold', '-1')
                .set('spark.sql.broadcastTimeout', '300000')  
                .set('spark.executor.cores', '4')
                .set('spark.executor.instances', '2')
                .set('spark.default.parallelism', '2')
                .set('spark.sql.debug.maxToStringFields', 1000)
                .set('spark.kryoserializer.buffer.max', 2047)
                .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
                .set('spark.jars', spark_jars)
                .set('spark.ui.showConsoleProgress', True)
                .set('spark.logConf', True)
                .set('spark.driver.bindAddress', '0.0.0.0')
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set('spark.delta.logStore.class', 'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore')
                .set('spark.sql.parquet.compression.codec', 'snappy')
                .set("spark.eventLog.enabled", True)
                .set("spark.eventLog.dir", "/home/thiago/Documentos/GitHub/pipeline_de_dados/logs/spark-events")
                .set("spark.eventLog.logBlockUpdates.enabled", "true")
                .set("spark.eventLog.jsonFormat.enabled", "true")
            )

        spark = (
                SparkSession
                .builder
                .config(conf=conf)
                .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
                .master('local[*]')
                .appName(appname)
            )
        
        spark = configure_spark_with_delta_pip(spark).getOrCreate()

        spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", config['aws_access_key_id'])
        spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", config['aws_secret_access_key'])

        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "us-east-1.amazonaws.com")
        spark._jsc.hadoopConfiguration().set("fs.s3.fast.upload", "true") 
        spark._jsc.hadoopConfiguration().set("fs.s3.buffer.dir", "/tmp")

        return spark
