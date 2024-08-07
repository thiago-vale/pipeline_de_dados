from spark_config import SparkConfig

class Extract():

    def __init__(self):
        self.spark = SparkConfig().spark_config()


    def csv(self, path):

        df = self.spark.read.format('csv').load(path)

        return df

    def parquet(self, path):

        df = self.spark.read.format('parquet').load(path)

        return df
    
    def delta(self, path):

        df = self.spark.read.format('delta').load(path)

        return df