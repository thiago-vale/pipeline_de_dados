from spark_config import SparkConfig

class Extract():
    """
    A class to extract data from local files, databases, or storages.

    Attributes:
        spark (SparkSession): A SparkSession object configured using SparkConfig.
    """

    def __init__(self):
        """
        Initializes the Extract class, setting up the Spark configuration.
        """
        self.spark = SparkConfig().spark_config()

    def session(self):
        """
        Returns the SparkSession object.
        """
        return self.spark
    
    def csv(self, path):
        """
        Reads a CSV file from the specified path and returns it as a DataFrame.

        Args:
            path (str): The path to the CSV file.

        Returns:
            DataFrame: The data read from the CSV file.
        """
        df = self.spark.read.csv(path, header=True, inferSchema=True)
        return df

    def parquet(self, path):
        """
        Reads a Parquet file from the specified path and returns it as a DataFrame.

        Args:
            path (str): The path to the Parquet file.

        Returns:
            DataFrame: The data read from the Parquet file.
        """
        df = self.spark.read.format('parquet').load(path)

        return df
    
    def delta(self, path):
        """
        Reads a Delta file from the specified path and returns it as a DataFrame.

        Args:
            path (str): The path to the Delta file.

        Returns:
            DataFrame: The data read from the Delta file.
        """
        df = self.spark.read.format('delta').load(path)

        return df