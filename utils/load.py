import logging

class Load():
    """
    A class to load data into different formats (CSV, Parquet, Delta) with specified modes.

    Attributes:
        logger (logging.Logger): A logger for logging information and errors.
    """

    def __init__(self):
        """
        Initializes the Load class, setting up the logger configuration.
        """
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def csv(self,df, mode, path):
        """
        Writes a DataFrame to a CSV file with the specified mode.

        Args:
            df (DataFrame): The DataFrame to write.
            mode (str): The write mode ('incremen' for append, 'full' for overwrite).
            path (str): The path where the CSV file will be saved.

        """
        if mode == 'incremen':

            df.write.format("csv").mode('append').save(path)
        
        elif mode == 'full':

            df.write.format("csv").mode('overwrite').save(path)

        else:

            self.logger.info("mode not found")
    
    def parquet(self, df, mode, path):
        """
        Writes a DataFrame to a Parquet file with the specified mode.

        Args:
            df (DataFrame): The DataFrame to write.
            mode (str): The write mode ('incremen' for append, 'full' for overwrite).
            path (str): The path where the Parquet file will be saved.
        """
        if mode == 'incremen':

            df.write.format("parquet").mode('append').save(path)
        
        elif mode == 'full':

            df.write.format("parquet").mode('overwrite').save(path)

        else:
            
            self.logger.info("mode not found")
    
    def delta(self, df, mode, path):
        """
        Writes a DataFrame to a Delta file with the specified mode.

        Args:
            df (DataFrame): The DataFrame to write.
            mode (str): The write mode ('incremen' for append, 'full' for overwrite).
            path (str): The path where the Delta file will be saved.
        """
        if mode == 'incremen':

            df.write.format("delta").mode('append').save(path)
        
        elif mode == 'full':

            df.write.format("delta").mode('overwrite').save(path)

        else:
            
            self.logger.info("mode not found")