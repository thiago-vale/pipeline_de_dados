
from abc import ABC, abstractmethod

class Base(ABC):

    """
    An abstract base class for ETL (Extract, Transform, Load) operations.

    This class defines the structure for an ETL process, with abstract methods
    for extracting data, transforming data, and loading data. Subclasses should
    implement these methods.

    Methods:
        extract: Abstract method for extracting data.
        transform: Abstract method for transforming data.
        load: Abstract method for loading data.
    """
    
    def __init__(self):

        """
        Initializes the Base class.

        Since this is an abstract base class, it does not perform any initialization.
        """
        pass

    @abstractmethod
    def extract(self):
        """
        Abstract method to extract data.

        This method should be implemented by subclasses to extract data from a source.
        """
        pass
    
    @abstractmethod
    def transform(self, df):
        """
        Abstract method to transform data.

        This method should be implemented by subclasses to transform the extracted data.

        Args:
            df (DataFrame): The data to be transformed.

        Returns:
            DataFrame: The transformed data.
        """        
        pass
    
    @abstractmethod
    def load(self,df):
        """
        Abstract method to load data.

        This method should be implemented by subclasses to load the transformed data to a destination.

        Args:
            df (DataFrame): The data to be loaded.
        """
        pass
