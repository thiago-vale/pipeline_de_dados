from pyspark.sql.functions import *

class Transform():
    """
    A class to perform data transformations on Spark DataFrames.

    Attributes:
        None
    """

    def __init__(self):
        """
        Initializes the Transform class.
        """
        pass

    def drop_columns(self, df, columns):
        """
        Drops specified columns from the DataFrame.

        Args:
            df (DataFrame): The DataFrame to transform.
            columns (list): List of column names to drop.

        Returns:
            DataFrame: The transformed DataFrame with specified columns dropped.
        """
        return df.drop(*columns)

    def rename_columns(self, df, column_map):
        """
        Renames columns in the DataFrame according to a mapping.

        Args:
            df (DataFrame): The DataFrame to transform.
            column_map (dict): Dictionary mapping old column names to new column names.

        Returns:
            DataFrame: The transformed DataFrame with renamed columns.
        """
        for old_name, new_name in column_map.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def add_column(self, df, column_name, expr):
        """
        Adds a new column to the DataFrame with a specified expression.

        Args:
            df (DataFrame): The DataFrame to transform.
            column_name (str): The name of the new column.
            expr (Column): The expression to compute the new column values.

        Returns:
            DataFrame: The transformed DataFrame with the new column added.
        """
        return df.withColumn(column_name, expr)

    def filter_data(self, df, condition):
        """
        Filters rows in the DataFrame based on a condition.

        Args:
            df (DataFrame): The DataFrame to transform.
            condition (Column): The condition to filter rows.

        Returns:
            DataFrame: The filtered DataFrame.
        """
        return df.filter(condition)
    
    def cast_column(self, df, column_name, new_type):
        """
        Casts a column to a new data type.

        Args:
            df (DataFrame): The DataFrame to transform.
            column_name (str): The column to cast.
            new_type (str): The new data type to cast the column to.

        Returns:
            DataFrame: The transformed DataFrame with the column casted.
        """
        return df.withColumn(column_name, col(column_name).cast(new_type))
