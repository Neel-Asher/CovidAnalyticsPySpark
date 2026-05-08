from pyspark.sql.functions import col

class Cleaner:
    """Provides utility methods for cleaning and deduplicating COVID-19 datasets."""

    @staticmethod
    def fill_missing_province(df):
        """
        Handles missing geographic data by filling nulls in the Province/State column.

        In many datasets, national-level data lacks specific province information. 
        This method ensures these nulls are labeled as 'Unknown' to avoid issues 
        during grouping or filtering operations.

        Args:
            df (DataFrame): Spark DataFrame containing a 'Province/State' column.

        Returns:
            DataFrame: Spark DataFrame where null 'Province/State' values are 
                replaced with 'Unknown'.
        """
        return df.fillna({"Province/State": "Unknown"})

    @staticmethod
    def remove_duplicates(df):
        """
        Ensures data integrity by removing duplicate records for the same location and date.

        This is a critical step in time-series data to ensure that metrics like 
        'Total Confirmed' are not accidentally double-counted.

        Args:
            df (DataFrame): Spark DataFrame containing 'Country/Region' and 'Date' columns.

        Returns:
            DataFrame: Spark DataFrame containing unique records based on the 
                combination of Country/Region and Date.
        """
        return df.dropDuplicates(["Country/Region", "Date"])