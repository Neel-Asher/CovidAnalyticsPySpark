from pyspark.sql.functions import col

class TimeAnalysis:
    """Provides methods to identify critical milestones and peak periods in pandemic data."""

    @staticmethod
    def get_pandemic_peaks(df):
        """
        Identifies the specific days representing the global peaks for new cases and new deaths.

        This method sorts the dataset to find the single record with the highest 
        number of 'New cases' and the single record with the highest 'New deaths'.

        Args:
            df (DataFrame): Spark DataFrame containing 'New cases', 'New deaths', 
                and 'Date' columns.

        Returns:
            tuple: A tuple containing two Spark DataFrames:
                - max_cases_row: DataFrame with the single record for the case peak.
                - max_deaths_row: DataFrame with the single record for the death peak.
        """
        max_cases_row = df.orderBy(col("New cases").desc()).limit(1)
        max_deaths_row = df.orderBy(col("New deaths").desc()).limit(1)

        return max_cases_row, max_deaths_row