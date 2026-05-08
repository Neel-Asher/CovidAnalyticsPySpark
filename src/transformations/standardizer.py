from pyspark.sql.functions import col, when

class CountryStandardizer:
    """Provides functionality to normalize country and region names across datasets."""

    @staticmethod
    def standardize(df):
        """
        Normalizes specific country/region names to a common standard format.

        This method maps various naming conventions (e.g., 'US', 'Russian Federation') 
        to a single preferred name (e.g., 'USA', 'Russia'). Standardizing these 
        values is essential for accurate joins and aggregations across multiple 
        COVID-19 data sources.

        Args:
            df (DataFrame): Spark DataFrame containing a 'Country/Region' column.

        Returns:
            DataFrame: A Spark DataFrame with standardized country names in the 
                'Country/Region' column.
        """
        return df.withColumn(
            "Country/Region",
            when(col("Country/Region") == "US", "USA")
            .when(col("Country/Region") == "Korea, South", "South Korea")
            .when(col("Country/Region") == "Russian Federation", "Russia")
            .otherwise(col("Country/Region"))
        )