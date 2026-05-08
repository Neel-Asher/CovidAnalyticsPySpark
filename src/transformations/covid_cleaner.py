from pyspark.sql.functions import col, when, regexp_replace

class CovidCleaner:
    """Provides advanced data cleaning and validation utilities for COVID-19 datasets."""

    @staticmethod
    def standardize_country_names(df):
        """
        Maps inconsistent country naming conventions to a single standard format.

        This ensures that data joined from different sources (like JHU and Worldometer) 
        aligns correctly (e.g., merging "US" and "USA").

        Args:
            df (DataFrame): Spark DataFrame containing a 'Country/Region' column.

        Returns:
            DataFrame: Spark DataFrame with normalized country names.
        """
        return df.withColumn(
            "Country/Region",
            when(col("Country/Region") == "US", "USA")
            .when(col("Country/Region") == "Korea, South", "South Korea")
            .when(col("Country/Region") == "Russian Federation", "Russia")
            .when(col("Country/Region") == "Burma", "Myanmar")
            .otherwise(col("Country/Region"))
        )

    @staticmethod
    def clean_country_strings(df):
        """
        Trims leading and trailing whitespace from the 'Country/Region' column.

        Args:
            df (DataFrame): Spark DataFrame containing a 'Country/Region' column.

        Returns:
            DataFrame: Spark DataFrame with whitespace-cleaned strings.
        """
        return df.withColumn(
            "Country/Region",
            regexp_replace(
                col("Country/Region"),
                r"^\s+|\s+$",
                ""
            )
        )

    @staticmethod
    def fill_missing_province(df):
        """
        Fills null values in the 'Province/State' column with a placeholder value.

        Args:
            df (DataFrame): Spark DataFrame containing a 'Province/State' column.

        Returns:
            DataFrame: Spark DataFrame where null provinces are replaced by 'Unknown'.
        """
        return df.fillna(
            {"Province/State": "Unknown"}
        )

    @staticmethod
    def get_missing_province_count(df):
        """
        Calculates the total number of records missing 'Province/State' information.

        Args:
            df (DataFrame): Spark DataFrame to analyze.

        Returns:
            int: The total count of null entries in the 'Province/State' column.
        """
        return df.filter(
            col("Province/State").isNull()
        ).count()

    @staticmethod
    def get_country_null_report(df):
        """
        Generates a summary of missing province data grouped by country.

        Args:
            df (DataFrame): Spark DataFrame containing country and province columns.

        Returns:
            DataFrame: Spark DataFrame showing country names and their 
                respective null province counts, sorted descending.
        """
        return (
            df.filter(col("Province/State").isNull())
            .groupBy("Country/Region")
            .count()
            .orderBy("count", ascending=False)
        )

    @staticmethod
    def remove_duplicates(df):
        """
        Removes duplicate entries based on a composite key of country and date.

        Args:
            df (DataFrame): Spark DataFrame with potential duplicate records.

        Returns:
            DataFrame: Spark DataFrame containing only the first occurrence of 
                each country/date pair.
        """
        return df.dropDuplicates(
            ["Country/Region", "Date"]
        )
    
    @staticmethod
    def get_duplicate_records(df):
        """
        Identifies and returns records that have duplicate country/date entries.

        Args:
            df (DataFrame): Spark DataFrame to check for duplicates.

        Returns:
            DataFrame: A Spark DataFrame containing the duplicate keys and 
                the number of times they appear.
        """
        return (
            df.groupBy("Country/Region", "Date")
            .count()
            .filter(col("count") > 1)
        )
    
    @staticmethod
    def get_row_count(df):
        """
        Returns the total number of records currently in the DataFrame.

        Args:
            df (DataFrame): The Spark DataFrame to count.

        Returns:
            int: Total row count.
        """
        return df.count()
    
    @staticmethod
    def add_severity_category(df):
        """
        Classifies countries into severity tiers based on confirmed case counts.

        Tiers:
        - Low: < 10,000 cases
        - Medium: 10,000 - 99,999 cases
        - High: 100,000 - 999,999 cases
        - Critical: >= 1,000,000 cases

        Args:
            df (DataFrame): Spark DataFrame containing a 'Confirmed' column.

        Returns:
            DataFrame: Spark DataFrame with a new 'Severity' column.
        """
        return df.withColumn(
            "Severity",
            when(col("Confirmed") < 10000, "Low")
            .when((col("Confirmed") >= 10000) & (col("Confirmed") < 100000), "Medium")
            .when((col("Confirmed") >= 100000) & (col("Confirmed") < 1000000), "High")
            .otherwise("Critical")
        )