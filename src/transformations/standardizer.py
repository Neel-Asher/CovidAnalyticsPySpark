from pyspark.sql.functions import col, when

class CountryStandardizer:

    @staticmethod
    def standardize(df):
        return df.withColumn(
            "Country/Region",
            when(col("Country/Region") == "US", "USA")
            .when(col("Country/Region") == "Korea, South", "South Korea")
            .when(col("Country/Region") == "Russian Federation", "Russia")
            .otherwise(col("Country/Region"))
        )