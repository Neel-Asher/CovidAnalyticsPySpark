from pyspark.sql.functions import col, when, regexp_replace

class CovidCleaner:

    @staticmethod
    def standardize_country_names(df):

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

        return df.fillna(
            {"Province/State": "Unknown"}
        )

    @staticmethod
    def get_missing_province_count(df):

        return df.filter(
            col("Province/State").isNull()
        ).count()

    @staticmethod
    def get_country_null_report(df):

        return (
            df.filter(col("Province/State").isNull())
            .groupBy("Country/Region")
            .count()
            .orderBy("count", ascending=False)
        )

    @staticmethod
    def remove_duplicates(df):

        return df.dropDuplicates(
            ["Country/Region", "Date"]
        )
    
    @staticmethod
    def get_duplicate_records(df):

        return (
            df.groupBy("Country/Region", "Date")
            .count()
            .filter(col("count") > 1)
        )
    
    @staticmethod
    def get_row_count(df):

        return df.count()