from pyspark.sql.functions import col, sum as sum_

class CountryAnalysis:

    @staticmethod
    def get_top_confirmed_countries(df, limit_count=10):
        return (
            df.select("Country/Region", "Confirmed")
            .orderBy(col("Confirmed").desc())
            .limit(limit_count)
        )

    @staticmethod
    def get_top_death_rate_countries(df, limit_count=10):
        return (
            df.select("Country/Region", "Deaths / 100 Cases")
            .orderBy(col("Deaths / 100 Cases").desc())
            .limit(limit_count)
        )
    
    @staticmethod
    def get_who_region_metrics(df):

        return (
            df.groupBy("WHO Region")
            .agg(
                sum_("Confirmed").alias("Total Confirmed"),
                sum_("Deaths").alias("Total Deaths"),
                sum_("Recovered").alias("Total Recovered")
            )
            .orderBy(col("Total Confirmed").desc())
        )