from pyspark.sql.functions import col

class CountryAnalysis:

    @staticmethod
    def get_top_confirmed_countries(df, limit_count=10):

        return (
            df.select(
                "Country/Region",
                "Confirmed"
            )
            .orderBy(
                col("Confirmed").desc()
            )
            .limit(limit_count)
        )