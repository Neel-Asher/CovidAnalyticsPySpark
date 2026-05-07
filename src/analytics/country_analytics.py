from pyspark.sql.functions import col, sum as sum_, lag, when, abs as abs_, desc
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import os
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

    @staticmethod
    def compute_daily_increase(full_grouped_df):

        window_spec = Window.partitionBy("Country/Region").orderBy("Date")

        df = full_grouped_df.withColumn(
            "prev_confirmed",
            lag("Confirmed").over(window_spec)
        )

        df = df.withColumn(
            "daily_increase",
            when(
                col("prev_confirmed").isNull(),
                0
            ).otherwise(
                col("Confirmed") - col("prev_confirmed")
            )
        )

        return df.select(
            "Date",
            "Country/Region",
            "Confirmed",
            "prev_confirmed",
            "daily_increase"
        )

    @staticmethod
    def plot_country_trend(df, country_name):

        country_df = df.filter(col("Country/Region") == country_name).orderBy("Date")

        data = country_df.collect()

        dates = []
        increase = []

        for row in data:
            if row["daily_increase"] is not None:
                dates.append(row["Date"])
                increase.append(row["daily_increase"])

        print(f"Plot points: {len(increase)}")

        os.makedirs("output", exist_ok=True)

        plt.figure(figsize=(12, 6))
        plt.plot(dates, increase)

        plt.title(f"Daily Case Increase Trend - {country_name}")
        plt.xlabel("Date")
        plt.ylabel("Daily Increase")
        plt.xticks(rotation=45)

        plt.tight_layout()

        output_path = f"output/daily_increase_{country_name}.png"
        plt.savefig(output_path)
        plt.close()

        print(f"Saved plot to: {output_path}")

    @staticmethod
    def compare_latest_sources(country_latest_df, worldometer_df):

        df1 = country_latest_df.select(
            col("Country/Region"),
            col("Confirmed").alias("Confirmed_cw"),
            col("Deaths").alias("Deaths_cw"),
            col("Recovered").alias("Recovered_cw")
        )

        df2 = worldometer_df.select(
            col("Country/Region"),
            col("TotalCases").alias("Confirmed_wm"),
            col("TotalDeaths").alias("Deaths_wm"),
            col("TotalRecovered").alias("Recovered_wm")
        )

        joined = df1.join(df2, on="Country/Region", how="inner")

        result = joined.withColumn(
            "confirmed_diff",
            abs_(col("Confirmed_cw") - col("Confirmed_wm"))
        ).withColumn(
            "deaths_diff",
            abs_(col("Deaths_cw") - col("Deaths_wm"))
        ).withColumn(
            "recovered_diff",
            abs_(col("Recovered_cw") - col("Recovered_wm"))
        )

        return result
    
    @staticmethod
    def compute_infection_rate(df, limit=10):

        df_clean = df.filter(
            (col("Population").isNotNull()) &
            (col("Population") > 0)
        )

        result = df_clean.withColumn(
            "infection_rate",
            (col("TotalCases") / col("Population")) * 100
        )

        top = result.orderBy(
            col("infection_rate").desc()
        ).limit(limit)

        return top