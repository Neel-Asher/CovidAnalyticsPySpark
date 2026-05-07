from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import os


class GlobalDeathTrend:

    @staticmethod
    def compute_death_growth(day_wise_df):

        window_spec = Window.orderBy("Date")

        df = day_wise_df.withColumn(
            "prev_deaths",
            lag("New deaths").over(window_spec)
        )

        df = df.withColumn(
            "death_growth_pct",
            when(
                (col("prev_deaths").isNull()) | (col("prev_deaths") == 0),
                0
            ).otherwise(
                (col("New deaths") / col("prev_deaths")) * 100
            )
        )

        return df.select("Date", "New deaths", "prev_deaths", "death_growth_pct")


    @staticmethod
    def plot_death_growth(df):

        data = df.orderBy("Date").collect()

        dates = [row["Date"] for row in data]
        growth = [row["death_growth_pct"] for row in data]

        os.makedirs("output", exist_ok=True)

        plt.figure(figsize=(12, 6))
        plt.plot(dates, growth)

        plt.title("Daily Global Death Growth % Trend")
        plt.xlabel("Date")
        plt.ylabel("Growth %")
        plt.xticks(rotation=45)
        plt.tight_layout()

        output_path = "output/daily_death_growth_trend.png"
        plt.savefig(output_path)
        plt.close()

        print(f"Saved plot to: {output_path}")