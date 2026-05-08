from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import os

class GlobalDeathTrend:
    """A collection of analytical methods to track and visualize global death growth rates."""

    @staticmethod
    def compute_death_growth(day_wise_df):
        """
        Calculates the daily percentage growth of new deaths compared to the previous day.

        The growth percentage is calculated as: (New deaths / Previous day's new deaths) * 100.
        If the previous day's deaths are null or zero, the growth percentage is set to 0.

        Args:
            day_wise_df (DataFrame): Spark DataFrame containing 'Date' and 'New deaths' columns.

        Returns:
            DataFrame: A Spark DataFrame with 'Date', 'New deaths', 'prev_deaths', 
                and 'death_growth_pct' columns.
        """
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
        """
        Generates and saves a line plot of the daily global death growth percentage trend.

        This method collects the data to the driver node for plotting. Use on 
        pre-aggregated dataframes to avoid memory issues.

        Args:
            df (DataFrame): Spark DataFrame containing 'Date' and 'death_growth_pct'.

        Returns:
            None: Saves the plot as a PNG to 'output/daily_death_growth_trend.png'.
        """
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