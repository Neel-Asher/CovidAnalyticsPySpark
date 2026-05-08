from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import os

class GlobalAnalysis:
    """A suite of tools for analyzing and visualizing global-scale COVID-19 trends."""

    @staticmethod
    def get_daily_new_cases_trend(day_wise_df):
        """
        Extracts the chronological trend of new daily COVID-19 cases globally.

        Args:
            day_wise_df (DataFrame): Spark DataFrame containing global daily records 
                with 'Date' and 'New cases' columns.

        Returns:
            DataFrame: A Spark DataFrame containing only 'Date' and 'New cases', 
                sorted by date in ascending order.
        """
        result_df = (
            day_wise_df
            .select("Date", "New cases")
            .orderBy("Date")
        )
        return result_df

    @staticmethod
    def plot_daily_new_cases(df):
        """
        Generates and saves a line plot showing the global daily new cases over time.

        This method collects data to the driver node, so it should be used on 
        aggregated or filtered DataFrames that fit in memory.

        Args:
            df (DataFrame): Spark DataFrame containing 'Date' and 'New cases' columns.

        Returns:
            None: Saves a PNG file named 'daily_new_cases_trend.png' to the 'output' directory.
        """
        data = df.collect()

        dates = [row["Date"] for row in data]
        new_cases = [row["New cases"] for row in data]

        os.makedirs("output", exist_ok=True)

        plt.figure(figsize=(12, 6))
        plt.plot(dates, new_cases)
        plt.title("Daily Global New COVID-19 Cases Trend")
        plt.xlabel("Date")
        plt.ylabel("New Cases")
        plt.xticks(rotation=45)
        plt.tight_layout()

        output_path = "output/daily_new_cases_trend.png"
        plt.savefig(output_path)
        plt.close()

        print(f"Saved plot to: {output_path}")