from pyspark.sql.functions import col, month, sum as _sum
import matplotlib.pyplot as plt
import os

class MonthlyAnalysis:
    """Provides methods for aggregating and visualizing COVID-19 data on a monthly basis."""

    @staticmethod
    def get_monthly_cases(full_grouped_df):
        """
        Extracts the month from dates and aggregates total confirmed cases per month.

        Args:
            full_grouped_df (DataFrame): Spark DataFrame containing 'Date' and 'Confirmed' columns.

        Returns:
            DataFrame: Aggregated Spark DataFrame with 'Month' and 'Total Confirmed' columns,
                ordered chronologically by month.
        """

        df = full_grouped_df.withColumn(
            "Month",
            month(col("Date"))
        )

        monthly_df = (
            df.groupBy("Month")
            .agg(_sum("Confirmed").alias("Total Confirmed"))
            .orderBy("Month")
        )

        return monthly_df


    @staticmethod
    def plot_monthly_cases(df):
        """
        Generates and saves a line plot with markers showing monthly confirmed case trends.

        This method collects data from the Spark cluster to the local driver. 
        It is intended for use with the aggregated output of get_monthly_cases.

        Args:
            df (DataFrame): Spark DataFrame containing 'Month' and 'Total Confirmed'.

        Returns:
            None: Saves the plot as a PNG to 'output/monthly_cases_trend.png'.
        """
        data = df.collect()

        months = [row["Month"] for row in data]
        cases = [row["Total Confirmed"] for row in data]

        os.makedirs("output", exist_ok=True)

        plt.figure(figsize=(10, 5))
        plt.plot(months, cases, marker='o')

        plt.title("Monthly COVID Confirmed Cases Trend")
        plt.xlabel("Month")
        plt.ylabel("Total Confirmed Cases")
        plt.xticks(months)
        plt.tight_layout()

        output_path = "output/monthly_cases_trend.png"
        plt.savefig(output_path)
        plt.close()

        print(f"Saved plot to: {output_path}")