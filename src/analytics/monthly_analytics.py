from pyspark.sql.functions import col, month, sum as _sum
import matplotlib.pyplot as plt
import os


class MonthlyAnalysis:

    @staticmethod
    def get_monthly_cases(full_grouped_df):

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