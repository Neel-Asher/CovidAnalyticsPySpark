from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import os

class GlobalAnalysis:

    @staticmethod
    def get_daily_new_cases_trend(day_wise_df):
        result_df = (
            day_wise_df
            .select("Date", "New cases")
            .orderBy("Date")
        )
        return result_df

    @staticmethod
    def plot_daily_new_cases(df):
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