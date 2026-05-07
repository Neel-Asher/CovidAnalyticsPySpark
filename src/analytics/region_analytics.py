from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank
import matplotlib.pyplot as plt
import os

class RegionAnalysis:

    @staticmethod
    def get_top5_by_region(country_latest_df):

        window_spec = Window.partitionBy("WHO Region") \
                            .orderBy(col("Confirmed").desc())

        ranked_df = country_latest_df.withColumn(
            "rank",
            dense_rank().over(window_spec)
        )

        top5_df = ranked_df.filter(col("rank") <= 5)

        return top5_df


    @staticmethod
    def plot_top5_by_region(df):

        data = df.collect()

        os.makedirs("output", exist_ok=True)

        regions = list(set([row["WHO Region"] for row in data]))

        plt.figure(figsize=(14, 7))

        for region in regions:
            region_data = [r for r in data if r["WHO Region"] == region]
            countries = [r["Country/Region"] for r in region_data]
            cases = [r["Confirmed"] for r in region_data]

            plt.bar([f"{region}-{c}" for c in countries], cases)

        plt.title("Top 5 Countries per WHO Region (Confirmed Cases)")
        plt.xlabel("Country (Grouped by Region)")
        plt.ylabel("Confirmed Cases")
        plt.xticks(rotation=45)

        plt.tight_layout()

        output_path = "output/top5_countries_by_region.png"
        plt.savefig(output_path)
        plt.close()

        print(f"Saved plot to: {output_path}")