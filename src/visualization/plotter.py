import matplotlib.pyplot as plt
import os

class Plotter:

    @staticmethod
    def save_plot(filename):

        output_dir = "output"

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        filepath = os.path.join(output_dir, filename)

        plt.tight_layout()
        plt.savefig(filepath, dpi=300)

        print(f"Saved plot to: {filepath}")

        plt.close()

    @staticmethod
    def plot_top_confirmed_countries(df):
        pandas_df = df.toPandas()

        plt.figure(figsize=(12, 6))
        plt.bar(pandas_df["Country/Region"], pandas_df["Confirmed"])
        plt.title("Top 10 Countries by Confirmed Cases")
        plt.xlabel("Country")
        plt.ylabel("Confirmed Cases")
        plt.xticks(rotation=45)
        plt.tight_layout()
        Plotter.save_plot("top_confirmed_countries.png")

    @staticmethod
    def plot_top_death_rate_countries(df):
        pandas_df = df.toPandas()

        plt.figure(figsize=(12, 6))

        plt.barh(
            pandas_df["Country/Region"],
            pandas_df["Deaths / 100 Cases"]
        )

        plt.title("Top 10 Countries by Death Rate (Deaths per 100 Cases)")
        plt.xlabel("Death Rate")
        plt.ylabel("Country")

        plt.tight_layout()
        Plotter.save_plot("top_death_rate_countries.png")

    @staticmethod
    def plot_who_region_metrics(df):

        pdf = df.toPandas()

        regions = pdf["WHO Region"]

        confirmed = pdf["Total Confirmed"]
        deaths = pdf["Total Deaths"]
        recovered = pdf["Total Recovered"]

        plt.figure(figsize=(12, 6))
        plt.bar(regions, confirmed, label="Confirmed")
        plt.bar(regions, deaths, bottom=confirmed, label="Deaths")
        plt.bar(regions, recovered, bottom=confirmed + deaths, label="Recovered")
        plt.title("WHO Region-wise COVID Metrics (Stacked)")
        plt.xlabel("WHO Region")
        plt.ylabel("Total Cases")
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        Plotter.save_plot("who_region_metrics.png")