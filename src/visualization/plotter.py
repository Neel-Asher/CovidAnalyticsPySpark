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

    @staticmethod
    def plot_infection_rate(df):
        pdf = df.select("Country/Region", "infection_rate").toPandas()

        plt.figure(figsize=(12,6))
        plt.bar(pdf["Country/Region"], pdf["infection_rate"])

        plt.xticks(rotation=45)
        plt.title("Top 10 Countries by Infection Rate")
        plt.ylabel("Infection Rate (%)")

        plt.tight_layout()
        plt.savefig("output/infection_rate.png")
        plt.close()

        print("Saved plot to: output/infection_rate.png")

    @staticmethod
    def plot_statewise_counties(df):

        pdf = df.toPandas()

        plt.figure(figsize=(14,6))
        plt.bar(pdf["Province_State"], pdf["County_Count"])

        plt.xticks(rotation=90)
        plt.title("USA State-wise County Coverage")
        plt.ylabel("Number of Counties")

        plt.tight_layout()
        plt.savefig("output/usa_state_counties.png")
        plt.close()

        print("Saved plot to: output/usa_state_counties.png")
    
    @staticmethod
    def plot_geo_clusters(df):

        pdf = df.toPandas()

        plt.figure(figsize=(12,6))

        plt.scatter(
            pdf["Long"],
            pdf["Lat"],
            s=pdf["Confirmed"] / 1000,   # scale size
            alpha=0.5
        )

        plt.title("COVID-19 Global Case Clusters")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")

        plt.tight_layout()

        plt.savefig("output/geo_clusters.png")
        plt.close()

        print("Saved plot to: output/geo_clusters.png")
    
    @staticmethod
    def plot_recovery_rates(df, title, filename):

        pdf = df.toPandas()

        plt.figure(figsize=(12,6))
        plt.bar(pdf["Country/Region"], pdf["recovery_rate"])

        plt.xticks(rotation=45)
        plt.title(title)
        plt.ylabel("Recovery Rate (%)")

        plt.tight_layout()
        plt.savefig(f"output/{filename}.png")
        plt.close()

        print(f"Saved plot to: output/{filename}.png")

    @staticmethod
    def plot_high_risk_countries(df):

        pdf = df.toPandas()

        plt.figure(figsize=(12,6))
        plt.bar(pdf["Country/Region"], pdf["Active"])

        plt.xticks(rotation=45)
        plt.title("High Risk Countries (Active Cases > Recovered)")
        plt.ylabel("Active Cases")

        plt.tight_layout()
        plt.savefig("output/high_risk_countries.png")
        plt.close()

        print("Saved plot to: output/high_risk_countries.png")

    @staticmethod
    def plot_peaks(df):

        pdf = df.toPandas().sort_values("Date")

        max_cases_date = pdf.loc[pdf["New cases"].idxmax(), "Date"]
        max_cases_value = pdf["New cases"].max()

        max_deaths_date = pdf.loc[pdf["New deaths"].idxmax(), "Date"]
        max_deaths_value = pdf["New deaths"].max()

        plt.figure(figsize=(14,6))

        plt.plot(pdf["Date"], pdf["New cases"], label="New Cases")
        plt.plot(pdf["Date"], pdf["New deaths"], label="New Deaths")

        # Peak markers
        plt.scatter(max_cases_date, max_cases_value, color="red", s=100, label="Max Cases Peak")
        plt.scatter(max_deaths_date, max_deaths_value, color="black", s=100, label="Max Deaths Peak")

        plt.xticks(rotation=45)
        plt.title("Pandemic Peaks Detection")
        plt.legend()

        plt.tight_layout()
        plt.savefig("output/pandemic_peaks.png")
        plt.close()

        print("Saved plot to: output/pandemic_peaks.png")