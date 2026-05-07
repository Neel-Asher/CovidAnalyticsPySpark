import matplotlib.pyplot as plt

class Plotter:

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
        plt.show()

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
        plt.show()