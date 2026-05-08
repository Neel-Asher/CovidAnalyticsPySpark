from loaders.data_loader import DataLoader
from transformations.cleaner import Cleaner
from transformations.standardizer import CountryStandardizer
from analytics.country_analytics import CountryAnalysis
from utils.io_utils import save_df

class CovidPipeline:
    """
    Orchestrates the end-to-end ETL and analytics workflow for COVID-19 datasets.

    This pipeline handles the data lifecycle from ingestion and cleaning to 
    advanced analytics and final persistence of results.
    """

    def __init__(self, spark, data_path):
        """
        Initializes the pipeline with a Spark session and the source data directory.

        Args:
            spark (SparkSession): The active Spark session for processing.
            data_path (str): The directory path where raw CSV files are stored.
        """
        self.spark = spark
        self.data_path = data_path

    def run(self):
        """
        Executes the full data pipeline.

        The execution flow includes:
        1. **Ingestion**: Loading multiple datasets via DataLoader.
        2. **Cleaning**: Handling missing values and removing duplicates.
        3. **Standardization**: Normalizing country names across datasets.
        4. **Analytics**: Computing top countries, death rates, and regional metrics.
        5. **Persistence**: Saving the resulting DataFrames to the output directory.

        Returns:
            dict: A dictionary containing the final Spark DataFrames:
                - 'top_countries': Top countries by confirmed cases.
                - 'death_report': Countries with highest death rates.
                - 'region_summary': Metrics grouped by WHO region.
                - 'daily_trend': Day-over-day increases in cases.
        """
        datasets = DataLoader.load_all(self.spark, self.data_path)

        full_grouped_df = datasets["full_grouped"]
        country_latest_df = datasets["country_latest"]
        covid_clean_df = datasets["covid_clean"]
        day_wise_df = datasets["day_wise"]

        covid_clean_df = Cleaner.fill_missing_province(covid_clean_df)
        full_grouped_df = Cleaner.remove_duplicates(full_grouped_df)

        country_latest_df = CountryStandardizer.standardize(country_latest_df)
        full_grouped_df = CountryStandardizer.standardize(full_grouped_df)

        top_countries = CountryAnalysis.get_top_confirmed_countries(country_latest_df)
        death_report = CountryAnalysis.get_top_death_rate_countries(country_latest_df)
        region_summary = CountryAnalysis.get_who_region_metrics(full_grouped_df)
        daily_trend = CountryAnalysis.compute_daily_increase(full_grouped_df)

        save_df(top_countries, "output/top_countries")
        save_df(death_report, "output/death_report")
        save_df(region_summary, "output/region_summary")
        save_df(daily_trend, "output/daily_trend")

        return {
            "top_countries": top_countries,
            "death_report": death_report,
            "region_summary": region_summary,
            "daily_trend": daily_trend
        }