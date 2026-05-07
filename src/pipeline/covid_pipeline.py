from loaders.data_loader import DataLoader
from transformations.cleaner import CovCleaner
from transformations.standardizer import CountryStandardizer
from analytics.country_analytics import CountryAnalysis

class CovidPipeline:

    def __init__(self, spark, data_path):
        self.spark = spark
        self.data_path = data_path

    def run(self):

        datasets = DataLoader.load_all(self.spark, self.data_path)

        full_grouped_df = datasets["full_grouped"]
        country_latest_df = datasets["country_latest"]
        covid_clean_df = datasets["covid_clean"]
        day_wise_df = datasets["day_wise"]

        covid_clean_df = CovCleaner.fill_missing_province(covid_clean_df)
        full_grouped_df = CovCleaner.remove_duplicates(full_grouped_df)

        country_latest_df = CountryStandardizer.standardize(country_latest_df)
        full_grouped_df = CountryStandardizer.standardize(full_grouped_df)

        top_countries = CountryAnalysis.get_top_confirmed_countries(country_latest_df)
        death_report = CountryAnalysis.get_top_death_rate_countries(country_latest_df)
        region_summary = CountryAnalysis.get_who_region_metrics(full_grouped_df)
        daily_trend = CountryAnalysis.compute_daily_increase(full_grouped_df)

        # save_df(top_countries, "output/top_countries")
        # save_df(death_report, "output/death_report")
        # save_df(region_summary, "output/region_summary")
        # save_df(daily_trend, "output/daily_trend")

        return {
            "top_countries": top_countries,
            "death_report": death_report,
            "region_summary": region_summary,
            "daily_trend": daily_trend
        }