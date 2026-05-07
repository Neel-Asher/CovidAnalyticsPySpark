from config.spark_session import SparkManager
from loaders.data_loader import DataLoader
from transformations.covid_cleaner import CovidCleaner
from analytics.country_analytics import CountryAnalysis
from visualization.plotter import Plotter
from analytics.global_analytics import GlobalAnalysis
from analytics.global_death_trend import GlobalDeathTrend
from analytics.monthly_analytics import MonthlyAnalysis
from analytics.region_analytics import RegionAnalysis
from pyspark.sql.functions import col

spark = SparkManager.create_spark_session()
raw_data_path = "data/raw/"
loader = DataLoader(spark, raw_data_path)
full_grouped_df = loader.load_csv("full_grouped.csv")
covid_clean_df = loader.load_csv("covid_19_clean_complete.csv")
country_latest_df = loader.load_csv("country_wise_latest.csv")
day_wise_df = loader.load_csv("day_wise.csv")
usa_county_df = loader.load_csv("usa_county_wise.csv")
worldometer_df = loader.load_csv("worldometer_data.csv")
missing_count = CovidCleaner.get_missing_province_count(covid_clean_df)
print("Null Province/State Rows:", missing_count)

country_null_report = CovidCleaner.get_country_null_report(covid_clean_df)
print("\nCountry-wise Null Province/State Count")

country_null_report.show(20)
covid_clean_df = CovidCleaner.fill_missing_province(covid_clean_df)
remaining_nulls = CovidCleaner.get_missing_province_count(covid_clean_df)
print("\nRemaining Null Province/State:", remaining_nulls)

full_grouped_df = CovidCleaner.standardize_country_names(full_grouped_df)
country_latest_df = CovidCleaner.standardize_country_names(country_latest_df)
worldometer_df = CovidCleaner.standardize_country_names(worldometer_df)
full_grouped_df = CovidCleaner.clean_country_strings(full_grouped_df)
country_latest_df = CovidCleaner.clean_country_strings(country_latest_df)
worldometer_df = CovidCleaner.clean_country_strings(worldometer_df)
full_grouped_df = CovidCleaner.remove_duplicates(full_grouped_df)
print("\nUnique Countries - full_grouped_df")

full_grouped_df.select("Country/Region").distinct().show(20, False)
before_count = CovidCleaner.get_row_count(full_grouped_df)
print("\nRows Before Removing Duplicates:", before_count)
duplicate_df = CovidCleaner.get_duplicate_records(full_grouped_df)
print("\nDuplicate Country-Date Records")
duplicate_df.show()
full_grouped_df = CovidCleaner.remove_duplicates(full_grouped_df)
after_count = CovidCleaner.get_row_count(full_grouped_df)
print("\nRows After Removing Duplicates:", after_count)
print("\nTotal Duplicate Rows Removed:",before_count - after_count)

top_confirmed_df = (CountryAnalysis.get_top_confirmed_countries(country_latest_df))
Plotter.plot_top_confirmed_countries(top_confirmed_df)
print("\nTop 10 Countries by Confirmed Cases")
top_confirmed_df.show()

top_death_rate_df = CountryAnalysis.get_top_death_rate_countries(country_latest_df)
print("\nTop 10 Countries by Death Rate (Deaths / 100 Cases)")
top_death_rate_df.show()
Plotter.plot_top_death_rate_countries(top_death_rate_df)    

who_region_df = CountryAnalysis.get_who_region_metrics(full_grouped_df)
print("\nWHO Region-wise Total Metrics")
who_region_df.show()
Plotter.plot_who_region_metrics(who_region_df)

print("\nDaily Global New Cases Trend")
daily_trend_df = GlobalAnalysis.get_daily_new_cases_trend(day_wise_df)
GlobalAnalysis.plot_daily_new_cases(daily_trend_df)

print("\nDaily Global Death Growth Trend")
death_trend_df = GlobalDeathTrend.compute_death_growth(day_wise_df)
death_trend_df.show(10)
GlobalDeathTrend.plot_death_growth(death_trend_df)

print("\nMonthly COVID Case Growth")
monthly_df = MonthlyAnalysis.get_monthly_cases(full_grouped_df)
monthly_df.show()
MonthlyAnalysis.plot_monthly_cases(monthly_df)

print("\nTop 5 Countries per WHO Region")
top5_region_df = RegionAnalysis.get_top5_by_region(country_latest_df)
top5_region_df.show(50, False)
RegionAnalysis.plot_top5_by_region(top5_region_df)

print("\nCountry-wise Daily Case Increase")
country_trend_df = CountryAnalysis.compute_daily_increase(full_grouped_df)
country_trend_df.show(10)
CountryAnalysis.plot_country_trend(country_trend_df, "USA")

print("\nCountry Source Comparison")
comparison_df = CountryAnalysis.compare_latest_sources(country_latest_df,worldometer_df)
comparison_df.show(20)
threshold_df = comparison_df.filter((col("confirmed_diff") > 10000)|(col("deaths_diff") > 1000)|(col("recovered_diff") > 10000))
threshold_df.show()

print("\nTop Countries by Infection Rate")
infection_df = CountryAnalysis.compute_infection_rate(worldometer_df, 10)
infection_df.show(truncate=False)
Plotter.plot_infection_rate(infection_df)