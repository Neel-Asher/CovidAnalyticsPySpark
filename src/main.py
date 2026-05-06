from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("COVID PySpark Analytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

raw_data_path = "data/raw/"

full_grouped_df = spark.read.csv(
    raw_data_path + "full_grouped.csv",
    header=True,
    inferSchema=True
)

covid_clean_df = spark.read.csv(
    raw_data_path + "covid_19_clean_complete.csv",
    header=True,
    inferSchema=True
)

country_latest_df = spark.read.csv(
    raw_data_path + "country_wise_latest.csv",
    header=True,
    inferSchema=True
)

day_wise_df = spark.read.csv(
    raw_data_path + "day_wise.csv",
    header=True,
    inferSchema=True
)

usa_county_df = spark.read.csv(
    raw_data_path + "usa_county_wise.csv",
    header=True,
    inferSchema=True
)

worldometer_df = spark.read.csv(
    raw_data_path + "worldometer_data.csv",
    header=True,
    inferSchema=True
)

missing_count = covid_clean_df.filter(
    col("Province/State").isNull()
).count()

print("Null Province/State Rows:", missing_count)

null_report_df = covid_clean_df.filter(
    col("Province/State").isNull()
)

country_null_report = (
    null_report_df
    .groupBy("Country/Region")
    .count()
    .orderBy("count", ascending=False)
)

print("\nCountry-wise Null Province/State Count")
country_null_report.show(20)

covid_clean_df = covid_clean_df.fillna(
    {"Province/State": "Unknown"}
)

remaining_nulls = covid_clean_df.filter(
    col("Province/State").isNull()
).count()

print("\nRemaining Null Province/State:", remaining_nulls)