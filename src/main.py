from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace

spark = (
    SparkSession.builder
    .appName("COVID PySpark Analytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

raw_data_path = "data/raw/"

def standardize_country(df):
    return df.withColumn(
        "Country/Region",
        when(col("Country/Region") == "US", "USA")
        .when(col("Country/Region") == "Korea, South", "South Korea")
        .when(col("Country/Region") == "Russian Federation", "Russia")
        .when(col("Country/Region") == "Burma", "Myanmar")
        .otherwise(col("Country/Region"))
    )


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

full_grouped_df = standardize_country(full_grouped_df)
country_latest_df = standardize_country(country_latest_df)
worldometer_df = standardize_country(worldometer_df)

def clean_country_strings(df):
    return df.withColumn(
        "Country/Region",
        regexp_replace(col("Country/Region"), r"^\s+|\s+$", "")
    )

full_grouped_df = clean_country_strings(full_grouped_df)
country_latest_df = clean_country_strings(country_latest_df)
worldometer_df = clean_country_strings(worldometer_df)

print("\nUnique Countries - full_grouped_df")
full_grouped_df.select("Country/Region").distinct().show(20, False)

print("\nUnique Countries - worldometer_df")
worldometer_df.select("Country/Region").distinct().show(20, False)