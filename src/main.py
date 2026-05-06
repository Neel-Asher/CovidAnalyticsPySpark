from pyspark.sql import SparkSession

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


print("\nFULL GROUPED DATASET")
full_grouped_df.printSchema()
print("Row Count:", full_grouped_df.count())

print("\nCOVID CLEAN COMPLETE DATASET")
covid_clean_df.printSchema()
print("Row Count:", covid_clean_df.count())

print("\nCOUNTRY WISE LATEST DATASET")
country_latest_df.printSchema()
print("Row Count:", country_latest_df.count())

print("\nDAY WISE DATASET")
day_wise_df.printSchema()
print("Row Count:", day_wise_df.count())

print("\nUSA COUNTY DATASET")
usa_county_df.printSchema()
print("Row Count:", usa_county_df.count())

print("\nWORLDOMETER DATASET")
worldometer_df.printSchema()
print("Row Count:", worldometer_df.count())

spark.stop()