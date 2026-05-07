class DataLoader:

    def __init__(self, spark, raw_data_path):
        self.spark = spark
        self.raw_data_path = raw_data_path

    def load_csv(self, file_name):

        return self.spark.read.csv(
            self.raw_data_path + file_name,
            header=True,
            inferSchema=True
        )
    
    @staticmethod
    def load_all(spark, path):

        return {
            "full_grouped": spark.read.csv(path + "full_grouped.csv", header=True, inferSchema=True),
            "country_latest": spark.read.csv(path + "country_wise_latest.csv", header=True, inferSchema=True),
            "covid_clean": spark.read.csv(path + "covid_19_clean_complete.csv", header=True, inferSchema=True),
            "day_wise": spark.read.csv(path + "day_wise.csv", header=True, inferSchema=True),
            "usa_county": spark.read.csv(path + "usa_county_wise.csv", header=True, inferSchema=True),
            "worldometer": spark.read.csv(path + "worldometer_data.csv", header=True, inferSchema=True)
        }