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