from pyspark.sql import SparkSession

class SparkManager:

    @staticmethod
    def create_spark_session():

        spark = (
            SparkSession.builder
            .appName("COVID PySpark Analytics")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        return spark