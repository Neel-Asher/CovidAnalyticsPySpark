from pyspark.sql import SparkSession

class SparkManager:
    """Manages the lifecycle and configuration of the SparkSession for the application."""

    @staticmethod
    def create_spark_session():
        """
        Initializes and returns a SparkSession with a predefined configuration.

        The session is configured with:
        - App Name: "COVID PySpark Analytics"
        - Master: "local[*]" (uses all available CPU cores on the local machine)
        - Log Level: "ERROR" (suppresses INFO and WARN messages for cleaner output)

        Returns:
            SparkSession: The entry point to programming Spark with the Dataset 
                and DataFrame API.
        """
        spark = (
            SparkSession.builder
            .appName("COVID PySpark Analytics")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        return spark