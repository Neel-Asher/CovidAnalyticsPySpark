class DataLoader:
    """Handles the ingestion of COVID-19 CSV datasets into Spark DataFrames."""

    def __init__(self, spark, raw_data_path):
        """
        Initializes the DataLoader with a Spark session and base directory path.

        Args:
            spark (SparkSession): The active Spark session used for reading data.
            raw_data_path (str): The base directory path where CSV files are located.
        """
        self.spark = spark
        self.raw_data_path = raw_data_path

    def load_csv(self, file_name):
        """
        Loads a single specific CSV file into a Spark DataFrame.

        This method automatically treats the first row as a header and attempts 
        to infer the data types of each column.

        Args:
            file_name (str): The name of the file (e.g., 'data.csv') to be appended 
                to the raw_data_path.

        Returns:
            DataFrame: A Spark DataFrame containing the loaded data.
        """
        return self.spark.read.csv(
            self.raw_data_path + file_name,
            header=True,
            inferSchema=True
        )
    
    @staticmethod
    def load_all(spark, path):
        """
        Batch loads all core pandemic datasets into a structured dictionary.

        This is a convenience method to initialize the entire analytical 
        environment in one call.

        Args:
            spark (SparkSession): The active Spark session.
            path (str): The base directory path containing the predefined CSV files.

        Returns:
            dict: A dictionary where keys are dataset identifiers (e.g., 'worldometer') 
                and values are the corresponding Spark DataFrames.
        """
        return {
            "full_grouped": spark.read.csv(path + "full_grouped.csv", header=True, inferSchema=True),
            "country_latest": spark.read.csv(path + "country_wise_latest.csv", header=True, inferSchema=True),
            "covid_clean": spark.read.csv(path + "covid_19_clean_complete.csv", header=True, inferSchema=True),
            "day_wise": spark.read.csv(path + "day_wise.csv", header=True, inferSchema=True),
            "usa_county": spark.read.csv(path + "usa_county_wise.csv", header=True, inferSchema=True),
            "worldometer": spark.read.csv(path + "worldometer_data.csv", header=True, inferSchema=True)
        }