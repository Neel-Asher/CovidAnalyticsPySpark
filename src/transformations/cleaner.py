from pyspark.sql.functions import col

class CovCleaner:

    @staticmethod
    def fill_missing_province(df):
        return df.fillna({"Province/State": "Unknown"})

    @staticmethod
    def remove_duplicates(df):
        return df.dropDuplicates(["Country/Region", "Date"])