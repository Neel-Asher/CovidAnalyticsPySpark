from pyspark.sql.functions import col

class TimeAnalysis:

    @staticmethod
    def get_pandemic_peaks(df):

        max_cases_row = df.orderBy(col("New cases").desc()).limit(1)
        max_deaths_row = df.orderBy(col("New deaths").desc()).limit(1)

        return max_cases_row, max_deaths_row