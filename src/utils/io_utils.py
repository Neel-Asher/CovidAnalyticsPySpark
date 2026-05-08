import os

def save_df(df, path):
    """
    Converts a Spark DataFrame to Pandas and saves it in both CSV and Parquet formats.

    This utility simplifies the process of exporting processed data for local use. 
    It ensures the target directory exists, performs the conversion to a Pandas 
    DataFrame, and persists the data to the local file system.

    Args:
        df (DataFrame): The Spark DataFrame to be exported.
        path (str): The directory path where the output files should be stored.

    Side Effects:
        - Creates a directory at the specified 'path' if it does not exist.
        - Pulls all data from the Spark cluster to the local driver memory.
        - Writes 'data.csv' and 'data.parquet' to the target directory.
        - Prints the absolute save paths to the console.

    Returns:
        None
    """
    os.makedirs(path, exist_ok=True)

    pandas_df = df.toPandas()

    csv_path = os.path.join(path, "data.csv")
    parquet_path = os.path.join(path, "data.parquet")

    pandas_df.to_csv(csv_path, index=False)
    pandas_df.to_parquet(parquet_path, index=False)

    print(f"Saved CSV to: {csv_path}")
    print(f"Saved Parquet to: {parquet_path}")