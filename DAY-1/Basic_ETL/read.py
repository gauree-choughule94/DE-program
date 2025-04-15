import pandas as pd
import os
from typing import Dict
import requests
from logger import log_error, log_warning, log_success


BEARER_TOKEN = "dc4cff04-06d4-4549-ac19-b456b49c5586"
HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Accept": "application/json",
}

API_ENDPOINTS = {
    "users": "https://fabricate.mockaroo.com/api/v1/databases/report/api/users",
    "payments": "https://fabricate.mockaroo.com/api/v1/databases/report/api/payments",
    "products": "https://fabricate.mockaroo.com/api/v1/databases/report/api/products",
    "details": "https://fabricate.mockaroo.com/api/v1/databases/report/api/details",
}


def fetch_api_data() -> Dict[str, pd.DataFrame]:
    """
    Fetches data from all API endpoints and returns them as a dictionary of DataFrames.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary where keys are dataset names and values are DataFrames.
    """
    api_data = {}
    for key, url in API_ENDPOINTS.items():
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                api_data[key] = pd.DataFrame(response.json())
                log_success(f"Successfully fetched data from {key} API.")
            else:
                log_warning(f"Failed to fetch {key} data. Status Code: {response.status_code}")
                api_data[key] = pd.DataFrame()
        except Exception as e:
            log_error(f"Error occurred while fetching {key} data: {e}")
            api_data[key] = pd.DataFrame()

    return api_data


def read_data_from_db(table_name, engine):
    """
    Reads data from the database table.

    Args:
        table_name (str): The name of the database table to read from.
        engine: SQLAlchemy engine object.

    Returns:
        DataFrame: Pandas DataFrame containing the table data.
    """
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=engine)
        log_success(f"Successfully read data from '{table_name}' table.")
        return df
    except Exception as e:
        log_error(f"Error reading from table '{table_name}': {e}")
        return pd.DataFrame()

CSV_DIR = "csv_files"

def read_all_files() -> Dict[str, pd.DataFrame]:
    """
    Reads all CSV files from their respective directories 
    and returns them as a dictionary of DataFrames.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary where keys are file names and values are DataFrames.
    """
    dataframes = {}

    if not os.path.exists(CSV_DIR):
        log_warning(f"{CSV_DIR} directory does not exist.")
        return dataframes

    files = os.listdir(CSV_DIR)
    if not files:
        log_warning(f"No files found in {CSV_DIR}.")
        return dataframes

    for file in files:
        if file.endswith(".csv"):
            try:
                file_path = os.path.join(CSV_DIR, file)
                df = pd.read_csv(file_path)
                dataframes[file] = df
                log_success(f"Successfully read {file}.")
            except Exception as e:
                log_error(f"Failed to read {file}: {e}")

    return dataframes
