import pandas as pd
from logger import log_success, log_error, log_warning
from typing import Tuple, Dict, Any
from pandas import DataFrame


COLUMN_ORDER = {
    "users": ["id", "name", "gender", "location", "email"],
    "payments": ["id", "product_id", "state", "bank_name", "date", "price"],
    "products": ["id", "user_id", "product_category", "product_name", "quantity"],
    "details": ["id", "payment_id", "country", "order_id"],
}

def clean_data(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Cleans and transforms the dataframes by:
    - Selecting only required columns
    - Removing duplicates
    - Dropping missing values (NaN)
    """
    cleaned_data = {}
    
    for key, df in dataframes.items():
        if df.empty:
            log_warning(f"No data found in '{key}' DataFrame. Skipping...")
            continue

        try:
            original_shape = df.shape
            df = df[[col for col in COLUMN_ORDER[key] if col in df.columns]]

            df.drop_duplicates(inplace=True)
            df.dropna(inplace=True)

            cleaned_shape = df.shape
            cleaned_data[key] = df

            log_success(f"Cleaned '{key}' data: {original_shape} -> {cleaned_shape}")
        except Exception as e:
            log_error(f"Error cleaning '{key}' data: {e}")

    return cleaned_data


def process_data(dfs: Dict[str, DataFrame]) -> Tuple[Any, Any, float, Any]:
    """
    Processes the provided DataFrames to extract insights.
    """
    expensive_users = []
    users_from_cluj = []
    total_price = 0
    jpmorgan_users = []

    try:
        payments_df = dfs["payments.csv"]
        products_df = dfs["products.csv"]
        users_df = dfs["users.csv"]

        payments_df["price"] = payments_df["price"].astype(float)
        total_price = payments_df["price"].sum()
        log_success(f"Total price calculated: {total_price}")

        expensive = payments_df[payments_df["price"] > 25.00]
        user_ids = products_df[products_df["id"].isin(expensive["product_id"])]["user_id"]
        expensive_users_df = users_df[users_df["id"].isin(user_ids)][["id", "name"]]
        expensive_users = expensive_users_df.to_records(index=False)
        log_success(f"Found {len(expensive_users)} expensive users.")

        cluj_users_df = users_df[users_df["location"] == "Cluj-Napoca"][["id", "name"]]
        users_from_cluj = cluj_users_df.to_records(index=False)
        log_success(f"Found {len(users_from_cluj)} users from Cluj-Napoca.")

        jpmorgan_orders = payments_df[payments_df["bank_name"] == "JPMORGAN CHASE"]
        jpm_user_ids = products_df[products_df["id"].isin(jpmorgan_orders["product_id"])]["user_id"]
        jpm_users_df = users_df[users_df["id"].isin(jpm_user_ids)][["id", "name"]]
        jpmorgan_users = jpm_users_df.to_records(index=False)
        log_success(f"Found {len(jpmorgan_users)} JPMorgan users.")

    except KeyError as ke:
        log_error(f"Missing key in input DataFrames: {ke}")
    except Exception as e:
        log_error(f"Error during transformation: {e}")

    return expensive_users, users_from_cluj, total_price, jpmorgan_users


def filter_data(df: pd.DataFrame, column: str, value) -> pd.DataFrame:
    """
    Filters rows where column == value.

    Args:
        df: DataFrame to filter
        column: Column name to filter on
        value: Value to filter for

    Returns:
        Filtered DataFrame
    """
    try:
        filtered_df = df[df[column] == value]
        log_success(f"Filtered data where {column} = {value}")
        return filtered_df
    except Exception as e:
        log_error(f"Error filtering data: {e}")
        return df


def append_data(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Appends df2 to df1.

    Args:
        df1: Base DataFrame
        df2: DataFrame to append

    Returns:
        Combined DataFrame
    """
    try:
        result_df = pd.concat([df1, df2], ignore_index=True)
        log_success("DataFrames appended successfully.")
        return result_df
    except Exception as e:
        log_error(f"Error appending data: {e}")
        return df1


def delete_data(df: pd.DataFrame, column: str, value, condition: str = "equals") -> pd.DataFrame:
    """
    Deletes rows based on a condition.

    Args:
        df: DataFrame to process
        column: Column name
        value: Value to match for deletion
        condition: Type of condition: "equals", "startswith", "endswith", "contains"

    Returns:
        Modified DataFrame
    """
    try:
        if condition == "equals":
            filtered_df = df[df[column] != value]
        elif condition == "startswith":
            filtered_df = df[~df[column].str.startswith(value, na=False)]  #value is NaN (i.e., missing), treat it as False instead of returning NaN
        elif condition == "endswith":
            filtered_df = df[~df[column].str.endswith(value, na=False)]
        elif condition == "contains":
            filtered_df = df[~df[column].str.contains(value, na=False)]
        else:
            raise ValueError(f"Unsupported condition: {condition}")

        log_success(f"Deleted rows where {column} {condition} '{value}'")
        return filtered_df
    except Exception as e:
        log_error(f"Error deleting data: {e}")
        return df


def union_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Returns union (unique rows) of two dataframes.

    Args:
        df1: First DataFrame
        df2: Second DataFrame

    Returns:
        DataFrame containing unique rows from both
    """
    try:
        union_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
        log_success("Union of DataFrames completed.")
        return union_df
    except Exception as e:
        log_error(f"Error in union_dataframes: {e}")
        return df1


def aggregate_data(df: pd.DataFrame, group_by_col: str, agg_col: str, agg_func: str = 'sum') -> pd.DataFrame:
    """
    Aggregates data by grouping and applying an aggregation function.

    Args:
        df: DataFrame to aggregate
        group_by_col: Column to group by
        agg_col: Column to aggregate
        agg_func: Aggregation function (sum, mean, count, etc.)

    Returns:
        Aggregated DataFrame
    """
    try:
        aggregated = df.groupby(group_by_col)[agg_col].agg(agg_func).reset_index()
        log_success(f"Aggregated data by {group_by_col} using {agg_func} on {agg_col}")
        return aggregated
    except Exception as e:
        log_error(f"Error in aggregate_data: {e}")
        return df

