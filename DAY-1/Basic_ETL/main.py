import pandas as pd
from sqlalchemy import create_engine
from read import read_all_files, fetch_api_data, read_data_from_db
from transform import clean_data, process_data, filter_data, aggregate_data, append_data, delete_data, union_dataframes
from write import save_data_in_folders, insert_csv_data, generate_report, save_transformed_outputs_to_excel
from logger import log_error


DB_URL = 'sqlite:///user_data.sqlite'
engine = create_engine(DB_URL)


if __name__ == "__main__":

    insert_csv_data(engine)
    read_data_from_db("users", engine)

    # 2. Read local files and API data
    all_data = read_all_files()
    api_data = fetch_api_data()

    # 3. Clean and store API data
    cleaned_data = clean_data(api_data)
    save_data_in_folders(cleaned_data)

    # 4. Generate insights and reports
    insights = process_data(all_data)
    generate_report(all_data, insights)

    df1 = api_data.get("users")
    df2 = api_data.get("products")

    if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
        # 1. Filter users in 'Cluj-Napoca'
        filtered_df = filter_data(df1, "location", "Cluj-Napoca")

        # 2. Append products to users (example use-case)
        appended_df = append_data(df1, df2)

        # 3. Delete users with Yahoo emails
        deleted_df = delete_data(df1, "email", "@yahoo.com", condition="endswith")

        # 4. Union of both DataFrames
        union_df = union_dataframes(df1, df2)

        # 5. Count of emails per domain
        df1["domain"] = df1["email"].str.split("@").str[-1]
        aggregated_df = aggregate_data(df1, "domain", "email", "count")

        # Save all to Excel
        save_transformed_outputs_to_excel(
            filtered_df,
            appended_df,
            deleted_df,
            union_df,
            aggregated_df
        )
    else:
        log_error("Required API data not found or not in expected format.")
