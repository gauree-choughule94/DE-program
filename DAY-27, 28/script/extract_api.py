import requests
import pandas as pd
import sys

def fetch_api_data(last_updated, output_path):
    url = "https://jsonplaceholder.typicode.com/users"
    resp = requests.get(url)
    users = resp.json()

    df = pd.json_normalize(users)
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    fetch_api_data(sys.argv[1], sys.argv[2])



