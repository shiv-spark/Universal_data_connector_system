import requests
import polars as pl

def api_connector(url):

    print("Fetching API data...")

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("API request failed")

    data = response.json()

    df = pl.DataFrame(data)

    return df