import pandas as pd
import os
from prefect import flow, task
from sodapy import Socrata
# from config import app_token, username, password
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path

app_token = os.environ.get("LP_APP_TOKEN")
username = os.environ.get("LP_USERNAME")
password = os.environ.get("LP_PASSWORD")

@task(name="authentication")
def auth(app_token, username, password):
    # authenticated client (needed for non-public datasets):
    client = Socrata("data.lacity.org",
                 app_token,
                 username=username,
                 password=password)
    
    return client

@task(name="extract_data", retries=3)
def extract_data(client, limit):
    # results returned as JSON from API / converted to Python list of dictionaries by sodapy.
    results = client.get("wjz9-h9np", limit=limit)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    return results_df

@task(log_prints=True)
def clean_data(df = pd.DataFrame) -> pd.DataFrame:
    #insert code here
    print(f"columns: {df.dtypes}")
    return df

@task()
def write_local(df: pd.DataFrame)->Path:
    path=Path("flows/gcp/lucky_parking.parquet")
    df.to_parquet(path,compression="gzip")
    return path

@task()
def write_gcs(path:Path) -> None:
    """Upload local parquet file """
    gcs_block = GcsBucket.load("lucky-parking-data")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return 

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    limit = 30
    client = auth(app_token, username, password)
    df = extract_data(client, limit)
    df = clean_data(df)
    path = write_local(df)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()


