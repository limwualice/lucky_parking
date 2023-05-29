import pandas as pd
import prefect
from prefect import Flow, task
from sodapy import Socrata
from config import app_token, username, password


@task(name="authentication")
def auth():
    # authenticated client (needed for non-public datasets):
    client = Socrata("data.lacity.org",
                 app_token,
                 username=username,
                 password=password)
    
    return client

@task(name="extract_data")
def extract_data(client, limit):
    # results returned as JSON from API / converted to Python list of dictionaries by sodapy.
    results = client.get("wjz9-h9np", limit=limit)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    return results_df


