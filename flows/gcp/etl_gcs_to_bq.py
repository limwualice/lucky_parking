import pandas as pd
import os
from prefect import flow, task
from sodapy import Socrata
# from config import app_token, username, password
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
from prefect_gcp import GcpCredentials
from google.cloud import bigquery





@task(retries=3)
def extract_from_gcs():
    """Download parking data from GCS"""
    gcs_path="flows/gcp/lucky_parking.parquet"
    gcs_block=GcsBucket.load("lucky-parking-data")
    gcs_block.get_directory(from_path=gcs_path)
    return Path(gcs_path)


@task()
def transform(path):
    """Data cleaning"""
    df=pd.read_parquet(path)
    print(f"columns: {df.dtypes}")
    print(f"missing data: {df['fine_amount'].isna().sum()}")
    df['fine_amount']=df['fine_amount'].fillna(0)
    print(f"missing data: {df['fine_amount'].isna().sum()}")
    print(df.head(1))
    df = df.astype(str)
    return df


@task()
# def update_schema(schema):
#     updated_schema = []
#     for field in schema:
#         name = field.name
#         updated_field = bigquery.SchemaField(name.lower().replace(" ", "_"), field.field_type)
#         updated_schema.append(updated_field)
#     return updated_schema

def write_bq(df):
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("luckyparking-gcpcredentials-block")
    # original_schema = [
    #     {'name':'ticket_number', 'type':'STRING'},
    #     {'name':'issue_date', 'type':'DATETIME'},
    #     {'name':'issue_time', 'type':'INTEGER'},
    #     {'name':'meter_id', 'type': 'STRING'},
    #     {'name':'marked_time', 'type':'STRING'},
    #     {'name':'rp_state_plate', 'type':'STRING'},
    #     {'name':'plate_expiry_date','type': 'STRING'},
    #     {'name':'vin', 'type':'STRING'},
    #     {'name':'make', 'type':'STRING'},
    #     {'name':'body_style','type': 'STRING'},
    #     {'name':'color', 'type':'STRING'},
    #     {'name':'location', 'type':'STRING'},
    #     {'name':'route', 'type':'STRING'},
    #     {'name':'agency', 'type':'INTEGER'},
    #     {'name':'violation_code', 'type':'STRING'},
    #     {'name':'violation_description','type': 'STRING'},
    #     {'name':'fine_amount', 'type':'FLOAT'},
    #     {'name':'latitude', 'type':'FLOAT'},
    #     {'name':'longitude','type': 'FLOAT'},
    #     {'name':'agency_description','type': 'STRING'},
    #     {'name':'color_description', 'type':'STRING'},
    #     {'name':'body_style_description','type': 'STRING'}
    # ]
    # # updated_schema = update_schema(original_schema)
    
    try:
        print("Writing DataFrame to BigQuery...")

        df.to_gbq(
            destination_table= "luckyparking_dataset.parking",
            project_id="celtic-surface-388300",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            # chunksize=10,
            if_exists="append",
            # table_schema=original_schema

        )
        print("DataFrame successfully written to BigQuery.")
    except Exception as e:
        print("An error occurred while writing DataFrame to BigQuery:")
        print(str(e))
        raise


@flow()
def etl_gcs_to_bq():
    """Main ETAL flow to load data into Big Query"""
    
    path=extract_from_gcs()
    df=transform(path)
    write_bq(df)


if __name__=="__main__":
    etl_gcs_to_bq()