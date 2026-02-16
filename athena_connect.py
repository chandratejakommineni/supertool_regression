import pandas as pd
import boto3
from urllib.parse import urlparse
import io
import time


def get_df_from_s3(s3_uri):
    s3_client = boto3.client("s3")
    loc = urlparse(s3_uri)
    bucket, key = loc.hostname, loc.path.lstrip("/")
    response = s3_client.get_object(
        Bucket=bucket, Key=key
    )
    body = response["Body"].read()
    df = pd.read_csv(io.BytesIO(body))

    return df

def athena_query_old(query: str, database: str, athena: boto3.client = None):
    if not athena:
        athena = boto3.client("athena")

    query_exec = athena.start_query_execution(
        QueryString=query.replace("\n", ""),         
        QueryExecutionContext={
            'Database': database
        },)
    athena_response = athena.get_query_execution(
        QueryExecutionId=query_exec["QueryExecutionId"]
    )
    while athena_response["QueryExecution"]["Status"]["State"] in ["RUNNING", "QUEUED"]:

        time.sleep(5)
        athena_response = athena.get_query_execution(
            QueryExecutionId=query_exec["QueryExecutionId"]
        )

    if athena_response["QueryExecution"]["Status"]["State"] == "SUCCEEDED":

        df = get_df_from_s3(
            athena_response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        )
        print('SUCCEEDED')
        return df
    else:
        error_statement = f"Stated Reason: {athena_response['QueryExecution']['Status']['StateChangeReason']}"
        if "Lake Formation" in error_statement:
            error_statement += "\n Check to make sure the table exists."
        print(f"Query failed: {error_statement}")

        return pd.DataFrame()
    
def athena_query(query: str, database: str, athena: boto3.client = None):
    if not athena:
        athena = boto3.client("athena")

    # Remove newlines once at the start instead of in the query execution
    query = query.replace("\n", " ")
    
    query_exec = athena.start_query_execution(
        QueryString=query,         
        QueryExecutionContext={
            'Database': database
        },)
    
    # Use exponential backoff for polling
    wait_time = 1  # Start with 1 second
    max_wait = 30  # Maximum wait of 30 seconds
    
    while True:
        athena_response = athena.get_query_execution(
            QueryExecutionId=query_exec["QueryExecutionId"]
        )
        state = athena_response["QueryExecution"]["Status"]["State"]
        
        if state not in ["RUNNING", "QUEUED"]:
            break
            
        time.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait)  # Exponential backoff with cap

    if athena_response["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        df = get_df_from_s3(
            athena_response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        )
        print('SUCCEEDED')
        return df
    else:
        error_statement = f"Stated Reason: {athena_response['QueryExecution']['Status']['StateChangeReason']}"
        if "Lake Formation" in error_statement:
            error_statement += "\n Check to make sure the table exists."
        print(f"Query failed: {error_statement}")

        return pd.DataFrame()