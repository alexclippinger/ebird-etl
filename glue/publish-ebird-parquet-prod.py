import sys
import boto3
from datetime import datetime

QUERY_RESULTS_BUCKET = "s3://ebird-etl/ebird-athena-query-results"
DATABASE_NAME = "ebird-db"
SOURCE_TABLE_NAME = "dev_ebird_ingest"
TARGET_TABLE_NAME = "prod_ebird_ingest"
TARGET_S3_BUCKET = "s3://ebird-etl/ebird-parquet-prod"


# this will be used in the table name and in the bucket path in S3 where the table is stored
TIMESTAMP = (
    str(datetime.now())
    .replace("-", "_")
    .replace(" ", "_")
    .replace(":", "_")
    .replace(".", "_")
)

client = boto3.client("athena")

# Refresh the table
queryStart = client.start_query_execution(
    QueryString=f"""
    CREATE TABLE {TARGET_TABLE_NAME} WITH
        (external_location='{TARGET_S3_BUCKET}/{TIMESTAMP}/',
        format='PARQUET',
        write_compression='SNAPPY',
        partitioned_by = ARRAY['year_month'])
    AS

    SELECT *
    FROM "{DATABASE_NAME}"."{SOURCE_TABLE_NAME}"

    ;
    """,
    QueryExecutionContext={"Database": f"{DATABASE_NAME}"},
    ResultConfiguration={"OutputLocation": f"{QUERY_RESULTS_BUCKET}"},
)

# list of responses
resp = ["FAILED", "SUCCEEDED", "CANCELLED"]

# get the response
response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

# wait until query finishes
while response["QueryExecution"]["Status"]["State"] not in resp:
    response = client.get_query_execution(
        QueryExecutionId=queryStart["QueryExecutionId"]
    )

# if it fails, exit and give the Athena error message in the logs
if response["QueryExecution"]["Status"]["State"] == "FAILED":
    sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])
