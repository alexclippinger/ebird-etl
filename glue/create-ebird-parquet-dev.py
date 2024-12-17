import sys
import boto3

client = boto3.client("athena")

SOURCE_TABLE_NAME = "raw_ebird_ingest"
TARGET_TABLE_NAME = "dev_ebird_ingest"
TARGET_S3_BUCKET = "s3://ebird-etl/ebird-parquet-dev"
DATABASE_NAME = "ebird-db"
QUERY_RESULTS_S3_BUCKET = "s3://ebird-etl/ebird-athena-query-results"

# Refresh the table
queryStart = client.start_query_execution(
    QueryString=f"""
    CREATE TABLE {TARGET_TABLE_NAME} WITH
        (external_location='{TARGET_S3_BUCKET}',
        format='PARQUET',
        write_compression='SNAPPY',
        partitioned_by=ARRAY['year_month'])
    AS

    SELECT
        speciescode,
        comname,
        sciname,
        locid,
        locname,
        obsdt,
        howmany,
        lat,
        lng,
        obsvalid,
        obsreviewed,
        locationprivate,
        subid,
        SUBSTRING(obsdt, 1, 7) AS year_month
    FROM "{DATABASE_NAME}"."{SOURCE_TABLE_NAME}"
    ;
    """,
    QueryExecutionContext={"Database": f"{DATABASE_NAME}"},
    ResultConfiguration={"OutputLocation": f"{QUERY_RESULTS_S3_BUCKET}"},
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
