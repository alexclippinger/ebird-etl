import os
import json
import time
import boto3
import urllib3
import logging
import awswrangler as wr
from urllib.parse import urlencode
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
athena = boto3.client("athena")

EBIRD_RESULTS_S3_BUCKET_NAME = "ebird-etl"
DATABASE_NAME = "ebird-db"

RAW_TABLE_NAME = "raw_ebird_ingest"
DEV_TABLE_NAME = "dev_ebird_ingest"
PROD_TABLE_NAME = "prod_ebird_ingest"

QUERY_RESULTS_S3_BUCKET = "s3://ebird-etl/ebird-athena-query-results"
DEV_S3_BUCKET = "s3://ebird-etl/ebird-parquet-dev"
PROD_S3_BUCKET = "s3://ebird-etl/ebird-parquet-prod"

RESPONSE_STATUSES = ["FAILED", "SUCCEEDED", "CANCELLED"]
TIMESTAMP = datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")


def get_ebird_notable():
    """
    https://documenter.getpostman.com/view/664302/S1ENwy59#3d2a17c1-2129-475c-b4c8-7d362d6000cd
    Endpoint description:
        Get the list of recent observations (up to 30 days ago) of birds seen in a country, state, county, or location.
        Results include only the most recent observation for each species in the region specified.
    """
    api_key = os.environ["EBIRD_API_KEY"]
    http = urllib3.PoolManager()
    base_url = "https://api.ebird.org/v2/data/obs/{region_code}/recent/notable".format(
        region_code="US-WA-073"  # Whatcom County, WA
    )
    headers = {"accept": "application/json", "X-eBirdApiToken": api_key}
    params = {
        "maxResults": 1000,
        "back": 7,
    }
    url = f"{base_url}?{urlencode(params)}"
    results = http.request("GET", url, headers=headers)

    if results.status != 200:
        raise Exception(f"eBird API returned status code {results.status}")

    data = json.loads(results.data.decode(encoding="utf-8", errors="strict"))
    json_lines = "\n".join(json.dumps(record) for record in data)

    s3.put_object(
        Bucket=EBIRD_RESULTS_S3_BUCKET_NAME,
        Key=f"ebird-ingest/ebird-notable-obs-{TIMESTAMP}.json",
        Body=json_lines,
        ContentType="application/json",
    )


def wait_for_athena_query(query_execution_id, interval=2, timeout=300):
    start_time = time.time()
    response = athena.get_query_execution(QueryExecutionId=query_execution_id)

    # Try until the query finishes or times out
    while response["QueryExecution"]["Status"]["State"] not in RESPONSE_STATUSES:
        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Athena query {query_execution_id} timed out after {timeout} seconds."
            )
        time.sleep(interval)
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)

    # If the query failed, raise an exception with the error message
    if response["QueryExecution"]["Status"]["State"] == "FAILED":
        raise Exception(response["QueryExecution"]["Status"]["StateChangeReason"])

    return response


def create_parquet_dev():
    # Refresh the table
    queryStart = athena.start_query_execution(
        QueryString=f"""
        CREATE TABLE {DEV_TABLE_NAME} WITH (
            external_location='{DEV_S3_BUCKET}',
            format='PARQUET',
            write_compression='SNAPPY',
            partitioned_by=ARRAY['year_month']
        )
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
        FROM "{DATABASE_NAME}"."{RAW_TABLE_NAME}";
        """,
        QueryExecutionContext={"Database": f"{DATABASE_NAME}"},
        ResultConfiguration={"OutputLocation": f"{QUERY_RESULTS_S3_BUCKET}"},
    )
    wait_for_athena_query(queryStart["QueryExecutionId"])


def dq_checks():
    checks = {
        "NULL_HOWMANY": f"""
            SELECT SUM(CASE WHEN howmany IS NULL THEN 1 ELSE 0 END) AS flag_count
            FROM "{DATABASE_NAME}"."{DEV_TABLE_NAME}";
        """,
        "NULL_LAT_LNG": f"""
            SELECT SUM(CASE WHEN lat IS NULL OR lng IS NULL THEN 1 ELSE 0 END) AS flag_count
            FROM "{DATABASE_NAME}"."{DEV_TABLE_NAME}";
        """,
        "INVALID_LAT_LNG": f"""
            SELECT SUM(CASE WHEN lat < -90 OR lat > 90 OR lng < -180 OR lng > 180 THEN 1 ELSE 0 END) AS flag_count
            FROM "{DATABASE_NAME}"."{DEV_TABLE_NAME}";
        """,
        "NULL_SPECIES_CODE": f"""
            SELECT SUM(CASE WHEN speciescode IS NULL OR TRIM(speciescode) = '' THEN 1 ELSE 0 END) AS flag_count
            FROM "{DATABASE_NAME}"."{DEV_TABLE_NAME}";
        """,
    }

    # Run each check and exit if any fail
    for check_name, query in checks.items():
        df = wr.athena.read_sql_query(sql=query, database="ebird-db")
        if df["flag_count"][0] > 0:
            raise Exception(
                f"Data Quality check failed for {check_name}. Found {df['flag_count'][0]} invalid rows."
            )
    logger.info("All data quality checks passed successfully!")


def publish_parquet_prod():
    queryStart = athena.start_query_execution(
        QueryString=f"""
        CREATE TABLE {PROD_TABLE_NAME} WITH
            (external_location='{PROD_S3_BUCKET}/{TIMESTAMP}/',
            format='PARQUET',
            write_compression='SNAPPY',
            partitioned_by = ARRAY['year_month'])
        AS
        SELECT * FROM "{DATABASE_NAME}"."{DEV_TABLE_NAME}";
        """,
        QueryExecutionContext={"Database": f"{DATABASE_NAME}"},
        ResultConfiguration={"OutputLocation": f"{QUERY_RESULTS_S3_BUCKET}"},
    )
    wait_for_athena_query(queryStart["QueryExecutionId"])


def lambda_handler(event=None, context=None):
    try:
        get_ebird_notable()
        create_parquet_dev()
        dq_checks()
        publish_parquet_prod()
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
