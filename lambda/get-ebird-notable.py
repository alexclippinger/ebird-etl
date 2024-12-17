import os
import json
import boto3
import urllib3
from urllib.parse import urlencode
from datetime import datetime

EBIRD_RESULTS_S3_BUCKET_NAME = "ebird-etl"


def lambda_handler(event=None, context=None):
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
    data = json.loads(results.data.decode(encoding="utf-8", errors="strict"))
    json_lines = "\n".join(json.dumps(record) for record in data)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=EBIRD_RESULTS_S3_BUCKET_NAME,
        Key=f"ebird-ingest/ebird-notable-obs-{timestamp}.json",
        Body=json_lines,
        ContentType="application/json",
    )
