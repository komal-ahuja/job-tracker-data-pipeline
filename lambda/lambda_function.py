import http.client
from urllib.parse import urlencode
import json
import boto3
import time
import os
from datetime import datetime, timezone

API_KEY = os.environ["API_KEY"]
queries = os.environ["QUERIES"].split("|")
BUCKET_NAME = os.environ["BUCKET_NAME"]

if not API_KEY:
    raise ValueError("Missing API_KEY environment variable")

if not BUCKET_NAME:
    raise ValueError("Missing BUCKET_NAME environment variable")

if not queries or queries == [""]:
    raise ValueError("Missing QUERIES environment variable")

s3 = boto3.client("s3")

MAX_RETRIES = 3
RETRY_DELAY = 10
API_TIMEOUT = 60


def clean_surrogates(value):
    """
    Replace invalid/unpaired Unicode surrogates with
    the Unicode replacement character.
    """
    if isinstance(value, str):
        return value.encode(
            "utf-16",
            "surrogatepass"
        ).decode(
            "utf-16",
            "replace"
        )

    if isinstance(value, dict):
        return {
            key: clean_surrogates(val)
            for key, val in value.items()
        }

    if isinstance(value, list):
        return [
            clean_surrogates(item)
            for item in value
        ]

    return value


def lambda_handler(event, context):

    uploaded_files = []

    for query in queries:

        params = urlencode({
            "query": query,
            "page": 1,
            "num_pages": 1,
            "date_posted": "month",
            "country": "us",
            "language": "en"
        })

        headers = {
            "Accept": "application/json",
            "x-api-key": API_KEY
        }

        # -------------------------
        # Call API with retries
        # -------------------------

        for attempt in range(1, MAX_RETRIES + 1):

            conn = http.client.HTTPSConnection(
                "api.openwebninja.com",
                timeout=API_TIMEOUT
            )

            try:
                conn.request(
                    "GET",
                    f"/realtime-jobs-data/google-jobs/search?{params}",
                    headers=headers
                )

                res = conn.getresponse()
                raw_response = res.read().decode("utf-8")

            finally:
                conn.close()

            if res.status == 200:
                break

            print(
                f"API returned {res.status} "
                f"on attempt {attempt}/{MAX_RETRIES}"
            )

            if attempt == MAX_RETRIES:
                raise Exception(
                    f"API returned {res.status}: {raw_response}"
                )

            time.sleep(RETRY_DELAY)

        # -------------------------
        # Parse API response
        # -------------------------

        source_ingest_timestamp = (
            datetime.now(timezone.utc).isoformat()
        )

        jobs = json.loads(raw_response)

        # -------------------------
        # Clean invalid Unicode
        # -------------------------

        jobs = clean_surrogates(jobs)

        # -------------------------
        # Add metadata
        # -------------------------

        jobs["metadata"] = {
            "source_ingest_timestamp": source_ingest_timestamp,
            "source": "OpenWebNinja",
            "api_name": "google-jobs"
        }

        # -------------------------
        # Serialize as valid UTF-8
        # -------------------------

        raw_response = json.dumps(
            jobs,
            ensure_ascii=False
        )

        # -------------------------
        # Create S3 key
        # -------------------------

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )

        query_name = query.replace(" ", "_").lower()

        s3_key = (
            f"raw/jobs/"
            f"ingest_date={today}/"
            f"{query_name}_{timestamp}.json"
        )

        # -------------------------
        # Upload to S3
        # -------------------------

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=raw_response.encode("utf-8"),
            ContentType="application/json"
        )

        uploaded_files.append(s3_key)

        print(f"Uploaded {s3_key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Ingestion complete",
            "files_uploaded": len(uploaded_files),
            "objects": uploaded_files
        })
    }