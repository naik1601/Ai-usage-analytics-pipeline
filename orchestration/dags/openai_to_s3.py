import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse
from uuid import uuid4

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.sdk import DAG, Connection, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


LOGGER = logging.getLogger(__name__)

SNOWFLAKE_CONNECTION = "snowflake_ai_usage"
BRONZE_SCHEMA = "AI_USAGE_ANALYTICS.BRONZE"
S3_STAGE = f"{BRONZE_SCHEMA}.AI_USAGE_S3_STAGE"
JSON_FORMAT = f"{BRONZE_SCHEMA}.AI_USAGE_JSON_FORMAT"
MANIFEST_TABLE = f"{BRONZE_SCHEMA}.INGESTION_MANIFESTS"

DATASETS = {
    "completions_usage": {
        "url": "https://api.openai.com/v1/organization/usage/completions",
        "group_by": ["model", "project_id", "api_key_id"],
        "table": f"{BRONZE_SCHEMA}.OPENAI_USAGE_RAW",
    },
    "costs": {
        "url": "https://api.openai.com/v1/organization/costs",
        "group_by": ["project_id", "line_item"],
        "table": f"{BRONZE_SCHEMA}.OPENAI_COSTS_RAW",
    },
}


def extract_to_s3(dataset, **context):
    """Extract one page at a time and upload directly to S3."""

    bucket = os.environ["S3_BUCKET"]
    settings = DATASETS[dataset]

    # Retrieve credentials only when the task executes.
    api_key = Connection.get("openai_admin").password
    if not api_key:
        raise ValueError("The openai_admin connection has no password.")

    # Anchor the default window to the run, not the retry time.
    anchor = context["data_interval_end"].in_timezone("UTC")
    default_end = anchor.start_of("day").date()

    params_input = context["params"]
    start_input = params_input.get("start_date")
    end_input = params_input.get("end_date")

    if bool(start_input) != bool(end_input):
        raise ValueError("Provide both start_date and end_date.")

    if start_input:
        start_date = date.fromisoformat(start_input)
        end_date = date.fromisoformat(end_input)
    else:
        end_date = default_end
        start_date = end_date - timedelta(days=1)

    today_utc = datetime.now(timezone.utc).date()

    if start_date >= end_date:
        raise ValueError("start_date must be before end_date.")

    if end_date > today_utc:
        raise ValueError(
            "end_date cannot be after today UTC. "
            "Only completed UTC days are supported."
        )

    start = pendulum.datetime(
        start_date.year, start_date.month, start_date.day, tz="UTC"
    )
    end = pendulum.datetime(
        end_date.year, end_date.month, end_date.day, tz="UTC"
    )

    query = {
        "start_time": int(start.timestamp()),
        "end_time": int(end.timestamp()),
        "bucket_width": "1d",
        "group_by": settings["group_by"],
        "limit": 31,
    }

    # Separate attempts prevent partial extractions from mixing.
    run_hash = hashlib.sha256(
        context["run_id"].encode("utf-8")
    ).hexdigest()[:16]

    attempt_id = uuid4().hex
    prefix = (
        f"raw/provider=openai/dataset={dataset}/"
        f"window_start={start_date}/window_end={end_date}/"
        f"run={run_hash}/attempt={attempt_id}"
    )

    # Use the mounted AWS profile through boto3's credential chain.
    s3 = S3Hook(aws_conn_id=None)

    retry_policy = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    page_number = 0
    record_count = 0
    seen_cursors = set()

    with requests.Session() as session:
        session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })
        session.mount(
            "https://",
            HTTPAdapter(max_retries=retry_policy),
        )

        while True:
            response = session.get(
                settings["url"],
                params=query,
                timeout=(10, 60),
            )

            if not response.ok:
                raise RuntimeError(
                    f"OpenAI {dataset} request failed: "
                    f"HTTP {response.status_code}. "
                    f"Request ID: {response.headers.get('x-request-id')}"
                )

            page = response.json()

            if (
                not isinstance(page, dict)
                or not isinstance(page.get("data"), list)
            ):
                raise ValueError("Unexpected OpenAI response structure.")

            page_records = 0

            for bucket_data in page["data"]:
                if not isinstance(bucket_data, dict):
                    raise ValueError("Unexpected usage bucket structure.")

                results = bucket_data.get("results")
                if not isinstance(results, list):
                    raise ValueError("Unexpected results structure.")

                page_records += len(results)

            page_number += 1
            record_count += page_records

            document = {
                "provider": "openai",
                "dataset": dataset,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "airflow_run_id": context["run_id"],
                "attempt_id": attempt_id,
                "page_number": page_number,
                "response": page,
            }

            object_key = f"{prefix}/pages/page_{page_number:05d}.json"

            # Serialize in memory—no local raw-data file.
            s3.load_string(
                string_data=json.dumps(document),
                key=object_key,
                bucket_name=bucket,
                replace=False,
                encrypt=True,
            )

            LOGGER.info(
                "Uploaded page %s with %s records to s3://%s/%s",
                page_number,
                page_records,
                bucket,
                object_key,
            )

            if not page.get("has_more"):
                break

            cursor = page.get("next_page")
            if not cursor or cursor in seen_cursors:
                raise ValueError("Missing or repeated pagination cursor.")

            seen_cursors.add(cursor)
            query["page"] = cursor

    # Publish only after every page has uploaded successfully.
    manifest_key = f"{prefix}/manifest.json"

    manifest = {
        "status": "complete",
        "provider": "openai",
        "dataset": dataset,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "airflow_run_id": context["run_id"],
        "attempt_id": attempt_id,
        "pages_prefix": f"{prefix}/pages/",
        "page_count": page_number,
        "record_count": record_count,
    }

    s3.load_string(
        string_data=json.dumps(manifest),
        key=manifest_key,
        bucket_name=bucket,
        replace=False,
        encrypt=True,
    )

    LOGGER.info(
        "Completed %s: %s pages, %s records",
        dataset,
        page_number,
        record_count,
    )

    # Only a small reference enters XCom.
    return {
        "manifest_uri": f"s3://{bucket}/{manifest_key}",
        "record_count": record_count,
    }


def load_to_snowflake(dataset, extract_task_id, **context):
    """Load a completed extraction into Bronze and verify its counts."""

    bucket = os.environ["S3_BUCKET"]
    table = DATASETS[dataset]["table"]

    extraction = context["ti"].xcom_pull(
        task_ids=extract_task_id,
        key="return_value",
    )

    if not isinstance(extraction, dict):
        raise ValueError("Missing extraction result in XCom.")

    manifest_uri = extraction.get("manifest_uri")
    if not isinstance(manifest_uri, str):
        raise ValueError("Missing manifest URI.")

    # Accept only this bucket and the expected extraction path.
    uri = urlparse(manifest_uri)

    if (
        uri.scheme != "s3"
        or uri.netloc != bucket
        or uri.query
        or uri.fragment
    ):
        raise ValueError("Unexpected manifest location.")

    manifest_key = uri.path.lstrip("/")

    run_hash = hashlib.sha256(
        context["run_id"].encode("utf-8")
    ).hexdigest()[:16]

    path_pattern = (
        rf"raw/provider=openai/dataset={re.escape(dataset)}/"
        rf"window_start=(\d{{4}}-\d{{2}}-\d{{2}})/"
        rf"window_end=(\d{{4}}-\d{{2}}-\d{{2}})/"
        rf"run={run_hash}/attempt=([0-9a-f]{{32}})/manifest\.json"
    )

    match = re.fullmatch(path_pattern, manifest_key)
    if not match:
        raise ValueError("Manifest path does not match this dataset/run.")

    path_start, path_end, attempt_id = match.groups()
    prefix = manifest_key.removesuffix("/manifest.json")
    pages_prefix = f"{prefix}/pages/"

    # Read only the small manifest into Airflow.
    s3 = S3Hook(aws_conn_id=None)
    manifest = json.loads(
        s3.read_key(key=manifest_key, bucket_name=bucket)
    )

    if not isinstance(manifest, dict):
        raise ValueError("Manifest must be a JSON object.")

    expected_fields = {
        "status": "complete",
        "provider": "openai",
        "dataset": dataset,
        "airflow_run_id": context["run_id"],
        "attempt_id": attempt_id,
        "pages_prefix": pages_prefix,
    }

    for field, expected in expected_fields.items():
        if manifest.get(field) != expected:
            raise ValueError(f"Unexpected manifest field: {field}")

    window_start = pendulum.parse(manifest["window_start"])
    window_end = pendulum.parse(manifest["window_end"])

    if (
        window_start.to_date_string() != path_start
        or window_end.to_date_string() != path_end
        or window_start >= window_end
    ):
        raise ValueError("Manifest window does not match its path.")

    page_count = manifest.get("page_count")
    record_count = manifest.get("record_count")

    if type(page_count) is not int or page_count < 1:
        raise ValueError("Invalid manifest page_count.")

    if type(record_count) is not int or record_count < 0:
        raise ValueError("Invalid manifest record_count.")

    # The stage URL already ends in /raw/, so remove that prefix.
    stage_pages_path = pages_prefix.removeprefix("raw/")

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONNECTION
    )
    connection = hook.get_conn()

    try:
        connection.autocommit(False)

        with connection.cursor() as cursor:
            cursor.execute("BEGIN")

            # Durable retry guard: do not reload a committed attempt.
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {MANIFEST_TABLE}
                WHERE SOURCE_FILE = %s
                """,
                (manifest_key,),
            )

            if cursor.fetchone()[0] > 0:
                connection.commit()
                LOGGER.info(
                    "Already loaded %s attempt %s; skipping.",
                    dataset,
                    attempt_id,
                )
                return {
                    "dataset": dataset,
                    "status": "already_loaded",
                    "record_count": record_count,
                }

            # Only validated paths and allowlisted table names are
            # interpolated into SQL. API keys never enter SQL.
            cursor.execute(
                f"""
                COPY INTO {table}
                    (PAYLOAD, SOURCE_FILE, LOADED_AT)
                FROM (
                    SELECT
                        $1,
                        METADATA$FILENAME,
                        CURRENT_TIMESTAMP()
                    FROM @{S3_STAGE}/{stage_pages_path}
                )
                FILE_FORMAT = (
                    FORMAT_NAME = '{JSON_FORMAT}'
                )
                PATTERN = '.*page_[0-9]+[.]json'
                ON_ERROR = 'ABORT_STATEMENT'
                FORCE = FALSE
                """
            )
            cursor.fetchall()

            # One Bronze row represents one complete API page.
            cursor.execute(
                f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT PAYLOAD:page_number::INTEGER),
                    MIN(PAYLOAD:page_number::INTEGER),
                    MAX(PAYLOAD:page_number::INTEGER)
                FROM {table}
                WHERE PAYLOAD:attempt_id::STRING = %s
                  AND PAYLOAD:dataset::STRING = %s
                  AND PAYLOAD:airflow_run_id::STRING = %s
                """,
                (attempt_id, dataset, context["run_id"]),
            )

            rows, distinct_pages, first_page, last_page = (
                cursor.fetchone()
            )

            if (
                rows != page_count
                or distinct_pages != page_count
                or first_page != 1
                or last_page != page_count
            ):
                raise ValueError(
                    f"Page validation failed: expected {page_count}, "
                    f"found {rows} rows and "
                    f"{distinct_pages} distinct pages."
                )

            # Count the actual API result records inside the pages.
            cursor.execute(
                f"""
                SELECT COALESCE(
                    SUM(ARRAY_SIZE(bucket.value:results)),
                    0
                )
                FROM {table} AS raw,
                    LATERAL FLATTEN(
                        INPUT => raw.PAYLOAD:response:data
                    ) AS bucket
                WHERE raw.PAYLOAD:attempt_id::STRING = %s
                  AND raw.PAYLOAD:dataset::STRING = %s
                  AND raw.PAYLOAD:airflow_run_id::STRING = %s
                """,
                (attempt_id, dataset, context["run_id"]),
            )

            loaded_records = int(cursor.fetchone()[0])

            if loaded_records != record_count:
                raise ValueError(
                    f"Record validation failed: expected "
                    f"{record_count}, found {loaded_records}."
                )

            # Commit marker and raw pages belong to one transaction.
            cursor.execute(
                f"""
                INSERT INTO {MANIFEST_TABLE}
                    (PAYLOAD, SOURCE_FILE, LOADED_AT)
                SELECT
                    PARSE_JSON(%s),
                    %s,
                    CURRENT_TIMESTAMP()
                """,
                (json.dumps(manifest), manifest_key),
            )

            connection.commit()

        LOGGER.info(
            "Loaded %s: %s pages, %s records into %s",
            dataset,
            page_count,
            record_count,
            table,
        )

        return {
            "dataset": dataset,
            "status": "loaded",
            "page_count": page_count,
            "record_count": record_count,
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


with DAG(
    dag_id="openai_usage_costs_to_s3",
    description="Extract OpenAI data to S3 and load Snowflake Bronze",
    start_date=pendulum.datetime(
        2026, 11, 17, tz="America/Chicago"
    ),
    schedule=CronTriggerTimetable(
        "0 8 * * *",
        timezone="America/Chicago",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            description="UTC start date, inclusive: YYYY-MM-DD",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            description="UTC end date, exclusive: YYYY-MM-DD",
        ),
    },
    tags=["openai", "s3", "snowflake", "bronze"],
) as dag:

    extract_usage = PythonOperator(
        task_id="extract_usage_to_s3",
        python_callable=extract_to_s3,
        op_kwargs={"dataset": "completions_usage"},
        show_return_value_in_logs=False,
    )

    extract_costs = PythonOperator(
        task_id="extract_costs_to_s3",
        python_callable=extract_to_s3,
        op_kwargs={"dataset": "costs"},
        show_return_value_in_logs=False,
    )

    load_usage = PythonOperator(
        task_id="load_usage_to_snowflake",
        python_callable=load_to_snowflake,
        op_kwargs={
            "dataset": "completions_usage",
            "extract_task_id": "extract_usage_to_s3",
        },
        show_return_value_in_logs=False,
    )

    load_costs = PythonOperator(
        task_id="load_costs_to_snowflake",
        python_callable=load_to_snowflake,
        op_kwargs={
            "dataset": "costs",
            "extract_task_id": "extract_costs_to_s3",
        },
        show_return_value_in_logs=False,
    )

    extract_usage >> load_usage
    extract_costs >> load_costs