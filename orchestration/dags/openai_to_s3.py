import hashlib
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.sdk import DAG, Connection, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


LOGGER = logging.getLogger(__name__)

DATASETS = {
    "completions_usage": {
        "url": "https://api.openai.com/v1/organization/usage/completions",
        "group_by": ["model", "project_id", "api_key_id"],
    },
    "costs": {
        "url": "https://api.openai.com/v1/organization/costs",
        "group_by": ["project_id", "line_item"],
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

    # A new prefix for each task attempt prevents partial attempts
    # from mixing with successful extractions.
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
                # Avoid logging headers or credentials.
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

            # Serialize in memory and upload—no local data file.
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


with DAG(
    dag_id="openai_usage_costs_to_s3",
    description="Extract OpenAI usage and costs directly into S3",
    start_date=pendulum.datetime(2026, 11, 17, tz="America/Chicago"),
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
    tags=["openai", "s3", "ingestion"],
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