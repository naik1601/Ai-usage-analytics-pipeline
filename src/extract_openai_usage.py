import argparse
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_URL = "https://api.openai.com/v1/organization/usage/completions"


def extract_usage(usage_date: date, monthly: bool = False) -> Path:
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    api_key = os.getenv("OPENAI_ADMIN_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_ADMIN_KEY is missing from .env")

    # Query one complete UTC day: start inclusive, end exclusive.
    if monthly:
      usage_date = usage_date.replace(day=1)

    start = datetime.combine(usage_date, time.min, tzinfo=timezone.utc)

    if monthly:
        next_month = (usage_date.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = datetime.combine(next_month, time.min, tzinfo=timezone.utc)

         # For the current month, include only completed UTC days.
        today_utc = datetime.combine(
            datetime.now(timezone.utc).date(),
            time.min,
            tzinfo=timezone.utc,
        )
        end = min(end, today_utc)
    else:
        end = start + timedelta(days=1)

    if end <= start:
        raise RuntimeError("The selected period has no completed UTC days.")
    

    params = {
        "start_time": int(start.timestamp()),
        "end_time": int(end.timestamp()),
        "bucket_width": "1d",
        "group_by": ["project_id", "line_item"],
        "limit": 31,
    }

    retries = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    pages = []
    seen_cursors = set()

    with requests.Session() as session:
        session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })
        session.mount("https://", HTTPAdapter(max_retries=retries))

        while True:
            response = session.get(
                API_URL,
                params=params,
                timeout=(10, 60),
            )

            if not response.ok:
                code = None

                try:
                    body = response.json()

                    if isinstance(body, dict):
                     error = body.get("error", body)

                     if isinstance(error, dict):
                        message = error.get("message", str(error))
                        code = error.get("code")
                     else:
                        message = str(error)
                    else:
                     message = str(body)

                except ValueError:
                  message = response.text[:1000]

                raise RuntimeError(
                    f"OpenAI HTTP {response.status_code}: "
                    f"{message} (code={code})"
                )

            page = response.json()

            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise RuntimeError("Unexpected OpenAI response structure.")

            pages.append(page)

            if not page.get("has_more"):
                break

            cursor = page.get("next_page")
            if not cursor or cursor in seen_cursors:
                raise RuntimeError("Missing or repeated pagination cursor.")

            seen_cursors.add(cursor)
            params["page"] = cursor

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid4().hex[:8]
    )

    output_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "provider=openai"
        / "dataset=completions_usage"
        / f"usage_date={usage_date.isoformat()}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "response.json"

    document = {
        "provider": "openai",
        "dataset": "completions_usage",
        "usage_date": usage_date.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "page_count": len(pages),
        "pages": pages,
    }

    output_file.write_text(
        json.dumps(document, indent=2),
        encoding="utf-8",
    )

    rows = [
        result
        for page in pages
        for bucket in page["data"]
        for result in bucket.get("results", [])
    ]

    print(f"Usage date (UTC): {usage_date}")
    print(f"Pages retrieved: {len(pages)}")
    print(f"Usage records: {len(rows)}")
    print(
        "Input tokens:",
        sum(row.get("input_tokens") or 0 for row in rows),
    )
    print(
        "Output tokens:",
        sum(row.get("output_tokens") or 0 for row in rows),
    )
    print(
        "Model requests:",
        sum(row.get("num_model_requests") or 0 for row in rows),
    )
    print(f"Saved to: {output_file}")

    if not rows:
        print("Extraction succeeded, but no usage records were returned.")

    return output_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date() - timedelta(days=1),
        help="UTC usage date in YYYY-MM-DD format; defaults to yesterday.",
    )
    parser.add_argument(
    "--monthly",
    action="store_true",
    help="Extract the calendar month containing --date.",
    )
    args = parser.parse_args()

    try:
        extract_usage(args.date, monthly=args.monthly)
    except (RuntimeError, requests.RequestException, ValueError) as exc:
        raise SystemExit(f"Extraction failed: {exc}")