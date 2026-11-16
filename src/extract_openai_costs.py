import argparse
import json
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_URL = "https://api.openai.com/v1/organization/costs"


def extract_costs(usage_date: date, monthly: bool = False) -> Path:
    # 1. Load credentials from the project's .env file.
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    api_key = os.getenv("OPENAI_ADMIN_KEY", "").strip()

    if not api_key:
        raise RuntimeError("OPENAI_ADMIN_KEY is missing from .env")

    # 2. Determine the requested UTC date window.
    if monthly:
        usage_date = usage_date.replace(day=1)

    start = datetime.combine(
        usage_date,
        time.min,
        tzinfo=timezone.utc,
    )

    if monthly:
        next_month = (
            usage_date.replace(day=28) + timedelta(days=4)
        ).replace(day=1)

        end = datetime.combine(
            next_month,
            time.min,
            tzinfo=timezone.utc,
        )
    else:
        end = start + timedelta(days=1)

    # Include only complete UTC days.
    today_utc = datetime.combine(
        datetime.now(timezone.utc).date(),
        time.min,
        tzinfo=timezone.utc,
    )

    end = min(end, today_utc)

    if end <= start:
        raise RuntimeError(
            "The selected period has no completed UTC days. "
            "Choose a date before today UTC."
        )

    # 3. Set the API query parameters.
    params = {
        "start_time": int(start.timestamp()),
        "end_time": int(end.timestamp()),
        "bucket_width": "1d",
        "group_by": ["project_id", "line_item"],
        "limit": 31,
    }

    # 4. Configure retries for temporary failures.
    retries = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    pages = []
    seen_cursors = set()

    # 5. Request all response pages.
    with requests.Session() as session:
        session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            }
        )

        session.mount(
            "https://",
            HTTPAdapter(max_retries=retries),
        )

        while True:
            response = session.get(
                API_URL,
                params=params,
                timeout=(10, 60),
            )

            # Handle dictionary, string, and non-JSON errors.
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

            if (
                not isinstance(page, dict)
                or not isinstance(page.get("data"), list)
            ):
                raise RuntimeError(
                    "Unexpected OpenAI response structure."
                )

            pages.append(page)

            if not page.get("has_more"):
                break

            cursor = page.get("next_page")

            if not cursor or cursor in seen_cursors:
                raise RuntimeError(
                    "Missing or repeated pagination cursor."
                )

            seen_cursors.add(cursor)
            params["page"] = cursor

    # 6. Gather cost records for the terminal summary.
    rows = [
        result
        for page in pages
        for bucket in page["data"]
        for result in bucket.get("results", [])
    ]

    # 7. Generate a unique identifier for this extraction.
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid4().hex[:8]
    )

    # Separate daily and monthly extraction folders.
    period_folder = (
        f"usage_month={usage_date:%Y-%m}"
        if monthly
        else f"usage_date={usage_date.isoformat()}"
    )

    output_dir = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "provider=openai"
        / "dataset=costs"
        / period_folder
        / f"run_id={run_id}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "response.json"

    # 8. Preserve the original responses with extraction metadata.
    document = {
        "provider": "openai",
        "dataset": "costs",
        "extraction_mode": "monthly" if monthly else "daily",
        "usage_date": usage_date.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "page_count": len(pages),
        "record_count": len(rows),
        "pages": pages,
    }

    output_file.write_text(
        json.dumps(document, indent=2),
        encoding="utf-8",
    )

    # 9. Sum reported costs separately for each currency.
    totals_by_currency = defaultdict(Decimal)
    missing_amounts = 0

    for row in rows:
        amount = row.get("amount") or {}
        value = amount.get("value")
        currency = amount.get("currency")

        if value is None or not currency:
            missing_amounts += 1
            continue

        try:
            totals_by_currency[currency.upper()] += Decimal(str(value))
        except InvalidOperation as exc:
            raise RuntimeError(
                f"Invalid cost amount encountered. "
                f"Raw response saved to: {output_file}"
            ) from exc

    # 10. Print the extraction summary.
    print(f"Mode: {'Monthly' if monthly else 'Daily'}")
    print(f"Window start (UTC): {start.isoformat()}")
    print(f"Window end (UTC, exclusive): {end.isoformat()}")
    print(f"Pages retrieved: {len(pages)}")
    print(f"Cost records: {len(rows)}")

    for currency, total in sorted(totals_by_currency.items()):
        print(f"Reported cost ({currency}): {total:.8f}")

    if missing_amounts:
        print(
            f"Records excluded from cost totals "
            f"due to missing amount or currency: {missing_amounts}"
        )

    print(f"Saved to: {output_file}")

    if not rows:
        print(
            "Extraction succeeded, but no cost records were returned."
        )

    return output_file


if __name__ == "__main__":
    # Read the optional date and monthly flag from the terminal.
    parser = argparse.ArgumentParser(
        description="Extract OpenAI organization costs as raw JSON."
    )

    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=(
            datetime.now(timezone.utc).date() - timedelta(days=1)
        ),
        help=(
            "UTC date in YYYY-MM-DD format. "
            "Defaults to yesterday."
        ),
    )

    parser.add_argument(
        "--monthly",
        action="store_true",
        help=(
            "Extract the calendar month containing --date, "
            "through the last completed UTC day."
        ),
    )

    args = parser.parse_args()

    try:
        extract_costs(
            args.date,
            monthly=args.monthly,
        )
    except (
        RuntimeError,
        requests.RequestException,
        ValueError,
        OSError,
    ) as exc:
        raise SystemExit(f"Extraction failed: {exc}")