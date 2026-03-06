#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dags.notifications import (  # noqa: E402
    DISCORD_COLOR_FAILURE,
    DISCORD_COLOR_SUCCESS,
    post_discord_embed,
)

DEFAULT_PAYLOAD: dict[str, Any] = {
    "env": "local",
    "dag_id": "mart_episode_credits",
    "task_id": "extract_and_load",
    "run_id": "manual__2026-03-06T10:07:05.460543+00:00",
    "error": (
        'could not extend file "base/16384/16793.2": No space left on device\n'
        "HINT:  Check free disk space.\n"
    ),
    "log_url": (
        "http://localhost:8080/dags/mart_episode_credits/runs/"
        "manual__2026-03-06T10%3A07%3A05.460543%2B00%3A00/tasks/"
        "extract_and_load?try_number=1"
    ),
}

DEFAULT_SUCCESS_PAYLOAD: dict[str, Any] = {
    "status": "completed",
    "table": "title_episode",
    "row_count": 9502275,
    "sample_rows_logged": 5,
    "run_id": "manual__2026-03-05T14:32:51.830229+00:00",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a test Discord notification using dags/notifications.py embed formatting."
    )
    parser.add_argument(
        "--payload-file",
        help="Path to a JSON file containing the payload dictionary.",
    )
    parser.add_argument(
        "--payload-json",
        help="Inline JSON string payload (overrides default unless --payload-file is provided).",
    )
    parser.add_argument(
        "--title",
        default=None,
        help="Embed title. If omitted, defaults to ❌ <dag_id> for failure or <table> for success.",
    )
    parser.add_argument(
        "--footer",
        default=None,
        help="Optional footer text.",
    )
    status_group = parser.add_mutually_exclusive_group()
    status_group.add_argument(
        "--success",
        action="store_true",
        help="Use success color preset.",
    )
    status_group.add_argument(
        "--failure",
        action="store_true",
        help="Use failure color preset (default).",
    )
    return parser.parse_args()


def load_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.payload_file:
        with open(args.payload_file, "r", encoding="utf-8") as payload_file:
            return json.load(payload_file)

    if args.payload_json:
        return json.loads(args.payload_json)

    if args.success:
        return dict(DEFAULT_SUCCESS_PAYLOAD)

    return dict(DEFAULT_PAYLOAD)


def normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized_payload = dict(payload)
    if str(normalized_payload.get("env") or "").strip().lower() == "local":
        normalized_payload.pop("env", None)
    return normalized_payload


def main() -> int:
    args = parse_args()

    try:
        payload = normalize_payload(load_payload(args))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Failed to load payload: {exc}", file=sys.stderr)
        return 1

    color = DISCORD_COLOR_SUCCESS if args.success else DISCORD_COLOR_FAILURE
    if args.title:
        title = args.title
    elif args.success:
        table_name = str(payload.get("table") or "Airflow DAG completed")
        title = f"✅ {table_name}"
    else:
        dag_id = str(payload.get("dag_id") or "unknown")
        title = f"❌ {dag_id}"

    post_discord_embed(
        title=title,
        data=payload,
        color=color,
        footer_text=args.footer,
    )

    print("Notification sent attempt complete. Check logs for webhook status.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
