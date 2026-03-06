import logging
import os
from datetime import datetime

import requests
from airflow.models import Variable

DISCORD_COLOR_SUCCESS = 3066993
DISCORD_COLOR_FAILURE = 15158332


def get_discord_webhook_url():
    env_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if env_url:
        return env_url
    return Variable.get("DISCORD_WEBHOOK_URL", default_var=None)


def get_environment_name():
    return os.environ.get("ENVIRONMENT") or os.environ.get("ENV") or "unknown"


def post_discord_embed(title: str, data: dict, color: int, footer_text: str | None = None):
    webhook_url = get_discord_webhook_url()
    if not webhook_url:
        logging.info("No Discord webhook configured. Set DISCORD_WEBHOOK_URL or Airflow Variable 'DISCORD_WEBHOOK_URL'.")
        return

    display_data = {}
    for key, value in data.items():
        if key in {"log_url", "status", "dag_id"}:
            continue
        if key == "table" and str(data.get("status") or "").strip().lower() == "completed":
            continue
        if key == "env" and str(value).strip().lower() == "local":
            continue
        display_data[key] = value
    fields = []
    for key, value in display_data.items():
        field_value = "null" if value is None else str(value)
        fields.append(
            {
                "name": key,
                "value": field_value[:1024] if field_value else "-",
                "inline": False,
            }
        )

    embed = {
        "title": title,
        "color": color,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "fields": fields[:25],
    }
    if footer_text:
        embed["footer"] = {"text": footer_text}
    log_url = data.get("log_url")
    if isinstance(log_url, str) and log_url:
        embed["url"] = log_url
    payload = {"embeds": [embed]}

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info("Discord webhook sent. Status: %s", response.status_code)
    except requests.RequestException as exc:
        logging.exception("Discord webhook request error: %s", exc)


def notify_discord_failure(context, title: str = "❌ Airflow task failed"):
    task_instance = context.get("task_instance")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception")
    log_url = task_instance.log_url if task_instance else "unavailable"

    data = {
        "status": "failed",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "error": str(exception),
        "log_url": log_url,
    }
    notification_title = title.replace("Airflow task failed", dag_id)
    post_discord_embed(notification_title, data, DISCORD_COLOR_FAILURE)


def notify_discord_success(
    verify_task_id: str,
    table: str,
    title: str = "✅ Airflow DAG completed",
    **context,
):
    result = context["ti"].xcom_pull(task_ids=verify_task_id) or {}
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    row_count = result.get("row_count", "unknown")
    sample_count = result.get("sample_count", 0)
    run_id = context.get("run_id", "unknown")

    data = {
        "status": "completed",
        "table": table,
        "row_count": row_count,
        "sample_rows_logged": sample_count,
        "run_id": run_id,
    }
    notification_title = f"✅ {table}" if table else title
    post_discord_embed(notification_title, data, DISCORD_COLOR_SUCCESS)

