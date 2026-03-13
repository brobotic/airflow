#!/usr/bin/env python3

import argparse
import os
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from rich.console import Console
from rich.table import Table


load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch mart_letterboxd_diary aggregates and print them in a "
            "Rich table for quick analysis."
        )
    )
    parser.add_argument(
        "--metric-type",
        choices=["overall", "watched_month", "film_year", "tag", "all"],
        default="all",
        help="Metric slice to query (default: all).",
    )
    parser.add_argument(
        "--metric-key",
        help=(
            "Optional exact metric key filter, e.g. '2026-03' for watched_month "
            "or 'sci-fi' for tag."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of rows to print (default: 25).",
    )
    parser.add_argument(
        "--random",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Show a random sample of metric keys (default: enabled). "
            "Sampled rows are still sorted by --sort-by; use --no-random for full-index ordering."
        ),
    )
    parser.add_argument(
        "--sort-by",
        choices=["films_logged", "unique_titles", "avg_rating", "rewatch_count", "metric_key"],
        default="films_logged",
        help="Sort column in output table or sampled rows (default: films_logged).",
    )
    parser.add_argument(
        "--ascending",
        action="store_true",
        help="Sort ascending (default is descending).",
    )
    return parser.parse_args()


def create_client() -> Elasticsearch:
    host = os.getenv("ELASTICSEARCH_HOST", "http://192.168.1.60:9200")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    password = os.getenv("ELASTICSEARCH_PASSWORD")

    if api_key:
        return Elasticsearch(hosts=[host], api_key=api_key)
    if username and password:
        return Elasticsearch(hosts=[host], basic_auth=(username, password))
    return Elasticsearch(hosts=[host])


def _to_display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _to_sort_value(value: Any) -> Any:
    if value is None:
        return -1
    if isinstance(value, str):
        return value.lower()
    return value


def query_metrics(
    client: Elasticsearch,
    index: str,
    metric_type: str,
    metric_key: str | None,
    limit: int,
    random_sample: bool,
    sort_by: str,
    ascending: bool,
) -> list[dict[str, Any]]:
    must_filters: list[dict[str, Any]] = []

    if metric_type != "all":
        must_filters.append({"term": {"metric_type.keyword": metric_type}})

    if metric_key:
        must_filters.append({"term": {"metric_key.keyword": metric_key}})

    base_query: dict[str, Any] = {
        "_source": [
            "metric_type",
            "metric_key",
            "films_logged",
            "unique_titles",
            "avg_rating",
            "rewatch_count",
            "first_watch_date",
            "last_watch_date",
            "last_refreshed_at",
        ],
        "query": {"bool": {"filter": must_filters}} if must_filters else {"match_all": {}},
    }

    if random_sample:
        random_query: dict[str, Any] = {
            **base_query,
            "size": limit,
            "query": {
                "function_score": {
                    "query": base_query["query"],
                    "random_score": {},
                }
            },
        }

        response = client.search(index=index, body=random_query)
        hits = response.get("hits", {}).get("hits", [])
    else:
        hits = list(scan(client=client, index=index, query=base_query))

    rows: list[dict[str, Any]] = []
    for hit in hits:
        source = hit.get("_source", {})
        rows.append(
            {
                "metric_type": source.get("metric_type") or "",
                "metric_key": source.get("metric_key") or "",
                "films_logged": source.get("films_logged"),
                "unique_titles": source.get("unique_titles"),
                "avg_rating": source.get("avg_rating"),
                "rewatch_count": source.get("rewatch_count"),
                "first_watch_date": source.get("first_watch_date"),
                "last_watch_date": source.get("last_watch_date"),
                "last_refreshed_at": source.get("last_refreshed_at"),
            }
        )

    rows.sort(
        key=lambda row: (
            _to_sort_value(row.get(sort_by)),
            _to_sort_value(row.get("metric_key")),
        ),
        reverse=not ascending,
    )

    if random_sample:
        # Random mode samples first, then sorts only the sampled rows for readability.
        return rows

    return rows[:limit]


def render_table(
    rows: list[dict[str, Any]],
    metric_type: str,
    metric_key: str | None,
    index: str,
    limit: int,
    random_sample: bool,
) -> None:
    console = Console()

    mode = "random sample" if random_sample else "sorted"
    title = f"Letterboxd Diary Metrics ({metric_type}, {mode})"
    if metric_key:
        title += f" | key={metric_key}"

    table = Table(title=title)
    table.add_column("Metric Type", style="cyan")
    table.add_column("Metric Key", style="bright_cyan")
    table.add_column("Films", justify="right", style="green")
    table.add_column("Unique", justify="right", style="green")
    table.add_column("Avg Rating", justify="right", style="magenta")
    table.add_column("Rewatches", justify="right", style="yellow")
    table.add_column("First Watch", style="blue")
    table.add_column("Last Watch", style="blue")
    table.add_column("Refreshed At", style="white")

    for row in rows:
        table.add_row(
            _to_display_value(row.get("metric_type")),
            _to_display_value(row.get("metric_key")),
            _to_display_value(row.get("films_logged")),
            _to_display_value(row.get("unique_titles")),
            _to_display_value(row.get("avg_rating")),
            _to_display_value(row.get("rewatch_count")),
            _to_display_value(row.get("first_watch_date")),
            _to_display_value(row.get("last_watch_date")),
            _to_display_value(row.get("last_refreshed_at")),
        )

    if not rows:
        console.print(
            f"No mart rows found in index '[bold]{index}[/bold]' for metric_type='[bold]{metric_type}[/bold]'."
        )
        if metric_key:
            console.print(f"Applied metric_key filter: [bold]{metric_key}[/bold]")
        return

    console.print(table)
    console.print(f"Showing [bold]{len(rows)}[/bold] row(s) (limit={limit}) from index '[bold]{index}[/bold]'.")


def main() -> int:
    args = parse_args()

    if args.limit <= 0:
        Console().print("--limit must be greater than 0", style="red")
        return 1

    index = os.getenv("ELASTICSEARCH_LETTERBOXD_DIARY_INDEX", "mart_letterboxd_diary")

    client = create_client()
    try:
        if not client.indices.exists(index=index):
            Console().print(
                f"Elasticsearch index '[bold]{index}[/bold]' does not exist.",
                style="red",
            )
            return 1

        rows = query_metrics(
            client=client,
            index=index,
            metric_type=args.metric_type,
            metric_key=args.metric_key,
            limit=args.limit,
            random_sample=args.random,
            sort_by=args.sort_by,
            ascending=args.ascending,
        )
        render_table(
            rows=rows,
            metric_type=args.metric_type,
            metric_key=args.metric_key,
            index=index,
            limit=args.limit,
            random_sample=args.random,
        )
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
