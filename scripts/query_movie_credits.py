#!/usr/bin/env python3

import argparse
import os
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from rich.console import Console
from rich.table import Table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch movie credits and print title/year/director/DoP "
            "for movies by a specific director or cinematographer."
        )
    )
    person_group = parser.add_mutually_exclusive_group(required=True)
    person_group.add_argument(
        "--director",
        help="Director name to search for, e.g. 'Christopher Nolan'",
    )
    person_group.add_argument(
        "--cinematographer",
        help="Cinematographer/DoP name to search for, e.g. 'Roger Deakins'",
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


def split_names(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part and part.strip()]


def has_exact_name_match(names: list[str], requested_name: str) -> bool:
    requested_lower = requested_name.strip().lower()
    for name in names:
        if name.lower() == requested_lower:
            return True
    return False


def query_movies_by_person(
    client: Elasticsearch,
    index: str,
    person_name: str,
    search_field: str,
) -> list[dict[str, Any]]:
    query = {
        "_source": [
            "primary_title",
            "start_year",
            "directors_names",
            "dop_names",
        ],
        "query": {
            "bool": {
                "should": [
                    {"term": {f"{search_field}.keyword": person_name}},
                    {"match_phrase": {search_field: person_name}},
                ],
                "minimum_should_match": 1,
            }
        },
    }

    rows: list[dict[str, Any]] = []
    for hit in scan(client=client, index=index, query=query):
        source = hit.get("_source", {})
        matched_names = split_names(source.get(search_field))

        if not has_exact_name_match(matched_names, person_name):
            continue

        rows.append(
            {
                "title": source.get("primary_title") or "",
                "year": source.get("start_year"),
                "director": source.get("directors_names") or "",
                "dop": source.get("dop_names") or "",
            }
        )

    rows.sort(key=lambda row: ((row["year"] is None), row["year"] or 0, row["title"]))
    return rows


def render_table(rows: list[dict[str, Any]], label: str, person_name: str) -> None:
    console = Console()
    table = Table(title=f"Movies for {label}: {person_name}")
    table.add_column("Title", style="cyan")
    table.add_column("Year", justify="right")
    table.add_column("Director", style="green")
    table.add_column("DoP", style="magenta")

    for row in rows:
        year = "" if row["year"] is None else str(row["year"])
        table.add_row(row["title"], year, row["director"], row["dop"])

    if not rows:
        console.print(f"No movies found for {label}: [bold]{person_name}[/bold]")
        return

    console.print(table)


def main() -> int:
    args = parse_args()
    index = os.getenv("ELASTICSEARCH_MOVIE_CREDITS_INDEX", "mart_movie_credits")
    if args.director:
        person_name = args.director
        search_field = "directors_names"
        label = "director"
    else:
        person_name = args.cinematographer
        search_field = "dop_names"
        label = "cinematographer"

    client = create_client()
    try:
        if not client.indices.exists(index=index):
            Console().print(
                f"Elasticsearch index '[bold]{index}[/bold]' does not exist.",
                style="red",
            )
            return 1

        rows = query_movies_by_person(
            client=client,
            index=index,
            person_name=person_name,
            search_field=search_field,
        )
        render_table(rows, label, person_name)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
