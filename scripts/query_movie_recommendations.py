#!/usr/bin/env python3

import argparse
import os
import time
from typing import Any

from elasticsearch import Elasticsearch
from rich.console import Console
from rich.table import Table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch mart_titles_enriched and print random movie "
            "recommendations where rating_bucket is 'excellent'."
        )
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of recommendations to return (default: 10)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Optional seed for deterministic random ordering",
    )
    parser.add_argument(
        "--min-votes",
        type=int,
        default=10000,
        help="Minimum number of votes required (default: 10000)",
    )
    parser.add_argument(
        "--era",
        help=(
            "Filter by era value from mart_titles_enriched, e.g. "
            "'1980s-1990s', '2010s', '2020s+'"
        ),
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Filter by exact start_year, e.g. 1999",
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


def query_random_excellent_movies(
    client: Elasticsearch,
    index: str,
    count: int,
    seed: int,
    min_votes: int,
    era: str | None,
    start_year: int | None,
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        {"term": {"rating_bucket.keyword": "excellent"}},
        {"term": {"title_type.keyword": "movie"}},
    ]

    if min_votes > 0:
        filters.append({"range": {"num_votes": {"gte": min_votes}}})
    if era:
        filters.append({"term": {"era.keyword": era}})
    if start_year is not None:
        filters.append({"term": {"start_year": start_year}})

    query = {
        "size": count,
        "track_total_hits": False,
        "_source": [
            "tconst",
            "primary_title",
            "start_year",
            "average_rating",
            "num_votes",
            "genres",
        ],
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "filter": filters,
                        "must": [{"exists": {"field": "primary_title"}}],
                    }
                },
                "random_score": {"seed": seed},
                "boost_mode": "replace",
            }
        },
    }

    response = client.search(index=index, body=query)
    hits = response.get("hits", {}).get("hits", [])
    rows: list[dict[str, Any]] = []

    for hit in hits:
        source = hit.get("_source", {})
        rows.append(
            {
                "tconst": source.get("tconst") or "",
                "title": source.get("primary_title") or "",
                "year": source.get("start_year"),
                "rating": source.get("average_rating"),
                "votes": source.get("num_votes"),
                "genres": source.get("genres") or "",
                "director": "",
                "cinematographer": "",
            }
        )

    return rows


def enrich_with_movie_credits(
    client: Elasticsearch,
    credits_index: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    tconst_to_row: dict[str, dict[str, Any]] = {}
    docs: list[dict[str, Any]] = []

    for row in rows:
        tconst = row.get("tconst")
        if not tconst:
            continue
        tconst_to_row[tconst] = row
        docs.append(
            {
                "_index": credits_index,
                "_id": tconst,
                "_source": ["directors_names", "dop_names"],
            }
        )

    if not docs:
        return rows

    response = client.mget(body={"docs": docs})
    for doc in response.get("docs", []):
        if not doc.get("found"):
            continue
        tconst = doc.get("_id")
        if not tconst:
            continue
        row = tconst_to_row.get(tconst)
        if not row:
            continue

        source = doc.get("_source", {})
        row["director"] = source.get("directors_names") or ""
        row["cinematographer"] = source.get("dop_names") or ""

    return rows


def render_table(rows: list[dict[str, Any]]) -> None:
    console = Console()
    table = Table(title="Random Excellent Movie Recommendations")
    table.add_column("#", justify="right")
    table.add_column("Title", style="cyan")
    table.add_column("Year", justify="right")
    table.add_column("Director", style="green")
    table.add_column("Cinematographer", style="yellow")
    table.add_column("Rating", justify="right", style="green")
    table.add_column("Votes", justify="right")
    table.add_column("Genres", style="magenta")

    for index, row in enumerate(rows, start=1):
        year = "" if row["year"] is None else str(row["year"])
        rating = "" if row["rating"] is None else f"{row['rating']:.1f}"
        votes = "" if row["votes"] is None else f"{row['votes']:,}"
        table.add_row(
            str(index),
            row["title"],
            year,
            row["director"],
            row["cinematographer"],
            rating,
            votes,
            row["genres"],
        )

    if not rows:
        console.print(
            "No matching movies found. Check that the index contains documents with "
            "rating_bucket='excellent' and title_type='movie'.",
            style="yellow",
        )
        return

    console.print(table)


def main() -> int:
    args = parse_args()

    if args.count <= 0:
        Console().print("--count must be greater than 0.", style="red")
        return 2
    if args.min_votes < 0:
        Console().print("--min-votes must be 0 or greater.", style="red")
        return 2

    index = os.getenv("ELASTICSEARCH_INDEX", "mart_titles_enriched")
    credits_index = os.getenv("ELASTICSEARCH_MOVIE_CREDITS_INDEX", "mart_movie_credits")
    seed = args.seed if args.seed is not None else int(time.time())

    client = create_client()
    try:
        if not client.indices.exists(index=index):
            Console().print(
                f"Elasticsearch index '[bold]{index}[/bold]' does not exist.",
                style="red",
            )
            return 1
        if not client.indices.exists(index=credits_index):
            Console().print(
                f"Elasticsearch index '[bold]{credits_index}[/bold]' does not exist.",
                style="red",
            )
            return 1

        rows = query_random_excellent_movies(
            client=client,
            index=index,
            count=args.count,
            seed=seed,
            min_votes=args.min_votes,
            era=args.era,
            start_year=args.start_year,
        )
        rows = enrich_with_movie_credits(
            client=client,
            credits_index=credits_index,
            rows=rows,
        )
        render_table(rows)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
