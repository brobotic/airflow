#!/usr/bin/env python3

import argparse
import os
import time
from typing import Any

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from rich.console import Console
from rich.table import Table


load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch mart_titles_enriched and print random movie "
            "recommendations filtered by rating_bucket."
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
    parser.add_argument(
        "--genre",
        help="Filter by genre token (non-wildcard), e.g. Comedy",
    )
    parser.add_argument(
        "--rating-bucket",
        nargs="+",
        default=["average", "good", "excellent"],
        help=(
            "One or more rating buckets to filter by "
            "(default: average good excellent)"
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print eligible rating_bucket distribution for current filters",
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
    genre: str | None,
    rating_buckets: list[str],
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        {"terms": {"rating_bucket.keyword": rating_buckets}},
        {"term": {"title_type.keyword": "movie"}},
    ]

    if min_votes > 0:
        filters.append({"range": {"num_votes": {"gte": min_votes}}})
    if era:
        filters.append({"term": {"era.keyword": era}})
    if start_year is not None:
        filters.append({"term": {"start_year": start_year}})
    genre_token = (genre or "").strip()
    if genre_token:
        filters.append(
            {
                "match": {
                    "genres": {
                        "query": genre_token,
                        "operator": "and",
                        "fuzziness": 0,
                    }
                }
            }
        )

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
            "rating_bucket",
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
                "rating_bucket": source.get("rating_bucket") or "",
                "director": "",
                "cinematographer": "",
            }
        )

    return rows


def get_rating_bucket_distribution(
    client: Elasticsearch,
    index: str,
    min_votes: int,
    era: str | None,
    start_year: int | None,
    genre: str | None,
) -> dict[str, int]:
    filters: list[dict[str, Any]] = [{"term": {"title_type.keyword": "movie"}}]

    if min_votes > 0:
        filters.append({"range": {"num_votes": {"gte": min_votes}}})
    if era:
        filters.append({"term": {"era.keyword": era}})
    if start_year is not None:
        filters.append({"term": {"start_year": start_year}})

    genre_token = (genre or "").strip()
    if genre_token:
        filters.append(
            {
                "match": {
                    "genres": {
                        "query": genre_token,
                        "operator": "and",
                        "fuzziness": 0,
                    }
                }
            }
        )

    response = client.search(
        index=index,
        body={
            "size": 0,
            "track_total_hits": False,
            "query": {
                "bool": {
                    "filter": filters,
                    "must": [{"exists": {"field": "primary_title"}}],
                }
            },
            "aggs": {
                "rating_buckets": {
                    "terms": {
                        "field": "rating_bucket.keyword",
                        "size": 20,
                    }
                }
            },
        },
    )

    buckets = (
        response.get("aggregations", {})
        .get("rating_buckets", {})
        .get("buckets", [])
    )
    distribution: dict[str, int] = {}
    for bucket in buckets:
        key = bucket.get("key")
        if not key:
            continue
        distribution[str(key)] = int(bucket.get("doc_count", 0))

    return distribution


def render_debug_distribution(distribution: dict[str, int], requested_buckets: list[str]) -> None:
    console = Console()
    requested_set = {bucket.strip() for bucket in requested_buckets if bucket.strip()}

    total = sum(distribution.values())
    requested_total = sum(
        count for bucket, count in distribution.items() if bucket in requested_set
    )

    console.print("Debug: eligible movie distribution by rating_bucket", style="cyan")
    table = Table(title="Eligible Counts (before random sampling)")
    table.add_column("Bucket", style="blue")
    table.add_column("Count", justify="right")
    table.add_column("Selected", justify="center")

    for bucket, count in sorted(distribution.items(), key=lambda item: item[0]):
        table.add_row(bucket, f"{count:,}", "yes" if bucket in requested_set else "")

    if not distribution:
        console.print("No eligible documents found for current filters.", style="yellow")
        return

    console.print(table)
    console.print(
        f"Total eligible: {total:,} | Selected-bucket eligible: {requested_total:,}",
        style="cyan",
    )


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
    table.add_column("Bucket", style="blue")
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
            row["rating_bucket"],
            rating,
            votes,
            row["genres"],
        )

    if not rows:
        console.print(
            "No matching movies found. Check that the index contains documents with "
            "the requested rating_bucket and title_type='movie'.",
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

        if args.debug:
            distribution = get_rating_bucket_distribution(
                client=client,
                index=index,
                min_votes=args.min_votes,
                era=args.era,
                start_year=args.start_year,
                genre=args.genre,
            )
            render_debug_distribution(
                distribution=distribution,
                requested_buckets=args.rating_bucket,
            )

        rows = query_random_excellent_movies(
            client=client,
            index=index,
            count=args.count,
            seed=seed,
            min_votes=args.min_votes,
            era=args.era,
            start_year=args.start_year,
            genre=args.genre,
            rating_buckets=args.rating_bucket,
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
