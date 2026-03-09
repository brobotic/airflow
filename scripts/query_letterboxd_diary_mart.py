#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


QUERY_SQL = {
    "overview": """
        SELECT
            metric_type,
            metric_key,
            films_logged,
            unique_titles,
            ROUND(avg_rating::numeric, 2) AS avg_rating,
            rewatch_count,
            first_watch_date,
            last_watch_date
        FROM mart_letterboxd_diary
        WHERE metric_type = 'overall';
    """,
    "monthly": """
        SELECT
            metric_key AS watched_month,
            films_logged,
            unique_titles,
            ROUND(avg_rating::numeric, 2) AS avg_rating,
            rewatch_count
        FROM mart_letterboxd_diary
        WHERE metric_type = 'watched_month'
        ORDER BY metric_key DESC
        LIMIT {limit};
    """,
    "top-tags": """
        SELECT
            metric_key AS tag,
            films_logged,
            unique_titles,
            ROUND(avg_rating::numeric, 2) AS avg_rating,
            rewatch_count
        FROM mart_letterboxd_diary
        WHERE metric_type = 'tag'
        ORDER BY films_logged DESC, tag
        LIMIT {limit};
    """,
    "top-film-years": """
        SELECT
            metric_key AS film_year,
            films_logged,
            unique_titles,
            ROUND(avg_rating::numeric, 2) AS avg_rating,
            rewatch_count
        FROM mart_letterboxd_diary
        WHERE metric_type = 'film_year'
        ORDER BY avg_rating DESC NULLS LAST, films_logged DESC
        LIMIT {limit};
    """,
    "rewatch-months": """
        SELECT
            metric_key AS watched_month,
            films_logged,
            rewatch_count,
            ROUND((rewatch_count::numeric / NULLIF(films_logged, 0)) * 100, 1) AS rewatch_pct
        FROM mart_letterboxd_diary
        WHERE metric_type = 'watched_month'
        ORDER BY rewatch_count DESC, metric_key DESC
        LIMIT {limit};
    """,
    "recent-entries": """
        SELECT
            COALESCE(watched_date, entry_date) AS activity_date,
            film_name,
            film_year,
            rating,
            rewatch,
            tags
        FROM letterboxd_diary
        ORDER BY COALESCE(watched_date, entry_date) DESC NULLS LAST, diary_id DESC
        LIMIT {limit};
    """,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ready-made analytical queries for Letterboxd diary marts.",
    )
    parser.add_argument(
        "--query",
        choices=sorted(QUERY_SQL.keys()),
        default="overview",
        help="Which query template to run (default: overview).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Row limit for query templates that support it (default: 20).",
    )
    parser.add_argument(
        "--service",
        default=os.getenv("POSTGRES_SERVICE", "postgres_movies"),
        help="Docker Compose Postgres service name (default: postgres_movies).",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("POSTGRES_USER", "movies_user"),
        help="Postgres user (default: movies_user).",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("POSTGRES_DB", "movies_db"),
        help="Postgres database (default: movies_db).",
    )
    parser.add_argument(
        "--compose-file",
        default=str(Path(__file__).resolve().parent.parent / "postgres.yaml"),
        help="Path to docker compose file (default: ./postgres.yaml).",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Disable psql headers and aligned output for easier piping.",
    )
    return parser.parse_args()


def ensure_prerequisites(compose_file: Path, service: str) -> None:
    if shutil.which("docker") is None:
        print("Error: docker command not found in PATH.", file=sys.stderr)
        raise SystemExit(1)

    if not compose_file.is_file():
        print(f"Error: compose file not found at {compose_file}", file=sys.stderr)
        raise SystemExit(1)

    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "ps",
            "--status",
            "running",
            "--services",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if stderr:
            print(stderr, file=sys.stderr)
        raise SystemExit(result.returncode)

    running_services = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    if service not in running_services:
        print(
            f"Error: service '{service}' is not running. Start it with scripts/start_postgres_stack.sh",
            file=sys.stderr,
        )
        raise SystemExit(1)


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        print("Error: --limit must be > 0", file=sys.stderr)
        return 1

    compose_file = Path(args.compose_file).expanduser().resolve()
    ensure_prerequisites(compose_file=compose_file, service=args.service)

    sql = QUERY_SQL[args.query].format(limit=args.limit).strip()

    psql_flags = ["-P", "pager=off"]
    if args.no_header:
        psql_flags.extend(["-A", "-t"])

    command = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        args.service,
        "psql",
        "-U",
        args.user,
        "-d",
        args.db,
        *psql_flags,
        "-c",
        sql,
    ]

    print(f"Running query '{args.query}' on {args.db} via service {args.service}...")
    print(f"SQL: {sql}\n")

    result = subprocess.run(command, text=True, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
