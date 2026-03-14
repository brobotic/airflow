#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import os
import shutil
import subprocess
import sys
from typing import Any
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table


load_dotenv()


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
        {month_year_filter}
        ORDER BY metric_key DESC
        LIMIT {limit};
    """,
    "top-tags": """
        WITH base AS (
            SELECT
                COALESCE(watched_date, entry_date) AS activity_date,
                film_name,
                film_year,
                letterboxd_uri,
                rating,
                rewatch,
                tags
            FROM letterboxd_diary
            {raw_year_filter}
        ),
        tags_expanded AS (
            SELECT
                lower(trim(tag_value)) AS tag,
                b.film_name,
                b.film_year,
                b.letterboxd_uri,
                b.rating,
                b.rewatch
            FROM base b
            CROSS JOIN LATERAL unnest(string_to_array(COALESCE(b.tags, ''), ',')) AS tag_value
            WHERE trim(tag_value) <> ''
        )
        SELECT
            tag,
            COUNT(*)::INTEGER AS films_logged,
            COUNT(DISTINCT COALESCE(letterboxd_uri, film_name))::INTEGER AS unique_titles,
            ROUND(AVG(rating)::numeric, 2) AS avg_rating,
            SUM(CASE WHEN rewatch THEN 1 ELSE 0 END)::INTEGER AS rewatch_count
        FROM tags_expanded
        GROUP BY tag
        ORDER BY films_logged DESC, tag
        LIMIT {limit};
    """,
    "top-film-years": """
        WITH base AS (
            SELECT
                COALESCE(watched_date, entry_date) AS activity_date,
                film_name,
                film_year,
                letterboxd_uri,
                rating,
                rewatch
            FROM letterboxd_diary
            {raw_year_filter}
        )
        SELECT
            film_year,
            COUNT(*)::INTEGER AS films_logged,
            COUNT(DISTINCT COALESCE(letterboxd_uri, film_name))::INTEGER AS unique_titles,
            ROUND(AVG(rating)::numeric, 2) AS avg_rating,
            SUM(CASE WHEN rewatch THEN 1 ELSE 0 END)::INTEGER AS rewatch_count
        FROM base
        WHERE film_year IS NOT NULL
        GROUP BY film_year
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
        {month_year_filter}
        ORDER BY rewatch_count DESC, metric_key DESC
        LIMIT {limit};
    """,
    "year-overview": """
        SELECT
            {year}::text AS watched_year,
            COUNT(*) AS films_logged,
            COUNT(DISTINCT (COALESCE(film_name, '') || '|' || COALESCE(film_year::text, ''))) AS unique_titles,
            ROUND(AVG(rating)::numeric, 2) AS avg_rating,
            COUNT(*) FILTER (WHERE COALESCE(rewatch, FALSE)) AS rewatch_count,
            MIN(COALESCE(watched_date, entry_date)) AS first_watch_date,
            MAX(COALESCE(watched_date, entry_date)) AS last_watch_date
        FROM letterboxd_diary
        WHERE EXTRACT(YEAR FROM COALESCE(watched_date, entry_date)) = {year};
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
        {recent_entries_filter}
        ORDER BY COALESCE(watched_date, entry_date) DESC NULLS LAST, diary_id DESC
        LIMIT {limit};
    """,
    "film-year-entries": """
        SELECT
            COALESCE(watched_date, entry_date) AS activity_date,
            entry_date,
            watched_date,
            film_name,
            film_year,
            rating,
            rewatch,
            tags,
            letterboxd_uri
        FROM letterboxd_diary
        WHERE film_year = {film_year}
        ORDER BY COALESCE(watched_date, entry_date) DESC NULLS LAST, diary_id DESC;
    """,
    "movie-entries": """
        SELECT
            COALESCE(watched_date, entry_date) AS activity_date,
            entry_date,
            watched_date,
            film_name,
            film_year,
            rating,
            rewatch,
            tags,
            letterboxd_uri,
            diary_id
        FROM letterboxd_diary
        {movie_entries_filter}
        ORDER BY COALESCE(watched_date, entry_date) DESC NULLS LAST, diary_id DESC;
    """,
    "never-rewatched-movies": """
        WITH base AS (
            SELECT
                COALESCE(watched_date, entry_date) AS activity_date,
                film_name,
                film_year,
                rating,
                rewatch
            FROM letterboxd_diary
            {never_rewatched_filter}
        )
        SELECT
            film_name,
            film_year,
            COUNT(*)::INTEGER AS diary_entries,
            ROUND(AVG(rating)::numeric, 2) AS avg_rating,
            MIN(activity_date) AS first_watch_date,
            MAX(activity_date) AS last_watch_date
        FROM base
        WHERE COALESCE(trim(film_name), '') <> ''
        GROUP BY film_name, film_year
        HAVING SUM(CASE WHEN COALESCE(rewatch, FALSE) THEN 1 ELSE 0 END) = 0
        ORDER BY last_watch_date DESC NULLS LAST, diary_entries DESC, film_name
        LIMIT {limit};
    """,
}


def _sql_quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
        "--year",
        type=int,
        help=(
            "Filter supported queries to a specific watched year (for example: 2024). "
            "Required for --query year-overview."
        ),
    )
    parser.add_argument(
        "--film-year",
        type=int,
        help=(
            "Filter queries by movie release year (film_year). "
            "Required for --query film-year-entries."
        ),
    )
    parser.add_argument(
        "--film-name",
        help=(
            "Filter queries by exact movie title (case-insensitive). "
            "Required for --query movie-entries."
        ),
    )
    parser.add_argument(
        "--execution-mode",
        choices=["remote", "direct", "docker"],
        default=os.getenv("POSTGRES_EXECUTION_MODE", "remote"),
        help=(
            "How to run psql: 'remote' for network Postgres access or 'docker' for "
            "docker compose exec (default: remote)."
        ),
    )
    parser.add_argument(
        "--service",
        default=os.getenv("POSTGRES_SERVICE", "postgres_movies"),
        help="Docker Compose Postgres service name used by --execution-mode docker.",
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
        "--host",
        default=os.getenv("POSTGRES_HOST", os.getenv("PGHOST", "127.0.0.1")),
        help="Postgres host used by --execution-mode remote (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", os.getenv("PGPORT", "5432"))),
        help="Postgres port used by --execution-mode remote (default: 5432).",
    )
    parser.add_argument(
        "--password-env",
        default=os.getenv("POSTGRES_PASSWORD_ENV", "POSTGRES_PASSWORD"),
        help=(
            "Environment variable name containing the Postgres password for "
            "--execution-mode remote (default: POSTGRES_PASSWORD)."
        ),
    )
    parser.add_argument(
        "--compose-file",
        default=str(Path(__file__).resolve().parent.parent / "postgres.yaml"),
        help="Path to docker compose file used by --execution-mode docker.",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Disable Rich table output and print raw psql rows for easier piping.",
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


def ensure_direct_prerequisites() -> None:
    if shutil.which("psql") is None:
        print("Error: psql command not found in PATH.", file=sys.stderr)
        raise SystemExit(1)


def _column_display_name(column_name: str) -> str:
    return column_name.replace("_", " ").title()


def _column_style(column_name: str) -> tuple[str | None, str]:
    name = column_name.lower()
    if name in {"metric_type"}:
        return "cyan", "left"
    if name in {"metric_key", "tag", "watched_month", "watched_year", "film_year", "tags"}:
        return "bright_cyan", "left"
    if name in {"films_logged", "unique_titles", "rewatch_count"}:
        color = "green" if name != "rewatch_count" else "yellow"
        return color, "right"
    if name in {"avg_rating", "rating"}:
        return "magenta", "right"
    if name in {"first_watch_date", "last_watch_date", "activity_date"}:
        return "blue", "left"
    if name in {"rewatch", "rewatch_pct"}:
        return "yellow", "right"
    return None, "left"


def render_table(args: argparse.Namespace, sql: str, csv_output: str) -> None:
    reader = csv.DictReader(io.StringIO(csv_output))
    fieldnames = reader.fieldnames or []
    rows = list(reader)

    title = f"Letterboxd Diary Mart ({args.query})"
    if args.year is not None:
        title += f" | year={args.year}"
    if args.film_year is not None and args.query == "movie-entries":
        title += f" | film_year={args.film_year}"
    if args.film_name:
        title += f" | film={args.film_name}"

    console = Console()

    if not fieldnames:
        console.print("No columns returned by query.", style="yellow")
        return

    table = Table(title=title)
    for field in fieldnames:
        style, justify = _column_style(field)
        table.add_column(_column_display_name(field), style=style, justify=justify) # type: ignore

    for row in rows:
        table.add_row(*[(row.get(field) or "") for field in fieldnames])

    if not rows:
        console.print("No rows returned for this query.", style="yellow")
        return

    console.print(table)
    console.print(f"Showing [bold]{len(rows)}[/bold] row(s) (limit={args.limit}).")


def run_direct_query(args: argparse.Namespace, sql: str, psql_flags: list[str]) -> int:
    ensure_direct_prerequisites()

    command = [
        "psql",
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-U",
        args.user,
        "-d",
        args.db,
        "--csv",
        *psql_flags,
        "-c",
        sql,
    ]

    env = os.environ.copy()
    password = env.get(args.password_env)
    if password:
        env["PGPASSWORD"] = password

    print(
        f"Running query '{args.query}' on {args.db} at {args.host}:{args.port} "
        "using direct psql..."
    )
    print(f"SQL: {sql}\n")

    result = subprocess.run(command, text=True, env=env, capture_output=True, check=False)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if stderr:
            print(stderr, file=sys.stderr)
        return result.returncode

    if args.no_header:
        print(result.stdout, end="")
        return 0

    render_table(args=args, sql=sql, csv_output=result.stdout)
    return result.returncode


def run_docker_query(
    args: argparse.Namespace,
    sql: str,
    psql_flags: list[str],
    compose_file: Path,
) -> int:
    ensure_prerequisites(compose_file=compose_file, service=args.service)

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
        "--csv",
        *psql_flags,
        "-c",
        sql,
    ]

    print(f"Running query '{args.query}' on {args.db} via service {args.service}...")
    print(f"SQL: {sql}\n")

    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if stderr:
            print(stderr, file=sys.stderr)
        return result.returncode

    if args.no_header:
        print(result.stdout, end="")
        return 0

    render_table(args=args, sql=sql, csv_output=result.stdout)
    return 0


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        print("Error: --limit must be > 0", file=sys.stderr)
        return 1
    if args.port <= 0:
        print("Error: --port must be > 0", file=sys.stderr)
        return 1
    if args.year is not None and (args.year < 1870 or args.year > 2100):
        print("Error: --year must be between 1870 and 2100", file=sys.stderr)
        return 1
    if args.film_year is not None and (args.film_year < 1870 or args.film_year > 2100):
        print("Error: --film-year must be between 1870 and 2100", file=sys.stderr)
        return 1
    if args.query == "year-overview" and args.year is None:
        print("Error: --query year-overview requires --year", file=sys.stderr)
        return 1
    if args.query == "film-year-entries" and args.film_year is None:
        print("Error: --query film-year-entries requires --film-year", file=sys.stderr)
        return 1
    if args.query == "movie-entries":
        if not args.film_name or not args.film_name.strip():
            print("Error: --query movie-entries requires --film-name", file=sys.stderr)
            return 1

    month_year_filter = ""
    if args.year is not None and args.query in {"monthly", "rewatch-months"}:
        month_year_filter = f"AND metric_key LIKE '{args.year}-%'"

    recent_entries_filters: list[str] = []
    if args.year is not None and args.query == "recent-entries":
        recent_entries_filters.append(
            f"EXTRACT(YEAR FROM COALESCE(watched_date, entry_date)) = {args.year}"
        )
    if args.film_year is not None and args.query == "recent-entries":
        recent_entries_filters.append(f"film_year = {args.film_year}")

    recent_entries_filter = ""
    if recent_entries_filters:
        recent_entries_filter = "WHERE " + " AND ".join(recent_entries_filters)

    raw_year_filter = ""
    if args.year is not None and args.query in {"top-tags", "top-film-years"}:
        raw_year_filter = (
            "WHERE EXTRACT(YEAR FROM COALESCE(watched_date, entry_date)) = "
            f"{args.year}"
        )

    movie_entries_filter = ""
    if args.query == "movie-entries":
        movie_entries_filters = [
            f"lower(trim(film_name)) = lower(trim({_sql_quote_literal(args.film_name.strip())}))"
        ]
        if args.film_year is not None:
            movie_entries_filters.append(f"film_year = {args.film_year}")
        movie_entries_filter = "WHERE " + " AND ".join(movie_entries_filters)

    never_rewatched_filters: list[str] = []
    if args.year is not None and args.query == "never-rewatched-movies":
        never_rewatched_filters.append(
            f"EXTRACT(YEAR FROM COALESCE(watched_date, entry_date)) = {args.year}"
        )
    if args.film_year is not None and args.query == "never-rewatched-movies":
        never_rewatched_filters.append(f"film_year = {args.film_year}")

    never_rewatched_filter = ""
    if never_rewatched_filters:
        never_rewatched_filter = "WHERE " + " AND ".join(never_rewatched_filters)

    sql = QUERY_SQL[args.query].format(
        limit=args.limit,
        year=args.year,
        film_year=args.film_year,
        month_year_filter=month_year_filter,
        recent_entries_filter=recent_entries_filter,
        raw_year_filter=raw_year_filter,
        movie_entries_filter=movie_entries_filter,
        never_rewatched_filter=never_rewatched_filter,
    ).strip()

    psql_flags = ["-P", "pager=off"]
    if args.no_header:
        psql_flags.extend(["-t"])

    if args.execution_mode == "docker":
        compose_file = Path(args.compose_file).expanduser().resolve()
        return run_docker_query(
            args=args,
            sql=sql,
            psql_flags=psql_flags,
            compose_file=compose_file,
        )

    return run_direct_query(args=args, sql=sql, psql_flags=psql_flags)


if __name__ == "__main__":
    raise SystemExit(main())
