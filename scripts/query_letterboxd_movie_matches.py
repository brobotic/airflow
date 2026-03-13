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
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE matched_tconst IS NOT NULL) AS matched_rows,
            COUNT(*) FILTER (WHERE matched_tconst IS NULL) AS unmatched_rows,
            ROUND(100.0 * COUNT(*) FILTER (WHERE matched_tconst IS NOT NULL) / NULLIF(COUNT(*), 0), 2) AS match_rate_pct
        FROM mart_letterboxd_movie_matches;
    """,
    "match-quality": """
        SELECT
            match_confidence,
            COUNT(*) AS row_count,
            ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_total
        FROM mart_letterboxd_movie_matches
        GROUP BY match_confidence
        ORDER BY row_count DESC;
    """,
    "rating-gap-monthly": """
        SELECT
            date_trunc('month', activity_date)::date AS month,
            ROUND(AVG(letterboxd_rating)::numeric, 2) AS avg_letterboxd_rating,
            ROUND(AVG(imdb_average_rating)::numeric, 2) AS avg_imdb_rating,
            ROUND((AVG(letterboxd_rating) - AVG(imdb_average_rating))::numeric, 2) AS rating_gap,
            COUNT(*) AS matched_entries
        FROM mart_letterboxd_movie_matches
        WHERE matched_tconst IS NOT NULL
          AND match_confidence IN ('exact_primary_title_year', 'exact_original_title_year')
          AND activity_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT {limit};
    """,
    "top-matched-movies": """
        SELECT
            matched_tconst,
            matched_primary_title,
            matched_start_year,
            COUNT(*) AS diary_watches,
            ROUND(AVG(letterboxd_rating)::numeric, 2) AS avg_letterboxd_rating,
            ROUND(AVG(imdb_average_rating)::numeric, 2) AS avg_imdb_rating,
            MAX(imdb_num_votes) AS imdb_num_votes
        FROM mart_letterboxd_movie_matches
        WHERE matched_tconst IS NOT NULL
        GROUP BY matched_tconst, matched_primary_title, matched_start_year
        ORDER BY diary_watches DESC, imdb_num_votes DESC NULLS LAST
        LIMIT {limit};
    """,
    "unmatched": """
        SELECT
            film_name,
            film_year,
            COUNT(*) AS diary_rows
        FROM mart_letterboxd_movie_matches
        WHERE matched_tconst IS NULL
        GROUP BY film_name, film_year
        ORDER BY diary_rows DESC, film_name
        LIMIT {limit};
    """,
    "recent-matches": """
        SELECT
            activity_date,
            film_name,
            film_year,
            letterboxd_rating,
            matched_tconst,
            matched_primary_title,
            imdb_average_rating,
            match_confidence
        FROM mart_letterboxd_movie_matches
        ORDER BY activity_date DESC NULLS LAST, diary_id DESC
        LIMIT {limit};
    """,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ready-made analytical queries for Letterboxd movie matches mart.",
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
        help="Disable psql headers and aligned output for easier piping.",
    )
    return parser.parse_args()


def ensure_prerequisites(compose_file: Path, service: str) -> None:
    if shutil.which("docker") is None:
        print("Error: docker command not found in PATH.", file=sys.stderr)
        raise SystemExit(1)


def ensure_direct_prerequisites() -> None:
    if shutil.which("psql") is None:
        print("Error: psql command not found in PATH.", file=sys.stderr)
        raise SystemExit(1)


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

    result = subprocess.run(command, text=True, env=env, check=False)
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
        *psql_flags,
        "-c",
        sql,
    ]

    print(f"Running query '{args.query}' on {args.db} via service {args.service}...")
    print(f"SQL: {sql}\n")

    result = subprocess.run(command, text=True, check=False)
    return result.returncode

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
    if args.port <= 0:
        print("Error: --port must be > 0", file=sys.stderr)
        return 1

    sql = QUERY_SQL[args.query].format(limit=args.limit).strip()

    psql_flags = ["-P", "pager=off"]
    if args.no_header:
        psql_flags.extend(["-A", "-t"])

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
