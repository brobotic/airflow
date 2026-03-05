#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


COMMENT_RE = re.compile(r"^\s*--\s*(.*)$")


@dataclass
class QueryBlock:
    description: str
    sql: str


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    default_sql_file = script_dir / "validate_mart.sql"

    parser = argparse.ArgumentParser(
        description="Run mart validation queries against Postgres in Docker Compose.",
    )
    parser.add_argument(
        "--sql-file",
        default=str(default_sql_file),
        help="SQL file to execute (default: scripts/validate_mart.sql)",
    )
    parser.add_argument(
        "--service",
        default=os.getenv("POSTGRES_SERVICE", "postgres_movies"),
        help="Docker Compose service name (default: postgres_movies)",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("POSTGRES_USER", "movies_user"),
        help="Postgres user (default: movies_user)",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("POSTGRES_DB", "movies_db"),
        help="Postgres database (default: movies_db)",
    )
    return parser.parse_args()


def split_query_blocks(sql_text: str) -> list[QueryBlock]:
    lines = sql_text.splitlines()
    blocks: list[QueryBlock] = []

    current_description = "Untitled query"
    current_sql_lines: list[str] = []

    def flush() -> None:
        nonlocal current_sql_lines, current_description
        sql = "\n".join(current_sql_lines).strip()
        if sql:
            blocks.append(QueryBlock(description=current_description, sql=sql))
        current_sql_lines = []

    for line in lines:
        comment_match = COMMENT_RE.match(line)
        if comment_match:
            flush()
            description = comment_match.group(1).strip()
            if description:
                current_description = description
            continue

        current_sql_lines.append(line)

    flush()
    return blocks


def run_command(command: Iterable[str], *, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        text=True,
        capture_output=True,
        check=check,
    )


def ensure_prerequisites(compose_file: Path, service: str) -> None:
    if shutil.which("docker") is None:
        print("Error: docker command not found in PATH.", file=sys.stderr)
        raise SystemExit(1)

    if not compose_file.is_file():
        print(f"Error: compose file not found at {compose_file}", file=sys.stderr)
        raise SystemExit(1)

    ps_result = run_command(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "ps",
            "--status",
            "running",
            "--services",
        ]
    )
    if ps_result.returncode != 0:
        print(ps_result.stderr.strip(), file=sys.stderr)
        raise SystemExit(ps_result.returncode)

    running_services = {line.strip() for line in ps_result.stdout.splitlines() if line.strip()}
    if service not in running_services:
        print(
            f"Error: service '{service}' is not running. Start it with scripts/start_postgres_stack.sh",
            file=sys.stderr,
        )
        raise SystemExit(1)


def execute_query(
    *,
    compose_file: Path,
    service: str,
    user: str,
    db: str,
    block: QueryBlock,
) -> tuple[bool, str, str]:
    command = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        service,
        "psql",
        "-U",
        user,
        "-d",
        db,
        "-v",
        "ON_ERROR_STOP=1",
        "-P",
        "pager=off",
        "-c",
        block.sql,
    ]

    result = run_command(command)
    success = result.returncode == 0
    return success, result.stdout, result.stderr


def main() -> int:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    compose_file = project_root / "postgres.yaml"
    sql_file = Path(args.sql_file).expanduser().resolve()

    ensure_prerequisites(compose_file=compose_file, service=args.service)

    if not sql_file.is_file():
        print(f"Error: SQL file not found at {sql_file}", file=sys.stderr)
        return 1

    sql_text = sql_file.read_text(encoding="utf-8")
    blocks = split_query_blocks(sql_text)

    if not blocks:
        print(f"Error: no executable SQL blocks found in {sql_file}", file=sys.stderr)
        return 1

    print(f"Running mart validation queries in {args.db} via service {args.service}...")
    print(f"SQL file: {sql_file}")
    print()

    failed_blocks: list[str] = []

    for block in blocks:
        print(f"=== {block.description} ===")
        ok, stdout, stderr = execute_query(
            compose_file=compose_file,
            service=args.service,
            user=args.user,
            db=args.db,
            block=block,
        )

        if stdout.strip():
            print(stdout.rstrip())

        if ok:
            print(f"Status: OK — {block.description}")
        else:
            failed_blocks.append(block.description)
            print(f"Status: ERROR — {block.description}")
            error_output = (stderr or "").strip()
            if error_output:
                print(error_output, file=sys.stderr)

        print()

    print("Validation run completed.")
    print(f"Queries executed: {len(blocks)}")
    print(f"Queries failed:   {len(failed_blocks)}")

    if failed_blocks:
        print("Failed query blocks:")
        for description in failed_blocks:
            print(f"- {description}")
        return 1

    print("All query blocks succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
