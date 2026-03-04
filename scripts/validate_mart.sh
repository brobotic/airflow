#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/postgres.yaml"

POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres_movies}"
POSTGRES_USER="${POSTGRES_USER:-movies_user}"
POSTGRES_DB="${POSTGRES_DB:-movies_db}"
SQL_FILE="${SCRIPT_DIR}/validate_mart.sql"

usage() {
  cat <<'EOF'
Run mart validation queries against Postgres in Docker Compose.

Usage:
  scripts/validate_mart.sh [options]

Options:
  --sql-file PATH        SQL file to execute (default: scripts/validate_mart.sql)
  --service NAME         Docker Compose service name (default: postgres_movies)
  --user NAME            Postgres user (default: movies_user)
  --db NAME              Postgres database (default: movies_db)
  -h, --help             Show this help

Examples:
  scripts/validate_mart.sh
  scripts/validate_mart.sh --service postgres_movies --db movies_db
  scripts/validate_mart.sh --sql-file scripts/validate_mart.sql
EOF
}

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found in PATH." >&2
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql-file)
      if [[ $# -lt 2 ]]; then
        echo "Error: --sql-file requires a value." >&2
        exit 1
      fi
      SQL_FILE="$2"
      shift 2
      ;;
    --service)
      if [[ $# -lt 2 ]]; then
        echo "Error: --service requires a value." >&2
        exit 1
      fi
      POSTGRES_SERVICE="$2"
      shift 2
      ;;
    --user)
      if [[ $# -lt 2 ]]; then
        echo "Error: --user requires a value." >&2
        exit 1
      fi
      POSTGRES_USER="$2"
      shift 2
      ;;
    --db)
      if [[ $# -lt 2 ]]; then
        echo "Error: --db requires a value." >&2
        exit 1
      fi
      POSTGRES_DB="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "Error: compose file not found at ${COMPOSE_FILE}" >&2
  exit 1
fi

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "Error: SQL file not found at ${SQL_FILE}" >&2
  exit 1
fi

TEMP_SQL="$(mktemp)"
cleanup() {
  rm -f "${TEMP_SQL}"
}
trap cleanup EXIT

awk '
  /^[[:space:]]*--/ {
    description = $0
    sub(/^[[:space:]]*--[[:space:]]*/, "", description)
    if (length(description) > 0) {
      print "\\echo"
      print "\\echo === " description " ==="
    }
  }
  { print }
' "${SQL_FILE}" > "${TEMP_SQL}"

if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -qx "${POSTGRES_SERVICE}"; then
  echo "Error: service '${POSTGRES_SERVICE}' is not running. Start it with scripts/start_postgres_stack.sh" >&2
  exit 1
fi

echo "Running mart validation queries in ${POSTGRES_DB} via service ${POSTGRES_SERVICE}..."
echo "SQL file: ${SQL_FILE}"
echo

if ! docker compose -f "${COMPOSE_FILE}" exec -T "${POSTGRES_SERVICE}" \
  psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1 -f - < "${TEMP_SQL}"; then
  echo
  echo "Validation queries failed." >&2
  exit 1
fi

echo
echo "Validation queries completed successfully."