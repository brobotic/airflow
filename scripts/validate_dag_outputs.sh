#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/postgres.yaml"

POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres_movies}"
POSTGRES_USER="${POSTGRES_USER:-movies_user}"
POSTGRES_DB="${POSTGRES_DB:-movies_db}"
DEFAULT_MIN_ROWS=1

declare -a DEFAULT_TABLES=(
  "title_basics"
  "name_basics"
  "title_crew"
  "title_principals"
  "title_ratings"
  "mart_titles_enriched"
  "mart_director_credits"
)

usage() {
  cat <<'EOF'
Validate that Airflow DAG output tables exist in Postgres and have expected row counts.

Usage:
  scripts/validate_dag_outputs.sh [options]

Options:
  --min-rows N           Global minimum rows for all default tables (default: 1)
  --table NAME:MIN       Add or override threshold for one table (repeatable)
  --service NAME         Docker Compose service name (default: postgres_movies)
  --user NAME            Postgres user (default: movies_user)
  --db NAME              Postgres database (default: movies_db)
  -h, --help             Show this help

Examples:
  scripts/validate_dag_outputs.sh
  scripts/validate_dag_outputs.sh --min-rows 100
  scripts/validate_dag_outputs.sh --table mart_titles_enriched:1000 --table title_ratings:5000
EOF
}

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found in PATH." >&2
  exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "Error: compose file not found at ${COMPOSE_FILE}" >&2
  exit 1
fi

declare -a TABLE_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --min-rows)
      if [[ $# -lt 2 ]]; then
        echo "Error: --min-rows requires a numeric value." >&2
        exit 1
      fi
      DEFAULT_MIN_ROWS="$2"
      shift 2
      ;;
    --table)
      if [[ $# -lt 2 ]]; then
        echo "Error: --table requires NAME:MIN format." >&2
        exit 1
      fi
      TABLE_ARGS+=("$2")
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

if ! [[ "${DEFAULT_MIN_ROWS}" =~ ^[0-9]+$ ]]; then
  echo "Error: --min-rows must be a non-negative integer." >&2
  exit 1
fi

declare -A TABLE_THRESHOLDS=()

for table in "${DEFAULT_TABLES[@]}"; do
  TABLE_THRESHOLDS["$table"]="${DEFAULT_MIN_ROWS}"
done

for table_arg in "${TABLE_ARGS[@]}"; do
  if [[ "${table_arg}" != *:* ]]; then
    echo "Error: --table must use NAME:MIN format. Got '${table_arg}'." >&2
    exit 1
  fi

  table_name="${table_arg%%:*}"
  min_rows="${table_arg##*:}"

  if ! [[ "${table_name}" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo "Error: invalid table name '${table_name}'." >&2
    exit 1
  fi

  if ! [[ "${min_rows}" =~ ^[0-9]+$ ]]; then
    echo "Error: invalid row threshold '${min_rows}' for table '${table_name}'." >&2
    exit 1
  fi

  TABLE_THRESHOLDS["${table_name}"]="${min_rows}"
done

if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -qx "${POSTGRES_SERVICE}"; then
  echo "Error: service '${POSTGRES_SERVICE}' is not running. Start it with scripts/start_postgres_stack.sh" >&2
  exit 1
fi

readarray -t SORTED_TABLES < <(printf '%s\n' "${!TABLE_THRESHOLDS[@]}" | sort)

echo "Validating tables in ${POSTGRES_DB} via service ${POSTGRES_SERVICE}..."
echo

failures=0

for table in "${SORTED_TABLES[@]}"; do
  min_required="${TABLE_THRESHOLDS[$table]}"

  sql="SELECT CASE WHEN to_regclass('public.${table}') IS NULL THEN 0 ELSE 1 END AS table_exists,"
  sql+=" CASE WHEN to_regclass('public.${table}') IS NULL THEN 0 ELSE (SELECT COUNT(*) FROM public.${table}) END AS row_count;"

  query_output="$({
    docker compose -f "${COMPOSE_FILE}" exec -T "${POSTGRES_SERVICE}" \
      psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -At -F ',' -c "${sql}"
  } 2>/dev/null || true)"

  exists_flag="${query_output%%,*}"
  row_count="${query_output##*,}"

  if [[ -z "${query_output}" || ! "${exists_flag}" =~ ^[0-9]+$ || ! "${row_count}" =~ ^[0-9]+$ ]]; then
    printf '❌ %-24s query failed\n' "${table}"
    failures=$((failures + 1))
    continue
  fi

  if [[ "${exists_flag}" -eq 0 ]]; then
    printf '❌ %-24s missing (min=%s)\n' "${table}" "${min_required}"
    failures=$((failures + 1))
    continue
  fi

  if [[ "${row_count}" -lt "${min_required}" ]]; then
    printf '❌ %-24s rows=%s (< min=%s)\n' "${table}" "${row_count}" "${min_required}"
    failures=$((failures + 1))
    continue
  fi

  printf '✅ %-24s rows=%s (min=%s)\n' "${table}" "${row_count}" "${min_required}"
done

echo
if [[ "${failures}" -gt 0 ]]; then
  echo "Validation failed: ${failures} check(s) did not meet expectations."
  exit 1
fi

echo "Validation passed: all table checks satisfied."
