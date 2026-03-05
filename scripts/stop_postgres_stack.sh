#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/postgres.yaml"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker command not found in PATH." >&2
  exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "Error: compose file not found at ${COMPOSE_FILE}" >&2
  exit 1
fi

echo "Stopping services from ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" down --remove-orphans

echo "Done. Data volume was preserved (no -v used)."