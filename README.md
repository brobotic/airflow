# Airflow (local)

# TODO

* postgres exporter

## Authoring new ETL DAGs

Use the shared helpers for notifications and standard task wiring:

- Guide: [DAG_AUTHORING.md](DAG_AUTHORING.md)
- Shared notifications: `dags/notifications.py`
- Shared task-chain factory: `dags/etl_tasks.py`

# Testing DAGs

`docker compose exec airflow-scheduler airflow tasks test movies_ratings_etl extract_and_load 2026-03-02`

## Unit tests

Run the DAG unit tests from this folder:

`/home/micha/github/homelab/docker/airflow/venv/bin/python -m pytest tests/test_dag_etl_movies.py -q`

Expected result:

`10 passed`

## Postgres persistence

The Postgres service in `postgres.yaml` stores data in the named Docker volume `postgres_movies_data`, so data persists across container restarts and `docker compose stop/start`.

To keep data, use:

`docker compose -f postgres.yaml up -d`
`docker compose -f postgres.yaml restart postgres_movies`

Avoid using `docker compose -f postgres.yaml down -v` unless you intentionally want to delete the database volume.

## Validate DAG outputs in Postgres

Use the helper script to quickly verify expected DAG output tables exist and contain at least N rows:

`scripts/validate_dag_outputs.sh`

Examples:

- Default check (all known ETL/mart tables, min 1 row each):
    - `scripts/validate_dag_outputs.sh`
- Set one global threshold for all default tables:
    - `scripts/validate_dag_outputs.sh --min-rows 100`
- Override specific tables with custom thresholds:
    - `scripts/validate_dag_outputs.sh --table mart_titles_enriched:1000 --table title_ratings:5000`

Default checks include both mart tables plus the new per-movie credits mart:

- `mart_titles_enriched`
- `mart_director_credits`
- `mart_movie_credits`

Default checks also include IMDb staging tables:

- `title_basics`
- `title_akas`
- `name_basics`
- `title_crew`
- `title_episode`
- `title_principals`
- `title_ratings`

The script exits with code `0` when all checks pass, and non-zero when any table is missing or below threshold.

## Link movies to directors

Linking movies to directors:

- Movie IDs come from `title_basics.tconst` (`dags/dag_etl_movies.py`).
- Director IDs come from `title_crew.directors` (`dags/dag_etl_crew.py`) as comma-separated `nconst` values.
- Director names come from `name_basics.nconst -> primary_name` (`dags/dag_etl_names.py`).

Use this query to get movie/director pairs:

```sql
SELECT
    b.tconst,
    b.primary_title,
    d.director_nconst,
    n.primary_name AS director_name
FROM title_basics b
JOIN title_crew c ON c.tconst = b.tconst
CROSS JOIN LATERAL unnest(string_to_array(c.directors, ',')) AS d(director_nconst)
LEFT JOIN name_basics n ON n.nconst = trim(d.director_nconst)
WHERE b.title_type = 'movie';
```

Note: `dags/dag_mart_director_credits.py` builds a director-level aggregate mart, not a per-movie mapping table.

For a per-movie enrichment mart containing director and DoP fields, use `dags/dag_mart_movie_credits.py` (`mart_movie_credits`).

Run it from the host with `psql`:

```bash
PGPASSWORD=movies_pass psql -h localhost -p 5433 -U movies_user -d movies_db -c "
SELECT
    b.tconst,
    b.primary_title,
    d.director_nconst,
    n.primary_name AS director_name
FROM title_basics b
JOIN title_crew c ON c.tconst = b.tconst
CROSS JOIN LATERAL unnest(string_to_array(c.directors, ',')) AS d(director_nconst)
LEFT JOIN name_basics n ON n.nconst = trim(d.director_nconst)
WHERE b.title_type = 'movie'
LIMIT 20;
"
```


# DAG Authoring (ETL pattern)

Use this pattern for TSV/CSV ETL DAGs so notifications and task wiring stay consistent.

## Shared modules

- `dags/notifications.py`
  - `notify_discord_failure(...)`
  - `notify_discord_success(...)`
- `dags/etl_tasks.py`
  - `create_standard_etl_tasks(...)`

## Minimal template

```python
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl_tasks import create_standard_etl_tasks
from notifications import notify_discord_failure

TABLE = "your_table"


def create_table():
    hook = PostgresHook(postgres_conn_id="postgres_movies")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id TEXT PRIMARY KEY
        );
    """)


def extract_and_load():
    pass


def verify_load():
    return {
        "row_count": 0,
        "sample_count": 0,
    }


with DAG(
    dag_id="your_etl_dag",
    description="ETL: ...",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ your_etl_dag task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["etl"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ your_etl_dag completed",
    )
```

## Notes

- Keep `verify_load()` returning a dictionary with `row_count` and `sample_count`.
- `create_standard_etl_tasks(...)` assumes task IDs:
  - `create_table`
  - `extract_and_load`
  - `verify_load`
  - `notify_discord`
- If you need custom task IDs or extra steps, wire operators manually but still reuse `notifications.py`.

## Unit test scaffold

Follow the same pytest + monkeypatch style used in `tests/test_dag_etl_movies.py` and `tests/test_dag_etl_ratings.py`.

### 1) Verify task queries and return shape

```python
from dags import dag_your_etl as module


class FakeHookVerify:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.get_first_calls = []
        self.get_records_calls = []

    def get_first(self, sql):
        self.get_first_calls.append(sql)
        return (2,)

    def get_records(self, sql):
        self.get_records_calls.append(sql)
        return [("id1", "sample")]


def test_verify_load_uses_queries(monkeypatch):
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookVerify(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)

    result = module.verify_load()

    hook = created["hook"]
    assert hook.postgres_conn_id == module.CONN_ID
    assert len(hook.get_first_calls) == 1
    assert len(hook.get_records_calls) == 1
    assert isinstance(result, dict)
    assert "row_count" in result
    assert "sample_count" in result
```

### 2) Verify task chain wiring

```python
def test_dag_task_chain():
    dag = module.dag
    create_task = dag.get_task("create_table")
    extract_task = dag.get_task("extract_and_load")
    verify_task = dag.get_task("verify_load")
    notify_task = dag.get_task("notify_discord")

    assert extract_task.task_id in create_task.downstream_task_ids
    assert verify_task.task_id in extract_task.downstream_task_ids
    assert notify_task.task_id in verify_task.downstream_task_ids
```

### 3) Verify failure callback is configured

```python
def test_failure_callback_exists():
    dag = module.dag
    assert "on_failure_callback" in dag.default_args
    assert callable(dag.default_args["on_failure_callback"])
```

### 4) Run tests

From `docker/airflow`:

`/home/micha/github/homelab/docker/airflow/venv/bin/python -m pytest tests/test_dag_your_etl.py -q`

### Starter file

A copy-ready scaffold is available at:

`tests/test_dag_etl_template.py.example`

Use it by copying and renaming it, then updating the import:

`cp tests/test_dag_etl_template.py.example tests/test_dag_etl_your_dag.py`

## Elasticsearch export tuning (`mart_titles_enriched`)

The DAG `dags/dag_mart_titles_enriched.py` includes an `export_to_elasticsearch` task with env-driven tuning.

### Environment variables

- `ELASTICSEARCH_HOST` (default: `http://192.168.1.60:9200`)
- `ELASTICSEARCH_INDEX` (default: `mart_titles_enriched`)
- `ELASTICSEARCH_API_KEY` (optional)
- `ELASTICSEARCH_USERNAME` / `ELASTICSEARCH_PASSWORD` (optional)
- `ELASTICSEARCH_FETCH_SIZE` (default: `2000`) — Postgres fetch batch size (streaming cursor)
- `ELASTICSEARCH_CHUNK_SIZE` (default: `500`) — docs per bulk chunk
- `ELASTICSEARCH_MAX_CHUNK_BYTES` (default: `10485760`) — max bytes per bulk request (10MB)
- `ELASTICSEARCH_THREAD_COUNT` (default: `2`) — parallel bulk workers
- `ELASTICSEARCH_QUEUE_SIZE` (default: `2`) — in-memory work queue per bulk helper
- `ELASTICSEARCH_REQUEST_TIMEOUT` (default: `120`) — request timeout in seconds
- `ELASTICSEARCH_PROGRESS_EVERY` (default: `10000`) — log progress every N processed docs
- `ELASTICSEARCH_FAST_INDEX_MODE` (default: `false`) — temporarily set `refresh_interval=-1` and `number_of_replicas=0`, then restore

### Recommended profiles

Safe profile (low memory pressure / OOM-resistant):

- `ELASTICSEARCH_FETCH_SIZE=2000`
- `ELASTICSEARCH_CHUNK_SIZE=500`
- `ELASTICSEARCH_MAX_CHUNK_BYTES=10485760`
- `ELASTICSEARCH_THREAD_COUNT=2`
- `ELASTICSEARCH_QUEUE_SIZE=2`
- `ELASTICSEARCH_REQUEST_TIMEOUT=120`
- `ELASTICSEARCH_PROGRESS_EVERY=10000`
- `ELASTICSEARCH_FAST_INDEX_MODE=true`

Fast profile (increase throughput if cluster has headroom):

- `ELASTICSEARCH_FETCH_SIZE=5000`
- `ELASTICSEARCH_CHUNK_SIZE=2000`
- `ELASTICSEARCH_MAX_CHUNK_BYTES=20971520`
- `ELASTICSEARCH_THREAD_COUNT=4`
- `ELASTICSEARCH_QUEUE_SIZE=4`
- `ELASTICSEARCH_REQUEST_TIMEOUT=180`
- `ELASTICSEARCH_PROGRESS_EVERY=20000`
- `ELASTICSEARCH_FAST_INDEX_MODE=true`

If task memory spikes or worker restarts occur, lower `ELASTICSEARCH_CHUNK_SIZE`, `ELASTICSEARCH_THREAD_COUNT`, and `ELASTICSEARCH_QUEUE_SIZE` first.


# Queries

Use `scripts/validate_mart.sh` to execute the maintained validation SQL in
`scripts/validate_mart.sql`.

The SQL file now validates all mart DAG outputs:

- `mart_titles_enriched`
- `mart_movie_credits`
- `mart_director_credits`

It includes row-count/freshness checks plus per-mart distribution and top-record sanity queries.