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
