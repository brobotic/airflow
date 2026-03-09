import csv
import logging
import os
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow_datasets import LETTERBOXD_DIARY_DATASET
    from etl_helpers import (
        configure_csv_field_limit,
        normalize_text_or_none,
        to_float_or_none,
        to_int_or_none,
    )
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import LETTERBOXD_DIARY_DATASET
    from dags.etl_helpers import (
        configure_csv_field_limit,
        normalize_text_or_none,
        to_float_or_none,
        to_int_or_none,
    )
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

CSV_PATH = os.getenv(
    "LETTERBOXD_DIARY_CSV_PATH",
    "/opt/airflow/datasets/letterboxd/letterboxd-brobots-2026-03-09-00-07-utc/diary.csv",
)
CONN_ID = "postgres_movies"
TABLE = "letterboxd_diary"


def _normalize(value: str | None) -> str | None:
    return normalize_text_or_none(value)


def _to_date_or_none(value: str | None):
    normalized = _normalize(value)
    if normalized is None:
        return None
    return datetime.strptime(normalized, "%Y-%m-%d").date()


def _to_bool_rewatch(value: str | None):
    normalized = _normalize(value)
    if normalized is None:
        return False
    return normalized.lower() in {"yes", "true", "1", "y"}


def create_table():
    """Create the Letterboxd diary staging table if missing."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            diary_id         BIGSERIAL PRIMARY KEY,
            entry_date       DATE,
            film_name        TEXT,
            film_year        INTEGER,
            letterboxd_uri   TEXT,
            rating           DOUBLE PRECISION,
            rewatch          BOOLEAN,
            tags             TEXT,
            watched_date     DATE,
            loaded_at        TIMESTAMP DEFAULT now()
        );
        """
    )
    hook.run(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_entry_date ON {TABLE} (entry_date);")
    hook.run(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_watched_date ON {TABLE} (watched_date);")
    hook.run(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_film_year ON {TABLE} (film_year);")
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    """Load diary.csv into Postgres as a replace-style snapshot."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    configure_csv_field_limit()

    required_columns = {
        "Date",
        "Name",
        "Year",
        "Letterboxd URI",
        "Rating",
        "Rewatch",
        "Tags",
        "Watched Date",
    }

    insert_sql = f"""
        INSERT INTO {TABLE} (
            entry_date,
            film_name,
            film_year,
            letterboxd_uri,
            rating,
            rewatch,
            tags,
            watched_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    batch = []
    batch_size = 5000
    total_rows = 0

    cursor.execute(f"TRUNCATE TABLE {TABLE};")

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = required_columns - fieldnames
        if missing:
            raise ValueError(f"Missing expected columns: {sorted(missing)}. Got: {reader.fieldnames}")

        for row in reader:
            record = (
                _to_date_or_none(row.get("Date")),
                _normalize(row.get("Name")),
                to_int_or_none(row.get("Year")),
                _normalize(row.get("Letterboxd URI")),
                to_float_or_none(row.get("Rating")),
                _to_bool_rewatch(row.get("Rewatch")),
                _normalize(row.get("Tags")),
                _to_date_or_none(row.get("Watched Date")),
            )
            batch.append(record)

            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_rows += len(batch)
                logging.info("Inserted %d rows so far…", total_rows)
                batch.clear()

    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        total_rows += len(batch)

    cursor.close()
    conn.close()

    logging.info("✅ Letterboxd diary load complete — %d rows loaded.", total_rows)


def verify_load():
    """Log row count and sample records for sanity-checking."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    total = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"""
        SELECT entry_date, film_name, film_year, rating, rewatch, tags
        FROM {TABLE}
        ORDER BY entry_date DESC NULLS LAST
        LIMIT 5;
        """
    )

    logging.info("Total rows: %d", total)
    for row in sample:
        logging.info("  %s", row)

    return {
        "row_count": total,
        "sample_count": len(sample),
    }


with DAG(
    dag_id="letterboxd_diary_etl",
    description="ETL: Load Letterboxd diary.csv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ letterboxd_diary_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["etl", "letterboxd", "movies"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ letterboxd_diary_etl completed",
        extract_outlets=[LETTERBOXD_DIARY_DATASET],
    )
