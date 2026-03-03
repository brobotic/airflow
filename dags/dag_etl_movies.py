import csv
import logging
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

# ── Config ────────────────────────────────────────────────────────────────────
TSV_PATH = "/opt/airflow/datasets/title.basics.tsv"
CONN_ID  = "postgres_movies"
TABLE    = "title_basics"

# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_value(value: str):
    """Replace IMDb null sentinel with Python None."""
    return None if value == r"\N" else value

def to_int_or_none(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value == r"\N":
        return None
    return int(value) if value.isdigit() else None


# ── Task functions ────────────────────────────────────────────────────────────

def create_table():
    """Create the target table if it doesn't already exist."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            tconst          VARCHAR(20)  PRIMARY KEY,
            title_type      VARCHAR(50),
            primary_title   TEXT,
            original_title  TEXT,
            is_adult        BOOLEAN,
            start_year      INTEGER,
            end_year        INTEGER,
            runtime_minutes INTEGER,
            genres          TEXT
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    """Read the TSV, clean each row, and bulk-insert into Postgres."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE} (
            tconst, title_type, primary_title, original_title,
            is_adult, start_year, end_year, runtime_minutes, genres
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tconst) DO NOTHING;
    """

    batch = []
    batch_size = 10_000
    total_rows = 0

    with open(TSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        for row in reader:
            # ── Transform ────────────────────────────────────────────────────
            tconst          = clean_value(row["tconst"])
            title_type      = clean_value(row["titleType"])
            primary_title   = clean_value(row["primaryTitle"])
            original_title  = clean_value(row["originalTitle"])
            start_year      = to_int_or_none(clean_value(row["startYear"]))
            end_year        = to_int_or_none(clean_value(row["endYear"]))
            runtime_minutes = to_int_or_none(clean_value(row["runtimeMinutes"]))
            genres          = clean_value(row["genres"])

            # Cast is_adult to bool; treat \N as None
            raw_adult = clean_value(row["isAdult"])
            is_adult  = bool(int(raw_adult)) if raw_adult is not None else None

            # Cast numeric fields
            start_year      = int(start_year)      if start_year      else None
            end_year        = int(end_year)        if end_year        else None
            runtime_minutes = int(runtime_minutes) if runtime_minutes else None

            batch.append((
                tconst, title_type, primary_title, original_title,
                is_adult, start_year, end_year, runtime_minutes, genres,
            ))

            # ── Load in batches ───────────────────────────────────────────────
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_rows += len(batch)
                logging.info("Inserted %d rows so far…", total_rows)
                batch.clear()

    # Insert any remaining rows
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        total_rows += len(batch)

    cursor.close()
    conn.close()
    logging.info("✅ ETL complete — %d total rows loaded.", total_rows)


def verify_load():
    """Log row count and a few sample records for quick sanity-check."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"SELECT tconst, primary_title, start_year, genres "
        f"FROM {TABLE} LIMIT 5;"
    )
    logging.info("Row count: %d", count)
    for rec in sample:
        logging.info("  %s", rec)

    return {
        "row_count": count,
        "sample_count": len(sample),
    }


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="movies_etl",
    description="ETL: Load title.basics.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_etl completed",
    )

