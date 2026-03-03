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

TSV_PATH = "/opt/airflow/datasets/title.crew.tsv"
CONN_ID = "postgres_movies"
TABLE = "title_crew"


def clean_value(value: str):
    return None if value == r"\N" else value


def create_table():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            tconst      VARCHAR(20) PRIMARY KEY,
            directors   TEXT,
            writers     TEXT
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE} (tconst, directors, writers)
        VALUES (%s, %s, %s)
        ON CONFLICT (tconst) DO UPDATE SET
          directors = EXCLUDED.directors,
          writers = EXCLUDED.writers;
    """

    batch = []
    batch_size = 50_000
    total = 0

    logging.info("Using crew dataset: %s", TSV_PATH)

    with open(TSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        required = {"tconst", "directors", "writers"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing expected columns: {missing}. Got: {reader.fieldnames}")

        for row in reader:
            tconst = clean_value(row["tconst"])
            directors = clean_value(row["directors"])
            writers = clean_value(row["writers"])

            batch.append((tconst, directors, writers))

            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                logging.info("Upserted %d rows so far…", total)
                batch.clear()

    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()
        total += len(batch)

    cur.close()
    conn.close()
    logging.info("✅ Crew ingest complete — %d total rows upserted.", total)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"SELECT tconst, directors, writers FROM {TABLE} LIMIT 5;"
    )
    logging.info("Row count: %d", count)
    for rec in sample:
        logging.info("  %s", rec)

    return {
        "row_count": count,
        "sample_count": len(sample),
    }


with DAG(
    dag_id="movies_crew_etl",
    description="ETL: Load title.crew.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_crew_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl", "crew"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_crew_etl completed",
    )
