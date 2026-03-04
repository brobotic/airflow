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

TSV_PATH = "/opt/airflow/datasets/title.principals.tsv"
CONN_ID = "postgres_movies"
TABLE = "title_principals"


def clean_value(value: str):
    return None if value == r"\N" else value


def to_int_or_none(value):
    value = clean_value(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    return int(value) if value.isdigit() else None


def create_table():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            tconst      VARCHAR(20) NOT NULL,
            ordering    INTEGER NOT NULL,
            nconst      VARCHAR(20),
            category    TEXT,
            job         TEXT,
            characters  TEXT,
            PRIMARY KEY (tconst, ordering)
        );
    """
    )
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE} (tconst, ordering, nconst, category, job, characters)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tconst, ordering) DO UPDATE SET
          nconst = EXCLUDED.nconst,
          category = EXCLUDED.category,
          job = EXCLUDED.job,
          characters = EXCLUDED.characters;
    """

    batch = []
    batch_size = 50_000
    total = 0

    logging.info("Using principals dataset: %s", TSV_PATH)

    with open(TSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        required = {"tconst", "ordering", "nconst", "category", "job", "characters"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing expected columns: {missing}. Got: {reader.fieldnames}")

        for row in reader:
            tconst = clean_value(row["tconst"])
            ordering = to_int_or_none(row["ordering"])
            nconst = clean_value(row["nconst"])
            category = clean_value(row["category"])
            job = clean_value(row["job"])
            characters = clean_value(row["characters"])

            if tconst is None or ordering is None:
                continue

            batch.append((tconst, ordering, nconst, category, job, characters))

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
    logging.info("✅ Principals ingest complete — %d total rows upserted.", total)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    cinematographer_count = hook.get_first(
        f"SELECT COUNT(*) FROM {TABLE} WHERE category = 'cinematographer' OR (job IS NOT NULL AND lower(job) LIKE '%director of photography%');"
    )[0]
    sample = hook.get_records(
        f"SELECT tconst, ordering, nconst, category, job FROM {TABLE} LIMIT 5;"
    )

    logging.info("Row count: %d", count)
    logging.info("Rows with cinematographer/DoP signals: %d", cinematographer_count)
    for rec in sample:
        logging.info("  %s", rec)

    return {
        "row_count": count,
        "sample_count": len(sample),
        "dop_signal_count": cinematographer_count,
    }


with DAG(
    dag_id="movies_principals_etl",
    description="ETL: Load title.principals.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_principals_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl", "principals"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_principals_etl completed",
    )
