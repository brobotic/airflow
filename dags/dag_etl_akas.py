import csv
import logging
import sys
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow_datasets import TITLE_AKAS_DATASET
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import TITLE_AKAS_DATASET
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

TSV_PATH = "/opt/airflow/datasets/title.akas.tsv"
CONN_ID = "postgres_movies"
TABLE = "title_akas"


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


def to_bool_or_none(value):
    value = clean_value(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    if value in {"0", "1"}:
        return bool(int(value))
    return None


def configure_csv_field_limit():
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            logging.info("Configured CSV field size limit to %d", limit)
            return
        except OverflowError:
            limit = int(limit / 10)


def create_table():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            title_id            VARCHAR(20) NOT NULL,
            ordering            INTEGER NOT NULL,
            title               TEXT,
            region              VARCHAR(10),
            language            VARCHAR(10),
            types               TEXT,
            attributes          TEXT,
            is_original_title   BOOLEAN,
            PRIMARY KEY (title_id, ordering)
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    configure_csv_field_limit()

    insert_sql = f"""
        INSERT INTO {TABLE} (
            title_id, ordering, title, region, language,
            types, attributes, is_original_title
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (title_id, ordering) DO NOTHING;
    """

    batch = []
    batch_size = 50_000
    total = 0

    logging.info("Using akas dataset: %s", TSV_PATH)

    with open(TSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        required = {
            "titleId",
            "ordering",
            "title",
            "region",
            "language",
            "types",
            "attributes",
            "isOriginalTitle",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing expected columns: {missing}. Got: {reader.fieldnames}")

        for row in reader:
            title_id = clean_value(row["titleId"])
            ordering = to_int_or_none(row["ordering"])
            title = clean_value(row["title"])
            region = clean_value(row["region"])
            language = clean_value(row["language"])
            types = clean_value(row["types"])
            attributes = clean_value(row["attributes"])
            is_original_title = to_bool_or_none(row["isOriginalTitle"])

            if title_id is None or ordering is None:
                continue

            batch.append(
                (
                    title_id,
                    ordering,
                    title,
                    region,
                    language,
                    types,
                    attributes,
                    is_original_title,
                )
            )

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
    logging.info("✅ Akas ingest complete — %d total rows upserted.", total)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"SELECT title_id, ordering, title, region, language "
        f"FROM {TABLE} LIMIT 5;"
    )
    logging.info("Row count: %d", count)
    for rec in sample:
        logging.info("  %s", rec)

    return {
        "row_count": count,
        "sample_count": len(sample),
    }


with DAG(
    dag_id="movies_akas_etl",
    description="ETL: Load title.akas.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_akas_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl", "akas"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_akas_etl completed",
        extract_outlets=[TITLE_AKAS_DATASET],
    )
