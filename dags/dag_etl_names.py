import csv
import logging
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.etl_tasks import create_standard_etl_tasks
from dags.notifications import (
    notify_discord_failure,
)

TSV_PATH = "/opt/airflow/datasets/name.basics.tsv"
CONN_ID = "postgres_movies"
TABLE = "name_basics"


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
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            nconst              VARCHAR(20) PRIMARY KEY,
            primary_name        TEXT,
            birth_year          INTEGER,
            death_year          INTEGER,
            primary_profession  TEXT,
            known_for_titles    TEXT
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE} (
            nconst, primary_name, birth_year, death_year,
            primary_profession, known_for_titles
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (nconst) DO UPDATE SET
          primary_name = EXCLUDED.primary_name,
          birth_year = EXCLUDED.birth_year,
          death_year = EXCLUDED.death_year,
          primary_profession = EXCLUDED.primary_profession,
          known_for_titles = EXCLUDED.known_for_titles;
    """

    batch = []
    batch_size = 50_000
    total = 0

    logging.info("Using names dataset: %s", TSV_PATH)

    with open(TSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        required = {
            "nconst",
            "primaryName",
            "birthYear",
            "deathYear",
            "primaryProfession",
            "knownForTitles",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing expected columns: {missing}. Got: {reader.fieldnames}")

        for row in reader:
            nconst = clean_value(row["nconst"])
            primary_name = clean_value(row["primaryName"])
            birth_year = to_int_or_none(row["birthYear"])
            death_year = to_int_or_none(row["deathYear"])
            primary_profession = clean_value(row["primaryProfession"])
            known_for_titles = clean_value(row["knownForTitles"])

            batch.append(
                (
                    nconst,
                    primary_name,
                    birth_year,
                    death_year,
                    primary_profession,
                    known_for_titles,
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
    logging.info("✅ Names ingest complete — %d total rows upserted.", total)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"SELECT nconst, primary_name, birth_year, primary_profession "
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
    dag_id="movies_names_etl",
    description="ETL: Load name.basics.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_names_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl", "names"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_names_etl completed",
    )
