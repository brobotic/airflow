import csv
import logging
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow_datasets import TITLE_EPISODE_DATASET
    from etl_helpers import (
        configure_csv_field_limit,
        normalize_imdb_null,
        to_int_or_none,
    )
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import TITLE_EPISODE_DATASET
    from dags.etl_helpers import (
        configure_csv_field_limit,
        normalize_imdb_null,
        to_int_or_none,
    )
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

TSV_PATH = "/opt/airflow/datasets/title.episode.tsv"
CONN_ID = "postgres_movies"
TABLE = "title_episode"


def create_table():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            tconst          VARCHAR(20) PRIMARY KEY,
            parent_tconst   VARCHAR(20),
            season_number   INTEGER,
            episode_number  INTEGER
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    configure_csv_field_limit()

    insert_sql = f"""
        INSERT INTO {TABLE} (tconst, parent_tconst, season_number, episode_number)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (tconst) DO NOTHING;
    """

    batch = []
    batch_size = 50_000
    total = 0

    logging.info("Using episode dataset: %s", TSV_PATH)

    with open(TSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        required = {"tconst", "parentTconst", "seasonNumber", "episodeNumber"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing expected columns: {missing}. Got: {reader.fieldnames}")

        for row in reader:
            tconst = normalize_imdb_null(row["tconst"])
            parent_tconst = normalize_imdb_null(row["parentTconst"])
            season_number = to_int_or_none(row["seasonNumber"])
            episode_number = to_int_or_none(row["episodeNumber"])

            if tconst is None:
                continue

            batch.append((tconst, parent_tconst, season_number, episode_number))

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
    logging.info("✅ Episode ingest complete — %d total rows upserted.", total)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"SELECT tconst, parent_tconst, season_number, episode_number "
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
    dag_id="movies_episode_etl",
    description="ETL: Load title.episode.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_episode_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl", "episode"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_episode_etl completed",
        extract_outlets=[TITLE_EPISODE_DATASET],
    )
