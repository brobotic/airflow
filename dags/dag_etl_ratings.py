import csv
import logging
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow_datasets import TITLE_RATINGS_DATASET
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import TITLE_RATINGS_DATASET
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

TSV_PATH = "/opt/airflow/datasets/title.ratings.tsv"
CONN_ID = "postgres_movies"
TABLE = "title_ratings"


def clean_value(value: str):
    return None if value == r"\N" else value


def to_int_or_none(value):
    value = clean_value(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    # IMDb ratings file should be numeric; be defensive anyway
    return int(value) if value.isdigit() else None


def to_float_or_none(value):
    value = clean_value(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None



def create_table():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            tconst          VARCHAR(20) PRIMARY KEY,
            average_rating  DOUBLE PRECISION,
            num_votes       INTEGER
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE} (tconst, average_rating, num_votes)
        VALUES (%s, %s, %s)
        ON CONFLICT (tconst) DO NOTHING;
    """

    batch = []
    batch_size = 50_000
    total = 0

    logging.info("Using ratings dataset: %s", TSV_PATH)

    with open(TSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        logging.info("Detected headers: %s", reader.fieldnames)

        required = {"tconst", "averageRating", "numVotes"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing expected columns: {missing}. Got: {reader.fieldnames}")

        for row in reader:
            tconst = clean_value(row["tconst"])
            avg = to_float_or_none(row["averageRating"])
            votes = to_int_or_none(row["numVotes"])

            # Basic sanity checks
            if avg is not None and not (0.0 <= avg <= 10.0):
                avg = None

            batch.append((tconst, avg, votes))

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
    logging.info("✅ Ratings ingest complete — %d total rows upserted.", total)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    sample = hook.get_records(
        f"SELECT tconst, average_rating, num_votes FROM {TABLE} "
        f"ORDER BY num_votes DESC NULLS LAST LIMIT 5;"
    )
    logging.info("Row count: %d", count)
    for rec in sample:
        logging.info("  %s", rec)

    return {
        "row_count": count,
        "sample_count": len(sample),
    }


with DAG(
    dag_id="movies_ratings_etl",
    description="ETL: Load title.ratings.tsv into PostgreSQL",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ movies_ratings_etl task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["movies", "etl", "ratings"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ movies_ratings_etl completed",
        extract_outlets=[TITLE_RATINGS_DATASET],
    )

