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

CONN_ID = "postgres_movies"
TABLE = "mart_titles_enriched"

# Depends on these source tables being loaded first
SOURCE_TABLES = ["title_basics", "title_ratings"]


def create_table():
    """Ensure required source tables are present and destination table can be created."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    for table in SOURCE_TABLES:
        result = hook.get_first(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_name = '{table}';"
        )
        if not result or result[0] == 0:
            raise ValueError(f"Source table '{table}' does not exist.")

        count = hook.get_first(f"SELECT COUNT(*) FROM {table};")[0]
        if count == 0:
            raise ValueError(f"Source table '{table}' is empty.")
        logging.info("Source table '%s' OK — %d rows.", table, count)

    hook.run(f"DROP TABLE IF EXISTS {TABLE};")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            tconst            VARCHAR(20) PRIMARY KEY,
            primary_title     TEXT,
            original_title    TEXT,
            title_type        VARCHAR(50),
            start_year        INTEGER,
            end_year          INTEGER,
            runtime_minutes   INTEGER,
            genres            TEXT,
            is_adult          BOOLEAN,
            average_rating    DOUBLE PRECISION,
            num_votes         INTEGER,
            rating_bucket     VARCHAR(20),
            era               VARCHAR(20),
            last_refreshed_at TIMESTAMP
        );
    """)
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    """Build mart_titles_enriched from source tables and add supporting indexes."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    logging.info("Refreshing '%s'...", TABLE)
    hook.run(f"TRUNCATE TABLE {TABLE};")
    hook.run(f"""
        INSERT INTO {TABLE} (
            tconst,
            primary_title,
            original_title,
            title_type,
            start_year,
            end_year,
            runtime_minutes,
            genres,
            is_adult,
            average_rating,
            num_votes,
            rating_bucket,
            era,
            last_refreshed_at
        )
        SELECT
            b.tconst,
            b.primary_title,
            b.original_title,
            b.title_type,
            b.start_year,
            b.end_year,
            b.runtime_minutes,
            b.genres,
            b.is_adult,
            r.average_rating,
            r.num_votes,
            CASE
                WHEN r.average_rating >= 8.0 THEN 'excellent'
                WHEN r.average_rating >= 6.0 THEN 'good'
                WHEN r.average_rating >= 4.0 THEN 'average'
                WHEN r.average_rating IS NOT NULL THEN 'poor'
                ELSE 'unrated'
            END AS rating_bucket,
            CASE
                WHEN b.start_year IS NULL THEN NULL
                WHEN b.start_year < 1950 THEN 'pre-1950'
                WHEN b.start_year < 1980 THEN '1950s-1970s'
                WHEN b.start_year < 2000 THEN '1980s-1990s'
                WHEN b.start_year < 2010 THEN '2000s'
                WHEN b.start_year < 2020 THEN '2010s'
                ELSE '2020s+'
            END AS era,
            now() AS last_refreshed_at
        FROM title_basics b
        LEFT JOIN title_ratings r ON b.tconst = r.tconst;
    """)

    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_title_type ON {TABLE} (title_type);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_start_year ON {TABLE} (start_year);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_rating_bucket ON {TABLE} (rating_bucket);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_avg_rating ON {TABLE} (average_rating DESC NULLS LAST);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_num_votes ON {TABLE} (num_votes DESC NULLS LAST);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_era ON {TABLE} (era);",
    ]

    for idx_sql in indexes:
        hook.run(idx_sql)
        logging.info("Ensured index exists: %s", idx_sql)

    logging.info("✅ Mart build complete.")


def verify_load():
    """Log row counts and sample queries against the mart."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    total = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    logging.info("Total rows: %d", total)

    rated = hook.get_first(
        f"SELECT COUNT(*) FROM {TABLE} WHERE average_rating IS NOT NULL;"
    )[0]
    logging.info("Rows with ratings: %d", rated)

    logging.info("--- Top 5 by votes ---")
    top = hook.get_records(
        f"SELECT primary_title, start_year, average_rating, num_votes, rating_bucket "
        f"FROM {TABLE} ORDER BY num_votes DESC NULLS LAST LIMIT 5;"
    )
    for row in top:
        logging.info("  %s", row)

    logging.info("--- Titles by era ---")
    by_era = hook.get_records(
        f"SELECT era, COUNT(*) AS cnt FROM {TABLE} "
        f"GROUP BY era ORDER BY era NULLS LAST;"
    )
    for row in by_era:
        logging.info("  %s", row)

    logging.info("--- Titles by rating bucket ---")
    by_bucket = hook.get_records(
        f"SELECT rating_bucket, COUNT(*) AS cnt FROM {TABLE} "
        f"GROUP BY rating_bucket ORDER BY cnt DESC;"
    )
    for row in by_bucket:
        logging.info("  %s", row)

    return {
        "row_count": total,
        "rated_count": rated,
        "sample_count": len(top),
    }


with DAG(
    dag_id="mart_titles_enriched",
    description="Build mart_titles_enriched from title_basics + title_ratings",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ mart_titles_enriched task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["movies", "mart"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_titles_enriched completed",
    )
