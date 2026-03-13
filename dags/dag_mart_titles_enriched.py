import logging
import os
import time
from importlib import import_module
from datetime import datetime
from functools import partial
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow_datasets import TITLE_BASICS_DATASET, TITLE_RATINGS_DATASET
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import TITLE_BASICS_DATASET, TITLE_RATINGS_DATASET
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

CONN_ID = "postgres_movies"
TABLE = "mart_titles_enriched"

# Depends on these source tables being loaded first
SOURCE_TABLES = ["title_basics", "title_ratings"]
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "mart_titles_enriched")
ES_CHUNK_SIZE = int(os.getenv("ELASTICSEARCH_CHUNK_SIZE", "500"))
ES_MAX_CHUNK_BYTES = int(os.getenv("ELASTICSEARCH_MAX_CHUNK_BYTES", str(10 * 1024 * 1024)))
ES_THREAD_COUNT = int(os.getenv("ELASTICSEARCH_THREAD_COUNT", "2"))
ES_QUEUE_SIZE = int(os.getenv("ELASTICSEARCH_QUEUE_SIZE", "2"))
ES_REQUEST_TIMEOUT = int(os.getenv("ELASTICSEARCH_REQUEST_TIMEOUT", "120"))
ES_FETCH_SIZE = int(os.getenv("ELASTICSEARCH_FETCH_SIZE", "2000"))
ES_PROGRESS_EVERY = int(os.getenv("ELASTICSEARCH_PROGRESS_EVERY", "10000"))
ES_FAST_INDEX_MODE = os.getenv("ELASTICSEARCH_FAST_INDEX_MODE", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _create_elasticsearch_client() -> Any:
    host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch-prod.home.lab:9200")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    password = os.getenv("ELASTICSEARCH_PASSWORD")

    Elasticsearch, _ = _get_elasticsearch_modules()
    if api_key:
        return Elasticsearch(hosts=[host], api_key=api_key)
    if username and password:
        return Elasticsearch(hosts=[host], basic_auth=(username, password))
    return Elasticsearch(hosts=[host])


def _get_elasticsearch_modules() -> tuple[Any, Any]:
    try:
        elasticsearch_module = import_module("elasticsearch")
        helpers_module = import_module("elasticsearch.helpers")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Elasticsearch client package not installed. Add 'elasticsearch>=8,<9' to requirements and rebuild Airflow image."
        ) from exc

    return elasticsearch_module.Elasticsearch, helpers_module


def _format_duration(seconds: float) -> str:
    total_seconds = max(int(seconds), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


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
        WITH basics_dedup AS (
            SELECT DISTINCT ON (tconst)
                tconst,
                primary_title,
                original_title,
                title_type,
                start_year,
                end_year,
                runtime_minutes,
                genres,
                is_adult
            FROM title_basics
            ORDER BY tconst, start_year DESC NULLS LAST
        ),
        ratings_dedup AS (
            SELECT DISTINCT ON (tconst)
                tconst,
                average_rating,
                num_votes
            FROM title_ratings
            ORDER BY tconst, num_votes DESC NULLS LAST, average_rating DESC NULLS LAST
        )
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
        FROM basics_dedup b
        LEFT JOIN ratings_dedup r ON b.tconst = r.tconst;
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


def export_to_elasticsearch():
    """Export mart_titles_enriched rows from Postgres into Elasticsearch."""
    logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)

    es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch-prod.home.lab:9200")
    auth_mode = "api_key" if os.getenv("ELASTICSEARCH_API_KEY") else (
        "basic_auth"
        if os.getenv("ELASTICSEARCH_USERNAME") and os.getenv("ELASTICSEARCH_PASSWORD")
        else "none"
    )
    logging.info(
        "Elasticsearch export settings: host=%s index=%s chunk_size=%d max_chunk_bytes=%d thread_count=%d queue_size=%d fetch_size=%d request_timeout=%ds progress_every=%d fast_index_mode=%s auth=%s",
        es_host,
        ES_INDEX,
        ES_CHUNK_SIZE,
        ES_MAX_CHUNK_BYTES,
        ES_THREAD_COUNT,
        ES_QUEUE_SIZE,
        ES_FETCH_SIZE,
        ES_REQUEST_TIMEOUT,
        ES_PROGRESS_EVERY,
        ES_FAST_INDEX_MODE,
        auth_mode,
    )

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    total_rows = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    if total_rows == 0:
        logging.warning("No rows in '%s'; skipping Elasticsearch export.", TABLE)
        return {"indexed": 0, "errors": 0, "index": ES_INDEX}

    logging.info(
        "Preparing to export %d rows from '%s' to index '%s' (chunk=%d, threads=%d, queue=%d, fetch=%d).",
        total_rows,
        TABLE,
        ES_INDEX,
        ES_CHUNK_SIZE,
        ES_THREAD_COUNT,
        ES_QUEUE_SIZE,
        ES_FETCH_SIZE,
    )

    _, es_helpers = _get_elasticsearch_modules()
    client = _create_elasticsearch_client()

    if client.indices.exists(index=ES_INDEX):
        client.indices.delete(index=ES_INDEX)

    client.indices.create(index=ES_INDEX)

    original_refresh_interval = None
    original_replicas = None

    if ES_FAST_INDEX_MODE:
        settings = client.indices.get_settings(index=ES_INDEX)
        index_settings = settings.get(ES_INDEX, {}).get("settings", {}).get("index", {})
        original_refresh_interval = index_settings.get("refresh_interval")
        original_replicas = index_settings.get("number_of_replicas")

        client.indices.put_settings(
            index=ES_INDEX,
            settings={
                "refresh_interval": "-1",
                "number_of_replicas": 0,
            },
        )
        logging.info(
            "Fast index mode enabled for '%s' (refresh_interval=-1, number_of_replicas=0).",
            ES_INDEX,
        )

    def actions():
        conn = hook.get_conn()
        cursor = conn.cursor(name="mart_titles_enriched_export")
        cursor.itersize = ES_FETCH_SIZE
        try:
            cursor.execute(
                f"""
                SELECT
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
                FROM {TABLE};
                """
            )

            while True:
                batch = cursor.fetchmany(ES_FETCH_SIZE)
                if not batch:
                    break

                for row in batch:
                    (
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
                        last_refreshed_at,
                    ) = row

                    yield {
                        "_index": ES_INDEX,
                        "_id": tconst,
                        "_source": {
                            "tconst": tconst,
                            "primary_title": primary_title,
                            "original_title": original_title,
                            "title_type": title_type,
                            "start_year": start_year,
                            "end_year": end_year,
                            "runtime_minutes": runtime_minutes,
                            "genres": genres,
                            "is_adult": is_adult,
                            "average_rating": average_rating,
                            "num_votes": num_votes,
                            "rating_bucket": rating_bucket,
                            "era": era,
                            "last_refreshed_at": (
                                last_refreshed_at.isoformat() if last_refreshed_at else None
                            ),
                        },
                    }
        finally:
            cursor.close()
            conn.close()

    indexed = 0
    errors = 0
    processed = 0
    started_at = time.monotonic()
    last_log_at = started_at

    try:
        for ok, _ in es_helpers.parallel_bulk(
            client,
            actions(),
            thread_count=ES_THREAD_COUNT,
            queue_size=ES_QUEUE_SIZE,
            chunk_size=ES_CHUNK_SIZE,
            max_chunk_bytes=ES_MAX_CHUNK_BYTES,
            request_timeout=ES_REQUEST_TIMEOUT,
            raise_on_error=False,
            raise_on_exception=False,
        ):
            processed += 1
            if ok:
                indexed += 1
            else:
                errors += 1

            if ES_PROGRESS_EVERY > 0 and processed % ES_PROGRESS_EVERY == 0:
                now = time.monotonic()
                elapsed = now - started_at
                interval = now - last_log_at
                total_rate = processed / elapsed if elapsed > 0 else 0.0
                interval_rate = ES_PROGRESS_EVERY / interval if interval > 0 else 0.0
                remaining_docs = max(total_rows - processed, 0)
                eta_seconds = (remaining_docs / total_rate) if total_rate > 0 else None
                eta_text = _format_duration(eta_seconds) if eta_seconds is not None else "n/a"
                logging.info(
                    "Elasticsearch export progress: processed=%d/%d indexed=%d errors=%d total_rate=%.1f docs/s interval_rate=%.1f docs/s eta=%s",
                    processed,
                    total_rows,
                    indexed,
                    errors,
                    total_rate,
                    interval_rate,
                    eta_text,
                )
                last_log_at = now

        if errors:
            raise ValueError(
                f"Elasticsearch bulk export completed with {errors} errors."
            )

        client.indices.refresh(index=ES_INDEX)
        completed_at = time.monotonic()
        total_elapsed = completed_at - started_at
        total_elapsed_text = _format_duration(total_elapsed)
        avg_rate = indexed / total_elapsed if total_elapsed > 0 else 0.0
        logging.info(
            "Export complete: indexed=%d errors=%d total=%d elapsed=%s avg_rate=%.1f docs/s index='%s'.",
            indexed,
            errors,
            processed,
            total_elapsed_text,
            avg_rate,
            ES_INDEX,
        )
        return {"indexed": indexed, "errors": errors, "index": ES_INDEX}
    finally:
        if ES_FAST_INDEX_MODE:
            restore_settings = {}
            if original_refresh_interval is not None:
                restore_settings["refresh_interval"] = original_refresh_interval
            if original_replicas is not None:
                restore_settings["number_of_replicas"] = original_replicas

            if restore_settings:
                client.indices.put_settings(index=ES_INDEX, settings=restore_settings)
                logging.info(
                    "Restored index settings for '%s': %s",
                    ES_INDEX,
                    restore_settings,
                )


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
    schedule=[TITLE_BASICS_DATASET, TITLE_RATINGS_DATASET],
    catchup=False,
    tags=["movies", "mart"],
) as dag:
    _, _, verify_load_task, notify_task = create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_titles_enriched completed",
    )

    export_to_elasticsearch_task = PythonOperator(
        task_id="export_to_elasticsearch",
        python_callable=export_to_elasticsearch,
    )

    verify_load_task.set_downstream(export_to_elasticsearch_task)
    export_to_elasticsearch_task.set_downstream(notify_task)
