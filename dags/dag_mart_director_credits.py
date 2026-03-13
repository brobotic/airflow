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
    from airflow_datasets import (
        NAME_BASICS_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_CREW_DATASET,
        TITLE_PRINCIPALS_DATASET,
        TITLE_RATINGS_DATASET,
    )
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import (
        NAME_BASICS_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_CREW_DATASET,
        TITLE_PRINCIPALS_DATASET,
        TITLE_RATINGS_DATASET,
    )
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

CONN_ID = "postgres_movies"
TABLE = "mart_director_credits"
REQUIRED_SOURCE_TABLES = ["title_basics", "title_crew", "name_basics", "title_principals"]
OPTIONAL_SOURCE_TABLES = ["title_ratings"]
FETCH_SIZE = int(os.getenv("MART_DIRECTOR_FETCH_SIZE", "2000"))
INSERT_BATCH_SIZE = int(os.getenv("MART_DIRECTOR_INSERT_BATCH_SIZE", "1000"))
PROGRESS_EVERY = int(os.getenv("MART_DIRECTOR_PROGRESS_EVERY", "10000"))
ES_INDEX = os.getenv("ELASTICSEARCH_DIRECTOR_INDEX", "mart_director_credits")
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


def _get_elasticsearch_modules() -> tuple[Any, Any]:
    try:
        elasticsearch_module = import_module("elasticsearch")
        helpers_module = import_module("elasticsearch.helpers")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Elasticsearch client package not installed. Add 'elasticsearch>=8,<9' to requirements and rebuild Airflow image."
        ) from exc

    return elasticsearch_module.Elasticsearch, helpers_module


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


def _format_duration(seconds: float) -> str:
    total_seconds = max(int(seconds), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _table_exists(hook: PostgresHook, table_name: str) -> bool:
    result = hook.get_first(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s;",
        parameters=(table_name,),
    )
    return bool(result and result[0] > 0)


def _assert_table_has_data(hook: PostgresHook, table_name: str):
    if not _table_exists(hook, table_name):
        raise ValueError(f"Source table '{table_name}' does not exist.")

    count = hook.get_first(f"SELECT COUNT(*) FROM {table_name};")[0]
    if count == 0:
        raise ValueError(f"Source table '{table_name}' is empty.")
    logging.info("Source table '%s' OK — %d rows.", table_name, count)


def create_table():
    """Ensure source tables exist and destination table is ready."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    for table_name in REQUIRED_SOURCE_TABLES:
        _assert_table_has_data(hook, table_name)

    for table_name in OPTIONAL_SOURCE_TABLES:
        if _table_exists(hook, table_name):
            logging.info("Optional source table '%s' is available.", table_name)
        else:
            logging.warning(
                "Optional source table '%s' is missing. Ratings aggregates will be NULL/0.",
                table_name,
            )

    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            director_nconst     VARCHAR(20),
            director_name       TEXT,
            dop_nconst          VARCHAR(20),
            dop_name            TEXT,
            movie_count         INTEGER,
            avg_rating          DOUBLE PRECISION,
            total_votes         BIGINT,
            first_movie_year    INTEGER,
            last_movie_year     INTEGER,
            last_refreshed_at   TIMESTAMP
        );
    """
    )
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    """Build a director mart enriched with DoP relationships from principals."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    has_ratings = _table_exists(hook, "title_ratings")

    hook.run(f"TRUNCATE TABLE {TABLE};")

    ratings_join = (
        "LEFT JOIN title_ratings r ON d.tconst = r.tconst"
        if has_ratings
        else "LEFT JOIN (SELECT NULL::VARCHAR(20) AS tconst, NULL::DOUBLE PRECISION AS average_rating, NULL::INTEGER AS num_votes) r ON false"
    )

    select_sql = f"""
        WITH directors AS (
            SELECT
                b.tconst,
                b.start_year,
                trim(director_nconst) AS director_nconst
            FROM title_basics b
            JOIN title_crew c ON c.tconst = b.tconst
            CROSS JOIN unnest(string_to_array(c.directors, ',')) AS director_nconst
            WHERE b.title_type = 'movie'
              AND c.directors IS NOT NULL
              AND c.directors <> ''
        ),
        dop_credits AS (
            SELECT DISTINCT
                p.tconst,
                p.nconst AS dop_nconst
            FROM title_principals p
            WHERE p.category IN ('cinematographer', 'director of photography', 'director_of_photography')
               OR (p.job IS NOT NULL AND lower(p.job) LIKE '%director of photography%')
        )
        SELECT
            d.director_nconst,
            dn.primary_name AS director_name,
            dc.dop_nconst,
            dpn.primary_name AS dop_name,
            COUNT(DISTINCT d.tconst) AS movie_count,
            AVG(r.average_rating) AS avg_rating,
            COALESCE(SUM(r.num_votes), 0) AS total_votes,
            MIN(d.start_year) AS first_movie_year,
            MAX(d.start_year) AS last_movie_year,
            now() AS last_refreshed_at
        FROM directors d
        LEFT JOIN dop_credits dc ON dc.tconst = d.tconst
        LEFT JOIN name_basics dn ON dn.nconst = d.director_nconst
        LEFT JOIN name_basics dpn ON dpn.nconst = dc.dop_nconst
        {ratings_join}
        GROUP BY d.director_nconst, dn.primary_name, dc.dop_nconst, dpn.primary_name
        ORDER BY d.director_nconst, dc.dop_nconst;
    """
    logging.info(
        "Building '%s' with batched load (fetch_size=%d insert_batch_size=%d progress_every=%d).",
        TABLE,
        FETCH_SIZE,
        INSERT_BATCH_SIZE,
        PROGRESS_EVERY,
    )

    conn = hook.get_conn()
    cursor = conn.cursor(name="mart_director_credits_export")
    cursor.itersize = FETCH_SIZE

    inserted = 0
    started_at = time.monotonic()
    last_log_at = started_at

    try:
        cursor.execute(select_sql)
        while True:
            batch = cursor.fetchmany(FETCH_SIZE)
            if not batch:
                break

            hook.insert_rows(
                table=TABLE,
                rows=batch,
                target_fields=[
                    "director_nconst",
                    "director_name",
                    "dop_nconst",
                    "dop_name",
                    "movie_count",
                    "avg_rating",
                    "total_votes",
                    "first_movie_year",
                    "last_movie_year",
                    "last_refreshed_at",
                ],
                commit_every=INSERT_BATCH_SIZE,
            )

            inserted += len(batch)
            if PROGRESS_EVERY > 0 and inserted % PROGRESS_EVERY == 0:
                now = time.monotonic()
                elapsed = now - started_at
                interval = now - last_log_at
                total_rate = inserted / elapsed if elapsed > 0 else 0.0
                interval_rate = PROGRESS_EVERY / interval if interval > 0 else 0.0
                logging.info(
                    "Director mart load progress: inserted=%d total_rate=%.1f rows/s interval_rate=%.1f rows/s",
                    inserted,
                    total_rate,
                    interval_rate,
                )
                last_log_at = now
    finally:
        cursor.close()
        conn.close()

    elapsed = time.monotonic() - started_at
    avg_rate = inserted / elapsed if elapsed > 0 else 0.0
    logging.info(
        "Director mart data load complete: inserted=%d elapsed=%.1fs avg_rate=%.1f rows/s.",
        inserted,
        elapsed,
        avg_rate,
    )

    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_director_nconst ON {TABLE} (director_nconst);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_dop_nconst ON {TABLE} (dop_nconst);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_movie_count ON {TABLE} (movie_count DESC);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_total_votes ON {TABLE} (total_votes DESC);",
    ]

    for idx_sql in indexes:
        hook.run(idx_sql)

    logging.info("✅ Mart build complete for '%s'.", TABLE)


def verify_load():
    """Log row counts and top relationships in the mart."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    total = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    with_dop = hook.get_first(f"SELECT COUNT(*) FROM {TABLE} WHERE dop_nconst IS NOT NULL;")[0]

    logging.info("Total rows: %d", total)
    logging.info("Rows with DoP credits: %d", with_dop)

    sample = hook.get_records(
        f"""
        SELECT director_name, dop_name, movie_count, avg_rating, total_votes
        FROM {TABLE}
        ORDER BY movie_count DESC, total_votes DESC
        LIMIT 10;
        """
    )
    for row in sample:
        logging.info("  %s", row)

    return {
        "row_count": total,
        "sample_count": len(sample),
        "with_dop_count": with_dop,
    }


def export_to_elasticsearch():
    """Export mart_director_credits rows from Postgres into Elasticsearch."""
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
        cursor = conn.cursor(name="mart_director_credits_export")
        cursor.itersize = ES_FETCH_SIZE
        try:
            cursor.execute(
                f"""
                SELECT
                    director_nconst,
                    director_name,
                    dop_nconst,
                    dop_name,
                    movie_count,
                    avg_rating,
                    total_votes,
                    first_movie_year,
                    last_movie_year,
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
                        director_nconst,
                        director_name,
                        dop_nconst,
                        dop_name,
                        movie_count,
                        avg_rating,
                        total_votes,
                        first_movie_year,
                        last_movie_year,
                        last_refreshed_at,
                    ) = row

                    doc_id = f"{director_nconst}::{dop_nconst or 'none'}"
                    yield {
                        "_index": ES_INDEX,
                        "_id": doc_id,
                        "_source": {
                            "director_nconst": director_nconst,
                            "director_name": director_name,
                            "dop_nconst": dop_nconst,
                            "dop_name": dop_name,
                            "movie_count": movie_count,
                            "avg_rating": avg_rating,
                            "total_votes": total_votes,
                            "first_movie_year": first_movie_year,
                            "last_movie_year": last_movie_year,
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
    dag_id="mart_director_credits",
    description="Build director-level movie mart and enrich with DoP credits",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ mart_director_credits task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule=[
        TITLE_BASICS_DATASET,
        TITLE_CREW_DATASET,
        NAME_BASICS_DATASET,
        TITLE_PRINCIPALS_DATASET,
        TITLE_RATINGS_DATASET,
    ],
    catchup=False,
    tags=["movies", "mart", "directors"],
) as dag:
    _, _, verify_load_task, notify_task = create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_director_credits completed",
    )

    export_to_elasticsearch_task = PythonOperator(
        task_id="export_to_elasticsearch",
        python_callable=export_to_elasticsearch,
    )

    verify_load_task.set_downstream(export_to_elasticsearch_task)
    export_to_elasticsearch_task.set_downstream(notify_task)
