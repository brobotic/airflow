import logging
import os
import time
from datetime import datetime
from functools import partial
from importlib import import_module
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow_datasets import (
        LETTERBOXD_DIARY_DATASET,
        MART_LETTERBOXD_MOVIE_MATCHES_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_RATINGS_DATASET,
    )
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import (
        LETTERBOXD_DIARY_DATASET,
        MART_LETTERBOXD_MOVIE_MATCHES_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_RATINGS_DATASET,
    )
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

CONN_ID = "postgres_movies"
TABLE = "mart_letterboxd_movie_matches"
REQUIRED_SOURCE_TABLES = ["letterboxd_diary", "title_basics"]
OPTIONAL_SOURCE_TABLES = ["title_ratings"]

ES_INDEX = os.getenv("ELASTICSEARCH_LETTERBOXD_MOVIE_MATCHES_INDEX", TABLE)
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
    host = os.getenv("ELASTICSEARCH_HOST", "http://192.168.1.60:9200")
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
                "Optional source table '%s' is missing. IMDb rating columns will be NULL.",
                table_name,
            )

    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            diary_id               BIGINT PRIMARY KEY,
            activity_date          DATE,
            film_name              TEXT,
            film_year              INTEGER,
            letterboxd_uri         TEXT,
            letterboxd_rating      DOUBLE PRECISION,
            rewatch                BOOLEAN,
            tags                   TEXT,
            matched_tconst         VARCHAR(20),
            matched_primary_title  TEXT,
            matched_original_title TEXT,
            matched_start_year     INTEGER,
            imdb_average_rating    DOUBLE PRECISION,
            imdb_num_votes         INTEGER,
            candidate_count        INTEGER,
            match_confidence       VARCHAR(40),
            last_refreshed_at      TIMESTAMP
        );
    """
    )

    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_matched_tconst ON {TABLE} (matched_tconst);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_film_year ON {TABLE} (film_year);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_activity_date ON {TABLE} (activity_date);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_match_confidence ON {TABLE} (match_confidence);",
    ]

    for idx_sql in indexes:
        hook.run(idx_sql)

    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    """Build diary-to-IMDb movie matches for cross-mart analytics in Kibana."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    has_ratings = _table_exists(hook, "title_ratings")

    ratings_join = (
        "LEFT JOIN title_ratings r ON r.tconst = c.tconst"
        if has_ratings
        else "LEFT JOIN (SELECT NULL::VARCHAR(20) AS tconst, NULL::DOUBLE PRECISION AS average_rating, NULL::INTEGER AS num_votes) r ON false"
    )

    hook.run(f"TRUNCATE TABLE {TABLE};")
    hook.run(
        f"""
        WITH diary_base AS (
            SELECT
                d.diary_id,
                COALESCE(d.watched_date, d.entry_date) AS activity_date,
                d.film_name,
                d.film_year,
                d.letterboxd_uri,
                d.rating AS letterboxd_rating,
                d.rewatch,
                d.tags
            FROM letterboxd_diary d
        ),
        candidates AS (
            SELECT
                db.diary_id,
                db.activity_date,
                db.film_name,
                db.film_year,
                db.letterboxd_uri,
                db.letterboxd_rating,
                db.rewatch,
                db.tags,
                b.tconst,
                b.primary_title,
                b.original_title,
                b.start_year,
                r.average_rating,
                r.num_votes,
                CASE
                    WHEN lower(b.primary_title) = lower(db.film_name) THEN 1
                    WHEN lower(b.original_title) = lower(db.film_name) THEN 2
                    ELSE 3
                END AS title_match_rank
            FROM diary_base db
            JOIN title_basics b
              ON b.title_type = 'movie'
             AND b.start_year = db.film_year
             AND (
                    lower(b.primary_title) = lower(db.film_name)
                 OR lower(b.original_title) = lower(db.film_name)
             )
            {ratings_join}
        ),
        ranked AS (
            SELECT
                c.*,
                COUNT(*) OVER (PARTITION BY c.diary_id) AS candidate_count,
                ROW_NUMBER() OVER (
                    PARTITION BY c.diary_id
                    ORDER BY
                        c.title_match_rank ASC,
                        c.num_votes DESC NULLS LAST,
                        c.average_rating DESC NULLS LAST,
                        c.tconst ASC
                ) AS candidate_rank
            FROM candidates c
        )
        INSERT INTO {TABLE} (
            diary_id,
            activity_date,
            film_name,
            film_year,
            letterboxd_uri,
            letterboxd_rating,
            rewatch,
            tags,
            matched_tconst,
            matched_primary_title,
            matched_original_title,
            matched_start_year,
            imdb_average_rating,
            imdb_num_votes,
            candidate_count,
            match_confidence,
            last_refreshed_at
        )
        SELECT
            db.diary_id,
            db.activity_date,
            db.film_name,
            db.film_year,
            db.letterboxd_uri,
            db.letterboxd_rating,
            db.rewatch,
            db.tags,
            r.tconst AS matched_tconst,
            r.primary_title AS matched_primary_title,
            r.original_title AS matched_original_title,
            r.start_year AS matched_start_year,
            r.average_rating AS imdb_average_rating,
            r.num_votes AS imdb_num_votes,
            r.candidate_count,
            CASE
                WHEN r.tconst IS NULL THEN 'unmatched'
                WHEN r.title_match_rank = 1 THEN 'exact_primary_title_year'
                WHEN r.title_match_rank = 2 THEN 'exact_original_title_year'
                ELSE 'low_confidence'
            END AS match_confidence,
            now() AS last_refreshed_at
        FROM diary_base db
        LEFT JOIN ranked r
            ON r.diary_id = db.diary_id
           AND r.candidate_rank = 1;
    """
    )

    logging.info("✅ Mart build complete for '%s'.", TABLE)


def verify_load():
    """Log row counts and match quality summary."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    total = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    matched = hook.get_first(
        f"SELECT COUNT(*) FROM {TABLE} WHERE matched_tconst IS NOT NULL;"
    )[0]

    logging.info("Total rows: %d", total)
    logging.info("Matched rows: %d", matched)

    top_unmatched = hook.get_records(
        f"""
        SELECT film_name, film_year, COUNT(*) AS cnt
        FROM {TABLE}
        WHERE matched_tconst IS NULL
        GROUP BY film_name, film_year
        ORDER BY cnt DESC, film_name
        LIMIT 10;
        """
    )
    for row in top_unmatched:
        logging.info("Unmatched: %s", row)

    return {
        "row_count": total,
        "matched_count": matched,
        "sample_count": len(top_unmatched),
    }


def export_to_elasticsearch():
    """Export mart_letterboxd_movie_matches rows from Postgres into Elasticsearch."""
    logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)

    es_host = os.getenv("ELASTICSEARCH_HOST", "http://192.168.1.60:9200")
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

    def actions():
        conn = hook.get_conn()
        cursor = conn.cursor(name="mart_letterboxd_movie_matches_export")
        cursor.itersize = ES_FETCH_SIZE
        try:
            cursor.execute(
                f"""
                SELECT
                    diary_id,
                    activity_date,
                    film_name,
                    film_year,
                    letterboxd_uri,
                    letterboxd_rating,
                    rewatch,
                    tags,
                    matched_tconst,
                    matched_primary_title,
                    matched_original_title,
                    matched_start_year,
                    imdb_average_rating,
                    imdb_num_votes,
                    candidate_count,
                    match_confidence,
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
                        diary_id,
                        activity_date,
                        film_name,
                        film_year,
                        letterboxd_uri,
                        letterboxd_rating,
                        rewatch,
                        tags,
                        matched_tconst,
                        matched_primary_title,
                        matched_original_title,
                        matched_start_year,
                        imdb_average_rating,
                        imdb_num_votes,
                        candidate_count,
                        match_confidence,
                        last_refreshed_at,
                    ) = row

                    yield {
                        "_index": ES_INDEX,
                        "_id": str(diary_id),
                        "_source": {
                            "diary_id": diary_id,
                            "activity_date": activity_date.isoformat() if activity_date else None,
                            "film_name": film_name,
                            "film_year": film_year,
                            "letterboxd_uri": letterboxd_uri,
                            "letterboxd_rating": letterboxd_rating,
                            "rewatch": rewatch,
                            "tags": tags,
                            "matched_tconst": matched_tconst,
                            "matched_primary_title": matched_primary_title,
                            "matched_original_title": matched_original_title,
                            "matched_start_year": matched_start_year,
                            "imdb_average_rating": imdb_average_rating,
                            "imdb_num_votes": imdb_num_votes,
                            "candidate_count": candidate_count,
                            "match_confidence": match_confidence,
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
            raise ValueError(f"Elasticsearch bulk export completed with {errors} errors.")

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


with DAG(
    dag_id="mart_letterboxd_movie_matches",
    description="Build movie-level Letterboxd-to-IMDb matching mart for cross-mart analytics",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ mart_letterboxd_movie_matches task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule=[
        LETTERBOXD_DIARY_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_RATINGS_DATASET,
    ],
    catchup=False,
    tags=["letterboxd", "mart", "movies"],
) as dag:
    _, _, verify_load_task, notify_task = create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_letterboxd_movie_matches completed",
        extract_outlets=[MART_LETTERBOXD_MOVIE_MATCHES_DATASET],
    )

    export_to_elasticsearch_task = PythonOperator(
        task_id="export_to_elasticsearch",
        python_callable=export_to_elasticsearch,
    )

    verify_load_task.set_downstream(export_to_elasticsearch_task)
    export_to_elasticsearch_task.set_downstream(notify_task)
