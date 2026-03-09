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
    from airflow_datasets import LETTERBOXD_DIARY_DATASET, MART_LETTERBOXD_DIARY_DATASET
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import LETTERBOXD_DIARY_DATASET, MART_LETTERBOXD_DIARY_DATASET
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

CONN_ID = "postgres_movies"
SOURCE_TABLE = "letterboxd_diary"
TABLE = "mart_letterboxd_diary"
ES_INDEX = os.getenv("ELASTICSEARCH_LETTERBOXD_DIARY_INDEX", TABLE)
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


def create_table():
    """Ensure source table has data and destination mart exists."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    source_exists = hook.get_first(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s;",
        parameters=(SOURCE_TABLE,),
    )[0]
    if source_exists == 0:
        raise ValueError(f"Source table '{SOURCE_TABLE}' does not exist.")

    source_count = hook.get_first(f"SELECT COUNT(*) FROM {SOURCE_TABLE};")[0]
    if source_count == 0:
        raise ValueError(f"Source table '{SOURCE_TABLE}' is empty.")

    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            metric_type       VARCHAR(50),
            metric_key        VARCHAR(100),
            films_logged      INTEGER,
            unique_titles     INTEGER,
            avg_rating        DOUBLE PRECISION,
            rewatch_count     INTEGER,
            first_watch_date  DATE,
            last_watch_date   DATE,
            last_refreshed_at TIMESTAMP,
            PRIMARY KEY (metric_type, metric_key)
        );
        """
    )
    hook.run(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_metric_type ON {TABLE} (metric_type);")
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    """Build analytics-friendly diary rollups by overall/month/film-year/tag."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    hook.run(f"TRUNCATE TABLE {TABLE};")

    hook.run(
        f"""
        WITH base AS (
            SELECT
                COALESCE(watched_date, entry_date) AS activity_date,
                film_name,
                film_year,
                letterboxd_uri,
                rating,
                rewatch,
                tags
            FROM {SOURCE_TABLE}
        ),
        tags_expanded AS (
            SELECT
                lower(trim(tag_value)) AS tag,
                b.activity_date,
                b.film_name,
                b.film_year,
                b.letterboxd_uri,
                b.rating,
                b.rewatch
            FROM base b
            CROSS JOIN LATERAL unnest(string_to_array(COALESCE(b.tags, ''), ',')) AS tag_value
            WHERE trim(tag_value) <> ''
        ),
        summary_rows AS (
            SELECT
                'overall'::VARCHAR(50) AS metric_type,
                'all'::VARCHAR(100) AS metric_key,
                COUNT(*)::INTEGER AS films_logged,
                COUNT(DISTINCT COALESCE(letterboxd_uri, film_name))::INTEGER AS unique_titles,
                AVG(rating) AS avg_rating,
                SUM(CASE WHEN rewatch THEN 1 ELSE 0 END)::INTEGER AS rewatch_count,
                MIN(activity_date) AS first_watch_date,
                MAX(activity_date) AS last_watch_date
            FROM base

            UNION ALL

            SELECT
                'watched_month'::VARCHAR(50) AS metric_type,
                to_char(date_trunc('month', activity_date), 'YYYY-MM')::VARCHAR(100) AS metric_key,
                COUNT(*)::INTEGER AS films_logged,
                COUNT(DISTINCT COALESCE(letterboxd_uri, film_name))::INTEGER AS unique_titles,
                AVG(rating) AS avg_rating,
                SUM(CASE WHEN rewatch THEN 1 ELSE 0 END)::INTEGER AS rewatch_count,
                MIN(activity_date) AS first_watch_date,
                MAX(activity_date) AS last_watch_date
            FROM base
            WHERE activity_date IS NOT NULL
            GROUP BY date_trunc('month', activity_date)

            UNION ALL

            SELECT
                'film_year'::VARCHAR(50) AS metric_type,
                film_year::VARCHAR(100) AS metric_key,
                COUNT(*)::INTEGER AS films_logged,
                COUNT(DISTINCT COALESCE(letterboxd_uri, film_name))::INTEGER AS unique_titles,
                AVG(rating) AS avg_rating,
                SUM(CASE WHEN rewatch THEN 1 ELSE 0 END)::INTEGER AS rewatch_count,
                MIN(activity_date) AS first_watch_date,
                MAX(activity_date) AS last_watch_date
            FROM base
            WHERE film_year IS NOT NULL
            GROUP BY film_year

            UNION ALL

            SELECT
                'tag'::VARCHAR(50) AS metric_type,
                tag::VARCHAR(100) AS metric_key,
                COUNT(*)::INTEGER AS films_logged,
                COUNT(DISTINCT COALESCE(letterboxd_uri, film_name))::INTEGER AS unique_titles,
                AVG(rating) AS avg_rating,
                SUM(CASE WHEN rewatch THEN 1 ELSE 0 END)::INTEGER AS rewatch_count,
                MIN(activity_date) AS first_watch_date,
                MAX(activity_date) AS last_watch_date
            FROM tags_expanded
            GROUP BY tag
        )
        INSERT INTO {TABLE} (
            metric_type,
            metric_key,
            films_logged,
            unique_titles,
            avg_rating,
            rewatch_count,
            first_watch_date,
            last_watch_date,
            last_refreshed_at
        )
        SELECT
            metric_type,
            metric_key,
            films_logged,
            unique_titles,
            avg_rating,
            rewatch_count,
            first_watch_date,
            last_watch_date,
            now() AS last_refreshed_at
        FROM summary_rows;
        """
    )

    logging.info("✅ Mart build complete for '%s'.", TABLE)


def verify_load():
    """Log mart volume and top tag/month slices for quick validation."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    total = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    logging.info("Total rows: %d", total)

    top_tags = hook.get_records(
        f"""
        SELECT metric_key, films_logged, avg_rating
        FROM {TABLE}
        WHERE metric_type = 'tag'
        ORDER BY films_logged DESC, metric_key
        LIMIT 10;
        """
    )
    for row in top_tags:
        logging.info("Top tag: %s", row)

    months = hook.get_records(
        f"""
        SELECT metric_key, films_logged, avg_rating
        FROM {TABLE}
        WHERE metric_type = 'watched_month'
        ORDER BY metric_key DESC
        LIMIT 6;
        """
    )
    for row in months:
        logging.info("Recent month: %s", row)

    return {
        "row_count": total,
        "top_tag_count": len(top_tags),
        "month_count": len(months),
    }


def export_to_elasticsearch():
    """Export mart_letterboxd_diary rows from Postgres into Elasticsearch."""
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
        cursor = conn.cursor(name="mart_letterboxd_diary_export")
        cursor.itersize = ES_FETCH_SIZE
        try:
            cursor.execute(
                f"""
                SELECT
                    metric_type,
                    metric_key,
                    films_logged,
                    unique_titles,
                    avg_rating,
                    rewatch_count,
                    first_watch_date,
                    last_watch_date,
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
                        metric_type,
                        metric_key,
                        films_logged,
                        unique_titles,
                        avg_rating,
                        rewatch_count,
                        first_watch_date,
                        last_watch_date,
                        last_refreshed_at,
                    ) = row

                    doc_id = f"{metric_type}::{metric_key}"
                    yield {
                        "_index": ES_INDEX,
                        "_id": doc_id,
                        "_source": {
                            "metric_type": metric_type,
                            "metric_key": metric_key,
                            "films_logged": films_logged,
                            "unique_titles": unique_titles,
                            "avg_rating": avg_rating,
                            "rewatch_count": rewatch_count,
                            "first_watch_date": (
                                first_watch_date.isoformat() if first_watch_date else None
                            ),
                            "last_watch_date": (
                                last_watch_date.isoformat() if last_watch_date else None
                            ),
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
    dag_id="mart_letterboxd_diary",
    description="Build analytics mart from Letterboxd diary entries",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ mart_letterboxd_diary task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule=[LETTERBOXD_DIARY_DATASET],
    catchup=False,
    tags=["letterboxd", "mart", "movies"],
) as dag:
    _, _, verify_load_task, notify_task = create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_letterboxd_diary completed",
        extract_outlets=[MART_LETTERBOXD_DIARY_DATASET],
    )

    export_to_elasticsearch_task = PythonOperator(
        task_id="export_to_elasticsearch",
        python_callable=export_to_elasticsearch,
    )

    verify_load_task.set_downstream(export_to_elasticsearch_task)
    export_to_elasticsearch_task.set_downstream(notify_task)
