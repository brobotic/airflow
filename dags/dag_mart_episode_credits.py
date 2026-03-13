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
        MART_EPISODE_CREDITS_DATASET,
        NAME_BASICS_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_CREW_DATASET,
        TITLE_EPISODE_DATASET,
        TITLE_PRINCIPALS_DATASET,
        TITLE_RATINGS_DATASET,
    )
    from etl_tasks import create_standard_etl_tasks
    from notifications import notify_discord_failure
except ModuleNotFoundError:
    from dags.airflow_datasets import (
        MART_EPISODE_CREDITS_DATASET,
        NAME_BASICS_DATASET,
        TITLE_BASICS_DATASET,
        TITLE_CREW_DATASET,
        TITLE_EPISODE_DATASET,
        TITLE_PRINCIPALS_DATASET,
        TITLE_RATINGS_DATASET,
    )
    from dags.etl_tasks import create_standard_etl_tasks
    from dags.notifications import notify_discord_failure

CONN_ID = "postgres_movies"
TABLE = "mart_episode_credits"
REQUIRED_SOURCE_TABLES = ["title_basics", "title_episode", "title_principals", "title_crew", "name_basics"]
OPTIONAL_SOURCE_TABLES = ["title_ratings"]

ES_INDEX = os.getenv("ELASTICSEARCH_EPISODE_CREDITS_INDEX", TABLE)
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
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    for table_name in REQUIRED_SOURCE_TABLES:
        _assert_table_has_data(hook, table_name)

    for table_name in OPTIONAL_SOURCE_TABLES:
        if _table_exists(hook, table_name):
            logging.info("Optional source table '%s' is available.", table_name)
        else:
            logging.warning(
                "Optional source table '%s' is missing. Rating columns will be NULL/0.",
                table_name,
            )

    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            series_tconst       VARCHAR(20),
            series_title        TEXT,
            episode_tconst      VARCHAR(20),
            episode_title       TEXT,
            season_number       INTEGER,
            episode_number      INTEGER,
            person_nconst       VARCHAR(20),
            person_name         TEXT,
            credit_type         TEXT,
            job                 TEXT,
            characters          TEXT,
            average_rating      DOUBLE PRECISION,
            num_votes           INTEGER,
            last_refreshed_at   TIMESTAMP
        );
    """
    )
    logging.info("Table '%s' is ready.", TABLE)


def extract_and_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    has_ratings = _table_exists(hook, "title_ratings")

    ratings_join = (
        "LEFT JOIN title_ratings r ON r.tconst = d.episode_tconst"
        if has_ratings
        else "LEFT JOIN (SELECT NULL::VARCHAR(20) AS tconst, NULL::DOUBLE PRECISION AS average_rating, NULL::INTEGER AS num_votes) r ON false"
    )

    hook.run(f"TRUNCATE TABLE {TABLE};")
    hook.run(
        f"""
        WITH series_base AS (
            SELECT
                s.tconst AS series_tconst,
                s.primary_title AS series_title
            FROM title_basics s
            WHERE s.title_type IN ('tvSeries', 'tvMiniSeries')
        ),
        episode_base AS (
            SELECT
                e.tconst AS episode_tconst,
                e.parent_tconst AS series_tconst,
                e.season_number,
                e.episode_number,
                eb.primary_title AS episode_title
            FROM title_episode e
            JOIN title_basics eb ON eb.tconst = e.tconst
            JOIN series_base sb ON sb.series_tconst = e.parent_tconst
        ),
        principals_source AS (
            SELECT
                eb.series_tconst,
                eb.episode_tconst,
                eb.season_number,
                eb.episode_number,
                p.nconst AS person_nconst,
                lower(p.category) AS credit_type,
                p.job,
                p.characters
            FROM episode_base eb
            JOIN title_principals p ON p.tconst = eb.episode_tconst
            WHERE p.nconst IS NOT NULL
        ),
        crew_directors_source AS (
            SELECT
                eb.series_tconst,
                eb.episode_tconst,
                eb.season_number,
                eb.episode_number,
                trim(director_nconst) AS person_nconst,
                'director' AS credit_type,
                NULL::TEXT AS job,
                NULL::TEXT AS characters
            FROM episode_base eb
            JOIN title_crew c ON c.tconst = eb.episode_tconst
            CROSS JOIN unnest(string_to_array(c.directors, ',')) AS director_nconst
            WHERE c.directors IS NOT NULL AND c.directors <> ''
        ),
        crew_writers_source AS (
            SELECT
                eb.series_tconst,
                eb.episode_tconst,
                eb.season_number,
                eb.episode_number,
                trim(writer_nconst) AS person_nconst,
                'writer' AS credit_type,
                NULL::TEXT AS job,
                NULL::TEXT AS characters
            FROM episode_base eb
            JOIN title_crew c ON c.tconst = eb.episode_tconst
            CROSS JOIN unnest(string_to_array(c.writers, ',')) AS writer_nconst
            WHERE c.writers IS NOT NULL AND c.writers <> ''
        ),
        all_credits AS (
            SELECT * FROM principals_source
            UNION ALL
            SELECT * FROM crew_directors_source
            UNION ALL
            SELECT * FROM crew_writers_source
        ),
        deduped AS (
            SELECT DISTINCT
                ac.series_tconst,
                ac.episode_tconst,
                ac.season_number,
                ac.episode_number,
                ac.person_nconst,
                ac.credit_type,
                ac.job,
                ac.characters
            FROM all_credits ac
            WHERE ac.person_nconst IS NOT NULL AND ac.person_nconst <> ''
        )
        INSERT INTO {TABLE} (
            series_tconst,
            series_title,
            episode_tconst,
            episode_title,
            season_number,
            episode_number,
            person_nconst,
            person_name,
            credit_type,
            job,
            characters,
            average_rating,
            num_votes,
            last_refreshed_at
        )
        SELECT
            d.series_tconst,
            sb.series_title,
            d.episode_tconst,
            eb.episode_title,
            d.season_number,
            d.episode_number,
            d.person_nconst,
            n.primary_name AS person_name,
            d.credit_type,
            d.job,
            d.characters,
            r.average_rating,
            r.num_votes,
            now() AS last_refreshed_at
        FROM deduped d
        JOIN series_base sb ON sb.series_tconst = d.series_tconst
        JOIN episode_base eb ON eb.episode_tconst = d.episode_tconst
        LEFT JOIN name_basics n ON n.nconst = d.person_nconst
        {ratings_join};
    """
    )

    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_series_tconst ON {TABLE} (series_tconst);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_episode_tconst ON {TABLE} (episode_tconst);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_person_nconst ON {TABLE} (person_nconst);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_credit_type ON {TABLE} (credit_type);",
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_season_episode ON {TABLE} (season_number, episode_number);",
    ]

    for idx_sql in indexes:
        hook.run(idx_sql)

    logging.info("✅ Mart build complete for '%s'.", TABLE)


def verify_load():
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    total = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    series_count = hook.get_first(f"SELECT COUNT(DISTINCT series_tconst) FROM {TABLE};")[0]
    episode_count = hook.get_first(f"SELECT COUNT(DISTINCT episode_tconst) FROM {TABLE};")[0]

    logging.info("Total rows: %d", total)
    logging.info("Distinct series: %d", series_count)
    logging.info("Distinct episodes: %d", episode_count)

    sample = hook.get_records(
        f"""
        SELECT series_title, season_number, episode_number, episode_title, person_name, credit_type
        FROM {TABLE}
        ORDER BY season_number NULLS LAST, episode_number NULLS LAST
        LIMIT 10;
        """
    )
    for row in sample:
        logging.info("  %s", row)

    return {
        "row_count": total,
        "sample_count": len(sample),
        "series_count": series_count,
        "episode_count": episode_count,
    }


def export_to_elasticsearch():
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
        cursor = conn.cursor(name="mart_episode_credits_export")
        cursor.itersize = ES_FETCH_SIZE
        try:
            cursor.execute(
                f"""
                SELECT
                    series_tconst,
                    series_title,
                    episode_tconst,
                    episode_title,
                    season_number,
                    episode_number,
                    person_nconst,
                    person_name,
                    credit_type,
                    job,
                    characters,
                    average_rating,
                    num_votes,
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
                        series_tconst,
                        series_title,
                        episode_tconst,
                        episode_title,
                        season_number,
                        episode_number,
                        person_nconst,
                        person_name,
                        credit_type,
                        job,
                        characters,
                        average_rating,
                        num_votes,
                        last_refreshed_at,
                    ) = row

                    doc_id = "::".join(
                        [
                            episode_tconst or "",
                            person_nconst or "",
                            credit_type or "",
                            job or "",
                            characters or "",
                        ]
                    )
                    yield {
                        "_index": ES_INDEX,
                        "_id": doc_id,
                        "_source": {
                            "series_tconst": series_tconst,
                            "series_title": series_title,
                            "episode_tconst": episode_tconst,
                            "episode_title": episode_title,
                            "season_number": season_number,
                            "episode_number": episode_number,
                            "person_nconst": person_nconst,
                            "person_name": person_name,
                            "credit_type": credit_type,
                            "job": job,
                            "characters": characters,
                            "average_rating": average_rating,
                            "num_votes": num_votes,
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
    dag_id="mart_episode_credits",
    description="Build episode-level credits mart for TV series analytics",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ mart_episode_credits task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule=[
        TITLE_BASICS_DATASET,
        TITLE_EPISODE_DATASET,
        TITLE_PRINCIPALS_DATASET,
        TITLE_CREW_DATASET,
        NAME_BASICS_DATASET,
        TITLE_RATINGS_DATASET,
    ],
    catchup=False,
    tags=["movies", "mart", "episodes", "credits"],
) as dag:
    _, _, verify_load_task, notify_task = create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_episode_credits completed",
        extract_outlets=[MART_EPISODE_CREDITS_DATASET],
    )

    export_to_elasticsearch_task = PythonOperator(
        task_id="export_to_elasticsearch",
        python_callable=export_to_elasticsearch,
    )

    verify_load_task.set_downstream(export_to_elasticsearch_task)
    export_to_elasticsearch_task.set_downstream(notify_task)
