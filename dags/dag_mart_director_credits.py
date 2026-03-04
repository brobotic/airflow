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
TABLE = "mart_director_credits"
REQUIRED_SOURCE_TABLES = ["title_basics", "title_crew", "name_basics", "title_principals"]
OPTIONAL_SOURCE_TABLES = ["title_ratings"]


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

    sql = f"""
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
        INSERT INTO {TABLE} (
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
        GROUP BY d.director_nconst, dn.primary_name, dc.dop_nconst, dpn.primary_name;
    """
    logging.info("Building '%s' with required Director + DoP credits.", TABLE)

    hook.run(sql)

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
    schedule="@daily",
    catchup=False,
    tags=["movies", "mart", "directors"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ mart_director_credits completed",
    )
