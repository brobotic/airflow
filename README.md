# Airflow (local)

## Accessing Airflow by hostname

The Airflow API/UI is published on host port `8080` by default and advertises
`http://airflow.home.lab` as its public URL.

To make that hostname resolve to this Docker host, add one of these outside the repo:

- A DNS record for `airflow.home.lab` pointing at the host running this stack.
- A local `/etc/hosts` entry such as `127.0.0.1 airflow.home.lab` if you only need access from the same machine.

If you already have a reverse proxy for `airflow.home.lab`, point it at this stack on port `8080`.

You can override the defaults with `.env`:

```env
AIRFLOW_HOST=airflow.home.lab
AIRFLOW_WEB_PORT=8080
```

# TODO

* make sure editors exist in movie credits mart. update movie credits query script to include editor column
* query script that can return all movies from a director and highlight watched/unwatched based on letterboxd data

## Authoring new ETL DAGs

Use the shared helpers for notifications and standard task wiring:

- Guide: [DAG_AUTHORING.md](DAG_AUTHORING.md)
- Shared notifications: `dags/notifications.py`
- Shared task-chain factory: `dags/etl_tasks.py`

# Testing DAGs

`docker compose exec airflow-scheduler airflow tasks test movies_ratings_etl extract_and_load 2026-03-02`

## Unit tests

Run the DAG unit tests from this folder:

`/home/micha/github/homelab/docker/airflow/venv/bin/python -m pytest tests/test_dag_etl_movies.py -q`

Expected result:

`10 passed`

## Postgres persistence

The Postgres service in `postgres.yaml` stores data in the named Docker volume `postgres_movies_data`, so data persists across container restarts and `docker compose stop/start`.

To keep data, use:

`docker compose -f postgres.yaml up -d`
`docker compose -f postgres.yaml restart postgres_movies`

Avoid using `docker compose -f postgres.yaml down -v` unless you intentionally want to delete the database volume.

## Validate DAG outputs in Postgres

Use the helper script to quickly verify expected DAG output tables exist and contain at least N rows:

`scripts/validate_dag_outputs.sh`

Examples:

- Default check (all known ETL/mart tables, min 1 row each):
    - `scripts/validate_dag_outputs.sh`
- Set one global threshold for all default tables:
    - `scripts/validate_dag_outputs.sh --min-rows 100`
- Override specific tables with custom thresholds:
    - `scripts/validate_dag_outputs.sh --table mart_titles_enriched:1000 --table title_ratings:5000`

Default checks include mart tables:

- `mart_titles_enriched`
- `mart_director_credits`
- `mart_movie_credits`
- `mart_episode_credits`
- `mart_series_people_rollup`
- `mart_episode_enriched`
- `mart_series_akas`

Default checks also include IMDb staging tables:

- `title_basics`
- `title_akas`
- `name_basics`
- `title_crew`
- `title_episode`
- `title_principals`
- `title_ratings`

The script exits with code `0` when all checks pass, and non-zero when any table is missing or below threshold.

## Letterboxd diary pipeline

Current Letterboxd scope is intentionally minimal: only `diary.csv`.

### DAGs

- ETL DAG: `letterboxd_diary_etl` → loads into `letterboxd_diary`
- Mart DAG: `mart_letterboxd_diary` → builds rollups in `mart_letterboxd_diary`
- Match mart DAG: `mart_letterboxd_movie_matches` → builds movie-level joins in `mart_letterboxd_movie_matches`
- Trigger wiring: `mart_letterboxd_diary` runs from `LETTERBOXD_DIARY_DATASET`

### Elasticsearch indices (kept separate)

- Diary rollup mart index env var: `ELASTICSEARCH_LETTERBOXD_DIARY_INDEX`
    - Default index: `mart_letterboxd_diary`
- Movie-match mart index env var: `ELASTICSEARCH_LETTERBOXD_MOVIE_MATCHES_INDEX`
    - Default index: `mart_letterboxd_movie_matches`

This keeps Letterboxd datasets separate from IMDb marts while still allowing cross-index dashboards.

### Kibana cross-index mapping guide

Use `mart_letterboxd_movie_matches` as the bridge index when blending with IMDb marts.

Recommended key fields for blended visuals:

- Join key to IMDb marts: `matched_tconst`
- Letterboxd-side rating: `letterboxd_rating`
- IMDb-side rating: `imdb_average_rating`
- IMDb popularity: `imdb_num_votes`
- Match quality filter: `match_confidence`
- Time axis: `activity_date`

Recommended filters:

- Include only confident matches: `match_confidence:(exact_primary_title_year OR exact_original_title_year)`
- Exclude unmatched rows when comparing with IMDb: `matched_tconst:*`

Example dashboard ideas:

- Letterboxd vs IMDb rating gap by month (`avg(letterboxd_rating)` vs `avg(imdb_average_rating)`)
- Most-watched matched movies ranked by `count(diary_id)` and `imdb_num_votes`
- Match-quality monitor split by `match_confidence`

### Saved query snippets (Kibana + SQL)

Use these for quick dashboard prototyping.

KQL filters (Kibana):

- Confident matches only:
    - `match_confidence:(exact_primary_title_year OR exact_original_title_year)`
- IMDb-comparable rows only:
    - `matched_tconst:*`
- Exclude unmatched + low-confidence in one filter:
    - `matched_tconst:* AND NOT match_confidence:low_confidence`

SQL snippets (Postgres):

1) Rating gap by month (Letterboxd vs IMDb):

```sql
SELECT
        date_trunc('month', activity_date)::date AS month,
        ROUND(AVG(letterboxd_rating)::numeric, 2) AS avg_letterboxd_rating,
        ROUND(AVG(imdb_average_rating)::numeric, 2) AS avg_imdb_rating,
        ROUND((AVG(letterboxd_rating) - AVG(imdb_average_rating))::numeric, 2) AS rating_gap,
        COUNT(*) AS matched_entries
FROM mart_letterboxd_movie_matches
WHERE matched_tconst IS NOT NULL
    AND match_confidence IN ('exact_primary_title_year', 'exact_original_title_year')
    AND activity_date IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

2) Most watched matched movies:

```sql
SELECT
        matched_tconst,
        matched_primary_title,
        matched_start_year,
        COUNT(*) AS diary_watches,
        ROUND(AVG(letterboxd_rating)::numeric, 2) AS avg_letterboxd_rating,
        ROUND(AVG(imdb_average_rating)::numeric, 2) AS avg_imdb_rating,
        MAX(imdb_num_votes) AS imdb_num_votes
FROM mart_letterboxd_movie_matches
WHERE matched_tconst IS NOT NULL
GROUP BY matched_tconst, matched_primary_title, matched_start_year
ORDER BY diary_watches DESC, imdb_num_votes DESC NULLS LAST
LIMIT 50;
```

3) Match-quality monitor:

```sql
SELECT
        match_confidence,
        COUNT(*) AS row_count,
        ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_total
FROM mart_letterboxd_movie_matches
GROUP BY match_confidence
ORDER BY row_count DESC;
```

### Run DAG tasks manually

```bash
docker compose -f airflow.yaml exec airflow-scheduler \
    airflow tasks test letterboxd_diary_etl create_table 2026-03-08

docker compose -f airflow.yaml exec airflow-scheduler \
    airflow tasks test letterboxd_diary_etl extract_and_load 2026-03-08

docker compose -f airflow.yaml exec airflow-scheduler \
    airflow tasks test mart_letterboxd_diary extract_and_load 2026-03-08
```

### Query helper script

Use `scripts/query_letterboxd_diary_mart.py` for ready-made analytics queries.

By default this script now runs in remote Postgres mode (no Docker required), so it can be
run from another server as long as it can reach your Postgres host.

Examples:

```bash
python scripts/query_letterboxd_diary_mart.py --query overview
python scripts/query_letterboxd_diary_mart.py --query monthly --limit 12
python scripts/query_letterboxd_diary_mart.py --query year-overview --year 2024
python scripts/query_letterboxd_diary_mart.py --query top-tags --limit 20
python scripts/query_letterboxd_diary_mart.py --query top-tags --year 2024 --limit 20
python scripts/query_letterboxd_diary_mart.py --query rewatch-months --limit 12
python scripts/query_letterboxd_diary_mart.py --query recent-entries --limit 25
python scripts/query_letterboxd_diary_mart.py --query film-year-entries --film-year 1999
```

Remote mode env vars:

- `POSTGRES_HOST` (or `PGHOST`)
- `POSTGRES_PORT` (or `PGPORT`)
- `POSTGRES_USER`
- `POSTGRES_DB`
- `POSTGRES_PASSWORD` (used via `PGPASSWORD`)

Docker fallback (old behavior):

```bash
python scripts/query_letterboxd_diary_mart.py --execution-mode docker --query overview
```

Available `--query` values:

- `overview`
- `monthly`
- `top-tags`
- `top-film-years`
- `rewatch-months`
- `recent-entries`
- `year-overview`
- `film-year-entries`

For the movie-level join mart, use `scripts/query_letterboxd_movie_matches.py`.

This script also defaults to remote Postgres mode for remote execution and supports
`--execution-mode docker` for local container execution.

Examples:

```bash
python scripts/query_letterboxd_movie_matches.py --query overview
python scripts/query_letterboxd_movie_matches.py --query match-quality
python scripts/query_letterboxd_movie_matches.py --query rating-gap-monthly --limit 12
python scripts/query_letterboxd_movie_matches.py --query top-matched-movies --limit 25
python scripts/query_letterboxd_movie_matches.py --query unmatched --limit 25
python scripts/query_letterboxd_movie_matches.py --query film-year-entries --film-year 1999
```

Available `--query` values:

- `overview`
- `match-quality`
- `rating-gap-monthly`
- `top-matched-movies`
- `unmatched`
- `recent-matches`
- `film-year-entries`

## Link movies to directors

Linking movies to directors:

- Movie IDs come from `title_basics.tconst` (`dags/dag_etl_movies.py`).
- Director IDs come from `title_crew.directors` (`dags/dag_etl_crew.py`) as comma-separated `nconst` values.
- Director names come from `name_basics.nconst -> primary_name` (`dags/dag_etl_names.py`).

Use this query to get movie/director pairs:

```sql
SELECT
    b.tconst,
    b.primary_title,
    d.director_nconst,
    n.primary_name AS director_name
FROM title_basics b
JOIN title_crew c ON c.tconst = b.tconst
CROSS JOIN LATERAL unnest(string_to_array(c.directors, ',')) AS d(director_nconst)
LEFT JOIN name_basics n ON n.nconst = trim(d.director_nconst)
WHERE b.title_type = 'movie';
```

Note: `dags/dag_mart_director_credits.py` builds a director-level aggregate mart, not a per-movie mapping table.

For a per-movie enrichment mart containing director and DoP fields, use `dags/dag_mart_movie_credits.py` (`mart_movie_credits`).

Run it from the host with `psql`:

```bash
PGPASSWORD=movies_pass psql -h localhost -p 5433 -U movies_user -d movies_db -c "
SELECT
    b.tconst,
    b.primary_title,
    d.director_nconst,
    n.primary_name AS director_name
FROM title_basics b
JOIN title_crew c ON c.tconst = b.tconst
CROSS JOIN LATERAL unnest(string_to_array(c.directors, ',')) AS d(director_nconst)
LEFT JOIN name_basics n ON n.nconst = trim(d.director_nconst)
WHERE b.title_type = 'movie'
LIMIT 20;
"
```


# DAG Authoring (ETL pattern)

Use this pattern for TSV/CSV ETL DAGs so notifications and task wiring stay consistent.

## Shared modules

- `dags/notifications.py`
  - `notify_discord_failure(...)`
  - `notify_discord_success(...)`
- `dags/etl_tasks.py`
  - `create_standard_etl_tasks(...)`

## Minimal template

```python
from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl_tasks import create_standard_etl_tasks
from notifications import notify_discord_failure

TABLE = "your_table"


def create_table():
    hook = PostgresHook(postgres_conn_id="postgres_movies")
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id TEXT PRIMARY KEY
        );
    """)


def extract_and_load():
    pass


def verify_load():
    return {
        "row_count": 0,
        "sample_count": 0,
    }


with DAG(
    dag_id="your_etl_dag",
    description="ETL: ...",
    default_args={
        "on_failure_callback": partial(
            notify_discord_failure,
            title="❌ your_etl_dag task failed",
        )
    },
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["etl"],
) as dag:
    create_standard_etl_tasks(
        create_table_callable=create_table,
        extract_and_load_callable=extract_and_load,
        verify_load_callable=verify_load,
        table=TABLE,
        success_title="✅ your_etl_dag completed",
    )
```

## Notes

- Keep `verify_load()` returning a dictionary with `row_count` and `sample_count`.
- `create_standard_etl_tasks(...)` assumes task IDs:
  - `create_table`
  - `extract_and_load`
  - `verify_load`
  - `notify_discord`
- If you need custom task IDs or extra steps, wire operators manually but still reuse `notifications.py`.

## Unit test scaffold

Follow the same pytest + monkeypatch style used in `tests/test_dag_etl_movies.py` and `tests/test_dag_etl_ratings.py`.

### 1) Verify task queries and return shape

```python
from dags import dag_your_etl as module


class FakeHookVerify:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.get_first_calls = []
        self.get_records_calls = []

    def get_first(self, sql):
        self.get_first_calls.append(sql)
        return (2,)

    def get_records(self, sql):
        self.get_records_calls.append(sql)
        return [("id1", "sample")]


def test_verify_load_uses_queries(monkeypatch):
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookVerify(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)

    result = module.verify_load()

    hook = created["hook"]
    assert hook.postgres_conn_id == module.CONN_ID
    assert len(hook.get_first_calls) == 1
    assert len(hook.get_records_calls) == 1
    assert isinstance(result, dict)
    assert "row_count" in result
    assert "sample_count" in result
```

### 2) Verify task chain wiring

```python
def test_dag_task_chain():
    dag = module.dag
    create_task = dag.get_task("create_table")
    extract_task = dag.get_task("extract_and_load")
    verify_task = dag.get_task("verify_load")
    notify_task = dag.get_task("notify_discord")

    assert extract_task.task_id in create_task.downstream_task_ids
    assert verify_task.task_id in extract_task.downstream_task_ids
    assert notify_task.task_id in verify_task.downstream_task_ids
```

### 3) Verify failure callback is configured

```python
def test_failure_callback_exists():
    dag = module.dag
    assert "on_failure_callback" in dag.default_args
    assert callable(dag.default_args["on_failure_callback"])
```

### 4) Run tests

From `docker/airflow`:

`/home/micha/github/homelab/docker/airflow/venv/bin/python -m pytest tests/test_dag_your_etl.py -q`

### Starter file

A copy-ready scaffold is available at:

`tests/test_dag_etl_template.py.example`

Use it by copying and renaming it, then updating the import:

`cp tests/test_dag_etl_template.py.example tests/test_dag_etl_your_dag.py`

## Elasticsearch export tuning (`mart_titles_enriched`)

The DAG `dags/dag_mart_titles_enriched.py` includes an `export_to_elasticsearch` task with env-driven tuning.

### Environment variables

- `ELASTICSEARCH_HOST` (default: `http://192.168.1.60:9200`)
- `ELASTICSEARCH_INDEX` (default: `mart_titles_enriched`)
- `ELASTICSEARCH_API_KEY` (optional)
- `ELASTICSEARCH_USERNAME` / `ELASTICSEARCH_PASSWORD` (optional)
- `ELASTICSEARCH_FETCH_SIZE` (default: `2000`) — Postgres fetch batch size (streaming cursor)
- `ELASTICSEARCH_CHUNK_SIZE` (default: `500`) — docs per bulk chunk
- `ELASTICSEARCH_MAX_CHUNK_BYTES` (default: `10485760`) — max bytes per bulk request (10MB)
- `ELASTICSEARCH_THREAD_COUNT` (default: `2`) — parallel bulk workers
- `ELASTICSEARCH_QUEUE_SIZE` (default: `2`) — in-memory work queue per bulk helper
- `ELASTICSEARCH_REQUEST_TIMEOUT` (default: `120`) — request timeout in seconds
- `ELASTICSEARCH_PROGRESS_EVERY` (default: `10000`) — log progress every N processed docs
- `ELASTICSEARCH_FAST_INDEX_MODE` (default: `false`) — temporarily set `refresh_interval=-1` and `number_of_replicas=0`, then restore

### Recommended profiles

Safe profile (low memory pressure / OOM-resistant):

- `ELASTICSEARCH_FETCH_SIZE=2000`
- `ELASTICSEARCH_CHUNK_SIZE=500`
- `ELASTICSEARCH_MAX_CHUNK_BYTES=10485760`
- `ELASTICSEARCH_THREAD_COUNT=2`
- `ELASTICSEARCH_QUEUE_SIZE=2`
- `ELASTICSEARCH_REQUEST_TIMEOUT=120`
- `ELASTICSEARCH_PROGRESS_EVERY=10000`
- `ELASTICSEARCH_FAST_INDEX_MODE=true`

Fast profile (increase throughput if cluster has headroom):

- `ELASTICSEARCH_FETCH_SIZE=5000`
- `ELASTICSEARCH_CHUNK_SIZE=2000`
- `ELASTICSEARCH_MAX_CHUNK_BYTES=20971520`
- `ELASTICSEARCH_THREAD_COUNT=4`
- `ELASTICSEARCH_QUEUE_SIZE=4`
- `ELASTICSEARCH_REQUEST_TIMEOUT=180`
- `ELASTICSEARCH_PROGRESS_EVERY=20000`
- `ELASTICSEARCH_FAST_INDEX_MODE=true`

If task memory spikes or worker restarts occur, lower `ELASTICSEARCH_CHUNK_SIZE`, `ELASTICSEARCH_THREAD_COUNT`, and `ELASTICSEARCH_QUEUE_SIZE` first.


# Queries

Use `scripts/validate_mart.sh` to execute the maintained validation SQL in
`scripts/validate_mart.sql`.

The SQL file now validates all mart DAG outputs:

- `mart_titles_enriched`
- `mart_movie_credits`
- `mart_director_credits`

It includes row-count/freshness checks plus per-mart distribution and top-record sanity queries.

## Kibana visualization recommendations (movies + TV)

Now that mart data is exported to Elasticsearch, build Kibana dashboards from the mart indices.

### Suggested data views (index patterns)

- `mart_titles_enriched`
- `mart_movie_credits`
- `mart_director_credits`
- `mart_episode_enriched`
- `mart_episode_credits`
- `mart_series_people_rollup`
- `mart_series_akas`

Tip: if you exported with a prefix, use wildcard patterns (for example `movies-mart_*`) and create one data view per mart for simpler Lens authoring.

### Dashboard 1: Movie quality and popularity

1. **Ratings bucket by era (stacked bar)**
    - Index: `mart_titles_enriched`
    - Filter: `title_type: "movie"`
    - X-axis: `era`
    - Break down: `rating_bucket`
    - Metric: `Count`

2. **Movie releases over time (line)**
    - Index: `mart_titles_enriched`
    - Filter: `title_type: "movie"`
    - X-axis: `start_year` (histogram)
    - Metric: `Count`

3. **Popularity vs rating (scatter/bubble)**
    - Index: `mart_titles_enriched`
    - Filter: `title_type: "movie" and average_rating:* and num_votes:*`
    - X-axis: `average_rating`
    - Y-axis: `num_votes`
    - Break down: `era` (or `title_type` if you want movie vs TV comparison)

4. **Top movies by votes (table)**
    - Index: `mart_titles_enriched`
    - Filter: `title_type: "movie"`
    - Columns: `primary_title`, `start_year`, `average_rating`, `num_votes`, `rating_bucket`
    - Sort: `num_votes` descending

### Dashboard 2: Movie credits and creator performance

1. **Director productivity leaderboard (horizontal bar)**
    - Index: `mart_director_credits`
    - Terms: `director_name`
    - Metric: `Max(movie_count)`
    - Sort by metric descending

2. **Director audience reach (horizontal bar)**
    - Index: `mart_director_credits`
    - Terms: `director_name`
    - Metric: `Max(total_votes)`

3. **Director quality vs scale (scatter)**
    - Index: `mart_director_credits`
    - X-axis: `avg_rating`
    - Y-axis: `movie_count`
    - Bubble size: `total_votes`
    - Break down: top `director_name`

4. **Director ↔ DoP collaboration matrix (heat map)**
    - Index: `mart_director_credits`
    - X-axis terms: `director_name`
    - Y-axis terms: `dop_name`
    - Metric: `Max(movie_count)`

5. **Movie credits completeness (donut)**
    - Index: `mart_movie_credits`
    - Slice by scripted field or runtime field:
      - `has_director = directors_nconsts != null`
      - `has_dop = dop_nconsts != null`
    - Metric: `Count`

### Dashboard 3: TV episode performance and people analytics

1. **Episode ratings by season (line)**
    - Index: `mart_episode_enriched`
    - X-axis: `season_number`
    - Metric: `Average(average_rating)`
    - Break down: `series_title` (Top N)

2. **Episode vote momentum by season (line)**
    - Index: `mart_episode_enriched`
    - X-axis: `season_number`
    - Metric: `Sum(num_votes)`
    - Break down: `series_title` (Top N)

3. **Episode volume by series (bar)**
    - Index: `mart_episode_enriched`
    - Terms: `series_title`
    - Metric: `Count`

4. **Credit type mix for TV episodes (stacked bar or donut)**
    - Index: `mart_episode_credits`
    - Bucket: `credit_type`
    - Metric: `Count`
    - Optional split: `series_title`

5. **Most recurring people per series (table/bar)**
    - Index: `mart_series_people_rollup`
    - Terms: `person_name` (or split by `credit_type`)
    - Metric: `Max(episode_count)`
    - Optional filter: one `series_tconst`

6. **People impact (bubble)**
    - Index: `mart_series_people_rollup`
    - X-axis: `episode_count`
    - Y-axis: `avg_episode_rating`
    - Bubble size: `total_episode_votes`
    - Break down: `credit_type`

### Dashboard 4: Localization and international footprint

1. **AKA coverage by region (treemap/bar)**
    - Index: `mart_series_akas`
    - Terms: `region`
    - Metric: `Sum(aka_count)`

2. **AKA coverage by language (treemap/bar)**
    - Index: `mart_series_akas`
    - Terms: `language`
    - Metric: `Sum(aka_count)`

3. **Series with broadest localization (table)**
    - Index: `mart_series_akas`
    - Group by: `series_title`
    - Metrics: `Cardinality(region)`, `Cardinality(language)`, `Sum(aka_count)`

### Aggregation notes (important)

- `mart_director_credits` grain is `director + dop` pair, so use `Max(movie_count)` / `Max(total_votes)` for director-level charts (not `Sum`, which can overcount).
- `mart_episode_credits` grain is `episode + person + credit_type`; for episode-level totals, prefer `Cardinality(episode_tconst)`.
- `mart_series_people_rollup` is already aggregated; use it for leaderboard and impact visuals instead of recomputing from `mart_episode_credits`.

### Optional first dashboard layout

If you want one starting dashboard quickly, use 8 panels:

1. Ratings bucket by era (`mart_titles_enriched`)
2. Movie releases over time (`mart_titles_enriched`)
3. Top movies by votes (`mart_titles_enriched`)
4. Director productivity (`mart_director_credits`)
5. Director quality vs scale (`mart_director_credits`)
6. Episode ratings by season (`mart_episode_enriched`)
7. Credit type mix (`mart_episode_credits`)
8. AKA coverage by region (`mart_series_akas`)

### Kibana KQL snippets (copy/paste)

Use these in the Kibana query bar for each visualization.

#### `mart_titles_enriched`

- Movies only:
    - `title_type : "movie"`
- TV only:
    - `title_type : ("tvSeries" or "tvMiniSeries")`
- Rated titles with sufficient signal:
    - `average_rating >= 6 and num_votes >= 1000`
- High-confidence hits:
    - `average_rating >= 8 and num_votes >= 10000`
- Recent era cut:
    - `era : ("2010s" or "2020s+")`

#### `mart_movie_credits`

- Movies with director credits:
    - `directors_nconsts : *`
- Movies missing director credits:
    - `not directors_nconsts : *`
- Movies with DoP credits:
    - `dop_nconsts : *`
- Movies missing DoP credits:
    - `not dop_nconsts : *`
- Strongly rated + popular movies:
    - `average_rating >= 7 and num_votes >= 5000`

#### `mart_director_credits`

- Director rows with known DoP relationship:
    - `dop_nconst : *`
- Director rows without DoP relationship:
    - `not dop_nconst : *`
- Productive directors:
    - `movie_count >= 5`
- Productive and well-rated directors:
    - `movie_count >= 5 and avg_rating >= 7`
- Audience-heavy collaborations:
    - `total_votes >= 100000`

#### `mart_episode_enriched`

- Rated episodes only:
    - `average_rating : * and num_votes : *`
- High-performing episodes:
    - `average_rating >= 8 and num_votes >= 1000`
- Episodes with broad AKA footprint:
    - `episode_aka_count >= 5`
- Seasonal cuts:
    - `season_number >= 1`

#### `mart_episode_credits`

- Directors only:
    - `credit_type : "director"`
- Writers only:
    - `credit_type : "writer"`
- Actors/actresses:
    - `credit_type : ("actor" or "actress")`
- Crew with job metadata:
    - `job : *`
- Episode people on strong episodes:
    - `average_rating >= 8 and num_votes >= 1000`

#### `mart_series_people_rollup`

- Core recurring contributors:
    - `episode_count >= 10`
- Long-running contributors:
    - `season_count >= 3`
- High-impact contributors:
    - `episode_count >= 10 and avg_episode_rating >= 7.5`
- Director-only rollups:
    - `credit_type : "director"`

#### `mart_series_akas`

- Known region and language:
    - `region : * and language : *`
- Strong localization pockets:
    - `aka_count >= 3`
- US English subset:
    - `region : "US" and language : "en"`

### Lens formula helpers (optional)

Use these as Lens Formula metrics when helpful:

- Director rows with DoP coverage (%):
    - `count(kql='dop_nconst : *') / count()`
- Movies with DoP coverage (%):
    - `count(kql='dop_nconsts : *') / count()`
- Movies with director coverage (%):
    - `count(kql='directors_nconsts : *') / count()`
- High-rated episode share (%):
    - `count(kql='average_rating >= 8') / count()`
- Highly localized episode-row share (%):
    - `count(kql='aka_count >= 3') / count()`

### Suggested dashboard controls (quick wins)

Add these global controls to make the dashboards interactive:

- `series_title` (options list)
- `credit_type` (options list)
- `start_year` or `season_number` (range slider)
- `average_rating` and `num_votes` (range sliders)

### Saved searches to create first (starter checklist)

Create these 8 Discover saved searches first, then build the 8-panel starter dashboard from them.

1. **Movies by era + rating bucket (for stacked bar)**
    - Data view: `mart_titles_enriched`
    - KQL: `title_type : "movie"`
    - Keep fields: `era`, `rating_bucket`, `start_year`, `average_rating`, `num_votes`

2. **Movie release trend seed (for line chart)**
    - Data view: `mart_titles_enriched`
    - KQL: `title_type : "movie" and start_year >= 1900`
    - Keep fields: `start_year`, `primary_title`, `average_rating`, `num_votes`

3. **Top-voted movies seed (for table)**
    - Data view: `mart_titles_enriched`
    - KQL: `title_type : "movie" and num_votes : *`
    - Sort in Discover: `num_votes` descending
    - Keep fields: `primary_title`, `start_year`, `average_rating`, `num_votes`, `rating_bucket`

4. **Director productivity seed (for horizontal bar)**
    - Data view: `mart_director_credits`
    - KQL: `director_name : * and movie_count >= 1`
    - Keep fields: `director_name`, `movie_count`, `avg_rating`, `total_votes`
    - Lens metric note: use `Max(movie_count)`

5. **Director quality/scale seed (for scatter)**
    - Data view: `mart_director_credits`
    - KQL: `director_name : * and avg_rating : * and total_votes >= 10000`
    - Keep fields: `director_name`, `movie_count`, `avg_rating`, `total_votes`, `dop_name`
    - Lens metric note: use `Max(movie_count)` and `Max(total_votes)`

6. **Episode rating trend seed (for line by season)**
    - Data view: `mart_episode_enriched`
    - KQL: `average_rating : * and season_number >= 1`
    - Keep fields: `series_title`, `season_number`, `episode_number`, `average_rating`, `num_votes`

7. **Episode credit mix seed (for donut/stacked bar)**
    - Data view: `mart_episode_credits`
    - KQL: `credit_type : *`
    - Keep fields: `series_title`, `episode_tconst`, `credit_type`, `person_name`
    - Lens metric note: for episode totals use `Cardinality(episode_tconst)`

8. **Localization by region seed (for treemap/bar)**
    - Data view: `mart_series_akas`
    - KQL: `region : * and aka_count >= 1`
    - Keep fields: `series_title`, `region`, `language`, `aka_count`

Optional naming convention for saved searches:

- `kibana_seed_01_movies_era_bucket`
- `kibana_seed_02_movies_release_trend`
- `kibana_seed_03_movies_top_votes`
- `kibana_seed_04_director_productivity`
- `kibana_seed_05_director_quality_scale`
- `kibana_seed_06_episode_rating_trend`
- `kibana_seed_07_episode_credit_mix`
- `kibana_seed_08_localization_region`

### Kibana build order (mini-runbook)

Use this order to build the starter dashboard quickly and consistently.

1. Create data views
    - Stack Management → Data Views → Create one per mart index pattern.
    - Start with: `mart_titles_enriched`, `mart_director_credits`, `mart_episode_enriched`, `mart_episode_credits`, `mart_series_akas`.

2. Create the 8 Discover saved searches
    - Open Discover, select the target data view, paste the matching KQL from the checklist above.
    - Keep only the listed fields, apply sort when noted, and Save each search with the suggested name.

3. Create Lens visualizations from each saved search
    - Open Lens from each saved search context so filters are inherited.
    - Configure chart type and metrics as listed in the starter dashboard + aggregation notes.
    - Save each Lens panel with the same numeric prefix as its seed search.

4. Assemble the dashboard
    - Dashboard → Create dashboard → Add all 8 saved Lens panels.
    - Suggested layout:
      - Row 1: panels 1, 2, 3
      - Row 2: panels 4, 5
      - Row 3: panels 6, 7, 8

5. Add global controls
    - Add controls for `series_title`, `credit_type`, `start_year/season_number`, and `average_rating/num_votes`.
    - Save as a base dashboard (for example: `movies_tv_starter_v1`).

6. Validate before sharing
    - Spot-check panel totals against Discover for the same filter.
    - Re-check director charts use `Max(movie_count)` / `Max(total_votes)` and episode totals use `Cardinality(episode_tconst)` where needed.

### Starter panel spec table (implementation sheet)

Use this as a one-page build sheet for the 8 starter panels.

| # | Panel name | Data view | Chart type | Dimension(s) | Metric(s) | Baseline KQL | Notes |
|---|---|---|---|---|---|---|---|
| 1 | Ratings bucket by era | `mart_titles_enriched` | Stacked bar | X: `era`; Breakdown: `rating_bucket` | `Count` | `title_type : "movie"` | Good top-left overview panel |
| 2 | Movie releases over time | `mart_titles_enriched` | Line | X: `start_year` (histogram) | `Count` | `title_type : "movie" and start_year >= 1900` | Use yearly interval |
| 3 | Top movies by votes | `mart_titles_enriched` | Table | Rows: `primary_title`, `start_year`, `rating_bucket` | `Max(num_votes)`, `Max(average_rating)` | `title_type : "movie" and num_votes : *` | Sort by `Max(num_votes)` desc |
| 4 | Director productivity | `mart_director_credits` | Horizontal bar | Terms: `director_name` | `Max(movie_count)` | `director_name : * and movie_count >= 1` | Do not use `Sum(movie_count)` |
| 5 | Director quality vs scale | `mart_director_credits` | Scatter/Bubble | X: `avg_rating`; Y: `movie_count`; Breakdown: `director_name` | Bubble size: `Max(total_votes)` | `director_name : * and avg_rating : * and total_votes >= 10000` | Use `Max(movie_count)` and `Max(total_votes)` |
| 6 | Episode ratings by season | `mart_episode_enriched` | Line | X: `season_number`; Breakdown: `series_title` (Top N) | `Average(average_rating)` | `average_rating : * and season_number >= 1` | Limit Top N series for readability |
| 7 | Credit type mix | `mart_episode_credits` | Donut or stacked bar | Bucket: `credit_type`; Optional split: `series_title` | `Count` or `Cardinality(episode_tconst)` | `credit_type : *` | Prefer `Cardinality(episode_tconst)` for episode-level totals |
| 8 | AKA coverage by region | `mart_series_akas` | Treemap or bar | Terms: `region` | `Sum(aka_count)` | `region : * and aka_count >= 1` | Pair with language panel when expanding |

Quick metric defaults:

- Use `Count` for raw row-volume panels.
- Use `Average(...)` for score/quality panels.
- Use `Max(...)` for `mart_director_credits` rollups.
- Use `Cardinality(episode_tconst)` when counting distinct episodes from `mart_episode_credits`.