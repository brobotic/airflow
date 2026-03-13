from dags import airflow_datasets
from dags import dag_mart_movie_credits as module


class FakeHook:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.get_first_calls = []

    def get_first(self, sql, parameters=None):
        self.get_first_calls.append((sql, parameters))
        if "information_schema.tables" in sql:
            return (1,)
        return (0,)


def test_table_exists_uses_information_schema(monkeypatch):
    """Confirm helper checks table existence via information_schema with parameters."""
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHook(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)

    hook = module.PostgresHook(postgres_conn_id=module.CONN_ID)
    exists = module._table_exists(hook, "title_principals")

    assert exists is True
    sql, params = hook.get_first_calls[0]
    assert "information_schema.tables" in sql
    assert params == ("title_principals",)


def test_required_source_tables_include_movie_credits_dependencies():
    """Ensure required dependencies include crew, principals, and names."""
    assert "title_crew" in module.REQUIRED_SOURCE_TABLES
    assert "title_principals" in module.REQUIRED_SOURCE_TABLES
    assert "name_basics" in module.REQUIRED_SOURCE_TABLES


def test_dag_task_chain_includes_elasticsearch_export():
    """Assert DAG wiring includes verify_load -> export_to_elasticsearch -> notify_discord."""
    dag = module.dag
    assert dag.dag_id == "mart_movie_credits"

    create_task = dag.get_task("create_table")
    extract_task = dag.get_task("extract_and_load")
    verify_task = dag.get_task("verify_load")
    export_task = dag.get_task("export_to_elasticsearch")
    notify_task = dag.get_task("notify_discord")

    assert extract_task.task_id in create_task.downstream_task_ids
    assert verify_task.task_id in extract_task.downstream_task_ids
    assert export_task.task_id in verify_task.downstream_task_ids
    assert notify_task.task_id in export_task.downstream_task_ids


def test_failure_callback_exists():
    """Ensure DAG failure callback is configured."""
    dag = module.dag
    assert "on_failure_callback" in dag.default_args
    assert callable(dag.default_args["on_failure_callback"])


def test_dag_uses_source_datasets_as_schedule():
    """Ensure mart DAG is triggered by source datasets needed for movie credits."""
    dag = module.dag
    dataset_condition = dag.timetable.dataset_condition
    schedule_dataset_uris = {uri for uri, _ in dataset_condition.iter_datasets()}

    assert airflow_datasets.TITLE_BASICS_DATASET.uri in schedule_dataset_uris
    assert airflow_datasets.TITLE_CREW_DATASET.uri in schedule_dataset_uris
    assert airflow_datasets.NAME_BASICS_DATASET.uri in schedule_dataset_uris
    assert airflow_datasets.TITLE_PRINCIPALS_DATASET.uri in schedule_dataset_uris


class FakeHookCreateTable:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.run_calls = []

    def get_first(self, sql, parameters=None):
        if "information_schema.tables" in sql:
            return (1,)
        return (5,)

    def run(self, sql, parameters=None):
        self.run_calls.append((sql, parameters))


def test_create_table_adds_composer_columns(monkeypatch):
    """Ensure composer columns are created/added for forward-compatible deployments."""
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookCreateTable(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)
    module.create_table()

    run_sql = "\n".join(sql for sql, _ in created["hook"].run_calls)
    assert "composer_nconsts" in run_sql
    assert "composer_names" in run_sql


class FakeHookVerify:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id

    def get_first(self, sql, parameters=None):
        if "COUNT(*) FROM mart_movie_credits;" in sql:
            return (10,)
        if "directors_nconsts" in sql:
            return (8,)
        if "dop_nconsts" in sql:
            return (7,)
        if "editor_nconsts" in sql:
            return (6,)
        if "composer_nconsts" in sql:
            return (5,)
        return (0,)

    def get_records(self, sql, parameters=None):
        return [
            (
                "Movie A",
                2000,
                "Director A",
                "DoP A",
                "Editor A",
                "Composer A",
                8.2,
                12345,
            )
        ]


def test_verify_load_includes_composer_count(monkeypatch):
    """Ensure verify_load returns composer coverage metrics."""
    monkeypatch.setattr(module, "PostgresHook", FakeHookVerify)

    result = module.verify_load()

    assert result["row_count"] == 10
    assert result["with_director_count"] == 8
    assert result["with_dop_count"] == 7
    assert result["with_editor_count"] == 6
    assert result["with_composer_count"] == 5
