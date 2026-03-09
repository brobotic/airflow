from typing import Any, cast

from dags import airflow_datasets
from dags import dag_mart_letterboxd_movie_matches as module


class FakeHook:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.get_first_calls = []

    def get_first(self, sql, parameters=None):
        self.get_first_calls.append((sql, parameters))
        if "information_schema.tables" in sql:
            return (1,)
        return (0,)


def test_table_exists_uses_information_schema():
    hook = FakeHook(module.CONN_ID)
    exists = module._table_exists(hook=cast(Any, hook), table_name="title_basics")

    assert exists is True
    sql, params = hook.get_first_calls[0]
    assert "information_schema.tables" in sql
    assert params == ("title_basics",)


def test_required_source_tables_include_letterboxd_and_titles():
    assert "letterboxd_diary" in module.REQUIRED_SOURCE_TABLES
    assert "title_basics" in module.REQUIRED_SOURCE_TABLES


def test_dag_task_chain_includes_elasticsearch_export():
    dag = module.dag
    assert dag.dag_id == "mart_letterboxd_movie_matches"

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
    dag = module.dag
    assert "on_failure_callback" in dag.default_args
    assert callable(dag.default_args["on_failure_callback"])


def test_dag_uses_source_datasets_as_schedule():
    dag = module.dag
    dataset_condition = dag.timetable.dataset_condition
    schedule_dataset_uris = {uri for uri, _ in dataset_condition.iter_datasets()}

    assert airflow_datasets.LETTERBOXD_DIARY_DATASET.uri in schedule_dataset_uris
    assert airflow_datasets.TITLE_BASICS_DATASET.uri in schedule_dataset_uris
    assert airflow_datasets.TITLE_RATINGS_DATASET.uri in schedule_dataset_uris


def test_extract_task_publishes_matches_dataset():
    extract_task = module.dag.get_task("extract_and_load")
    outlet_uris = {dataset.uri for dataset in extract_task.outlets}
    assert airflow_datasets.MART_LETTERBOXD_MOVIE_MATCHES_DATASET.uri in outlet_uris
