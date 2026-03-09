from dags import airflow_datasets
from dags import dag_mart_letterboxd_diary as module


def test_dag_task_chain():
    dag = module.dag
    assert dag.dag_id == "mart_letterboxd_diary"

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


def test_dag_uses_letterboxd_diary_dataset_schedule():
    dag = module.dag
    dataset_condition = dag.timetable.dataset_condition
    schedule_dataset_uris = {uri for uri, _ in dataset_condition.iter_datasets()}
    assert airflow_datasets.LETTERBOXD_DIARY_DATASET.uri in schedule_dataset_uris


def test_extract_task_publishes_mart_dataset():
    extract_task = module.dag.get_task("extract_and_load")
    outlet_uris = {dataset.uri for dataset in extract_task.outlets}
    assert airflow_datasets.MART_LETTERBOXD_DIARY_DATASET.uri in outlet_uris
