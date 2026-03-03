from dags import dag_mart_titles_enriched as module


def test_dag_task_chain_includes_elasticsearch_export():
    """Assert DAG wiring includes verify_load -> export_to_elasticsearch -> notify_discord."""
    dag = module.dag
    assert dag.dag_id == "mart_titles_enriched"

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
