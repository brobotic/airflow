from dags import dag_mart_director_credits as module


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


def test_principals_is_required_source_table():
    """Ensure title_principals is a hard dependency for this mart."""
    assert "title_principals" in module.REQUIRED_SOURCE_TABLES
    assert "title_principals" not in module.OPTIONAL_SOURCE_TABLES


def test_dag_task_chain_includes_elasticsearch_export():
    """Assert DAG wiring includes verify_load -> export_to_elasticsearch -> notify_discord."""
    dag = module.dag
    assert dag.dag_id == "mart_director_credits"

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
