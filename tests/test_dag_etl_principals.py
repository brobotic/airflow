from dags import airflow_datasets
from dags import dag_etl_principals as module


class FakeCursor:
    def __init__(self):
        self.executemany_calls = []
        self.closed = False

    def executemany(self, sql, params):
        self.executemany_calls.append((sql, params))

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


class FakeHookWithConn:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.conn = FakeConn()

    def get_conn(self):
        return self.conn


class FakeHookCreate:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.run_calls = []

    def run(self, sql):
        self.run_calls.append(sql)


class FakeHookVerify:
    def __init__(self, postgres_conn_id):
        self.postgres_conn_id = postgres_conn_id
        self.get_first_calls = []
        self.get_records_calls = []

    def get_first(self, sql):
        self.get_first_calls.append(sql)
        if "cinematographer" in sql or "director of photography" in sql:
            return (1,)
        return (2,)

    def get_records(self, sql):
        self.get_records_calls.append(sql)
        return [("tt1", 1, "nm1", "cinematographer", "director of photography")]


def test_normalize_imdb_null():
    """Ensure raw IMDb null marker is normalized while regular strings are preserved."""
    assert module.normalize_imdb_null(r"\N") is None
    assert module.normalize_imdb_null("hello") == "hello"


def test_to_int_or_none():
    """Check integer parsing behavior for valid and invalid values."""
    assert module.to_int_or_none("7") == 7
    assert module.to_int_or_none(r"\N") is None
    assert module.to_int_or_none("") is None
    assert module.to_int_or_none("abc") is None


def test_create_table_calls_hook_run(monkeypatch):
    """Check table creation uses the configured Postgres connection and executes one CREATE statement."""
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookCreate(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)

    module.create_table()

    hook = created["hook"]
    assert hook.postgres_conn_id == module.CONN_ID
    assert len(hook.run_calls) == 1
    assert f"CREATE TABLE IF NOT EXISTS {module.TABLE}" in hook.run_calls[0]


def test_extract_and_load_transforms_and_upserts(monkeypatch, tmp_path):
    """Validate TSV rows are transformed and upserted with expected values."""
    tsv_content = "\t".join(["tconst", "ordering", "nconst", "category", "job", "characters"]) + "\n"
    tsv_content += "\t".join(["tt0001", "1", "nm0001", "director", r"\N", r"\N"]) + "\n"
    tsv_content += "\t".join(["tt0001", "2", "nm0002", "cinematographer", "director of photography", r"\N"]) + "\n"

    tsv_file = tmp_path / "title.principals.tsv"
    tsv_file.write_text(tsv_content, encoding="utf-8")

    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookWithConn(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)
    monkeypatch.setattr(module, "TSV_PATH", str(tsv_file))

    module.extract_and_load()

    hook = created["hook"]
    cursor = hook.conn.cursor_obj

    assert hook.postgres_conn_id == module.CONN_ID
    assert hook.conn.commit_count == 1
    assert cursor.closed is True
    assert hook.conn.closed is True

    assert len(cursor.executemany_calls) == 1
    sql, rows = cursor.executemany_calls[0]

    assert f"INSERT INTO {module.TABLE}" in sql
    assert "ON CONFLICT (tconst, ordering) DO NOTHING" in sql
    assert len(rows) == 2
    assert rows[0] == ("tt0001", 1, "nm0001", "director", None, None)
    assert rows[1] == ("tt0001", 2, "nm0002", "cinematographer", "director of photography", None)


def test_verify_load_uses_queries(monkeypatch):
    """Confirm verification step runs expected count and sample queries."""
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookVerify(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)

    result = module.verify_load()

    hook = created["hook"]
    assert hook.postgres_conn_id == module.CONN_ID
    assert len(hook.get_first_calls) == 2
    assert f"SELECT COUNT(*) FROM {module.TABLE}" in hook.get_first_calls[0]
    assert "cinematographer" in hook.get_first_calls[1]
    assert len(hook.get_records_calls) == 1
    assert f"FROM {module.TABLE} LIMIT 5" in hook.get_records_calls[0]

    assert result["row_count"] == 2
    assert result["sample_count"] == 1
    assert result["dop_signal_count"] == 1


def test_dag_task_chain():
    """Assert DAG wiring enforces create_table -> extract_and_load -> verify_load order."""
    dag = module.dag
    assert dag.dag_id == "movies_principals_etl"

    create_task = dag.get_task("create_table")
    extract_task = dag.get_task("extract_and_load")
    verify_task = dag.get_task("verify_load")

    assert extract_task.task_id in create_task.downstream_task_ids
    assert verify_task.task_id in extract_task.downstream_task_ids


def test_failure_callback_exists():
    """Ensure DAG failure callback is configured."""
    dag = module.dag
    assert "on_failure_callback" in dag.default_args
    assert callable(dag.default_args["on_failure_callback"])


def test_extract_task_publishes_title_principals_dataset():
    """Ensure extract_and_load publishes the title_principals dataset event."""
    extract_task = module.dag.get_task("extract_and_load")
    outlet_uris = {dataset.uri for dataset in extract_task.outlets}
    assert airflow_datasets.TITLE_PRINCIPALS_DATASET.uri in outlet_uris
