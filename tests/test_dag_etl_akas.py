import pytest

from dags import airflow_datasets
from dags import dag_etl_akas as module


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
        return (2,)

    def get_records(self, sql):
        self.get_records_calls.append(sql)
        return [("tt1", 1, "Example", "US", "en")]


def test_clean_value():
    """Ensure raw IMDb null marker is normalized while regular strings are preserved."""
    assert module.clean_value(r"\N") is None
    assert module.clean_value("hello") == "hello"


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("", None),
        (r"\N", None),
        (" 42 ", 42),
        ("abc", None),
    ],
)
def test_to_int_or_none(value, expected):
    """Verify integer parsing handles blanks, null markers, valid ints, and invalid strings."""
    assert module.to_int_or_none(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("", None),
        (r"\N", None),
        ("0", False),
        ("1", True),
        ("x", None),
    ],
)
def test_to_bool_or_none(value, expected):
    """Verify boolean parsing handles IMDb bool conventions and invalid values."""
    assert module.to_bool_or_none(value) == expected


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
    tsv_content = "\t".join(
        [
            "titleId",
            "ordering",
            "title",
            "region",
            "language",
            "types",
            "attributes",
            "isOriginalTitle",
        ]
    ) + "\n"
    tsv_content += "\t".join(
        ["tt0001", "1", "Movie Name", "US", "en", "imdbDisplay", "literal title", "1"]
    ) + "\n"
    tsv_content += "\t".join(
        ["tt0001", "2", "Nom du film", "FR", "fr", r"\N", r"\N", "0"]
    ) + "\n"
    tsv_content += "\t".join(
        ["tt0002", "3", "Bad Bool", "US", "en", r"\N", r"\N", "x"]
    ) + "\n"

    tsv_file = tmp_path / "title.akas.tsv"
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
    assert "ON CONFLICT (title_id, ordering) DO NOTHING" in sql
    assert len(rows) == 3
    assert rows[0] == (
        "tt0001",
        1,
        "Movie Name",
        "US",
        "en",
        "imdbDisplay",
        "literal title",
        True,
    )
    assert rows[1] == (
        "tt0001",
        2,
        "Nom du film",
        "FR",
        "fr",
        None,
        None,
        False,
    )
    assert rows[2] == (
        "tt0002",
        3,
        "Bad Bool",
        "US",
        "en",
        None,
        None,
        None,
    )


def test_verify_load_uses_queries(monkeypatch):
    """Confirm verification step runs count and sample queries against the target table."""
    created = {}

    def fake_hook_ctor(postgres_conn_id):
        hook = FakeHookVerify(postgres_conn_id)
        created["hook"] = hook
        return hook

    monkeypatch.setattr(module, "PostgresHook", fake_hook_ctor)

    module.verify_load()

    hook = created["hook"]
    assert hook.postgres_conn_id == module.CONN_ID
    assert len(hook.get_first_calls) == 1
    assert f"SELECT COUNT(*) FROM {module.TABLE}" in hook.get_first_calls[0]
    assert len(hook.get_records_calls) == 1
    assert f"FROM {module.TABLE} LIMIT 5" in hook.get_records_calls[0]


def test_dag_task_chain():
    """Assert DAG wiring enforces create_table -> extract_and_load -> verify_load order."""
    dag = module.dag
    assert dag.dag_id == "movies_akas_etl"

    create_task = dag.get_task("create_table")
    extract_task = dag.get_task("extract_and_load")
    verify_task = dag.get_task("verify_load")

    assert extract_task.task_id in create_task.downstream_task_ids
    assert verify_task.task_id in extract_task.downstream_task_ids


def test_extract_task_publishes_title_akas_dataset():
    """Ensure extract_and_load publishes the title_akas dataset event."""
    extract_task = module.dag.get_task("extract_and_load")
    outlet_uris = {dataset.uri for dataset in extract_task.outlets}
    assert airflow_datasets.TITLE_AKAS_DATASET.uri in outlet_uris
