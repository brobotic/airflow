from types import SimpleNamespace

from scripts import query_letterboxd_diary_mart as module


def _base_args(**overrides):
    base = {
        "query": "overview",
        "limit": 20,
        "year": None,
        "film_year": None,
        "film_name": None,
        "execution_mode": "direct",
        "service": "postgres_movies",
        "user": "movies_user",
        "db": "movies_db",
        "host": "127.0.0.1",
        "port": 5432,
        "password_env": "POSTGRES_PASSWORD",
        "compose_file": "postgres.yaml",
        "no_header": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_parse_args_supports_movie_entries(monkeypatch):
    monkeypatch.setattr(
        module.sys,
        "argv",
        [
            "query_letterboxd_diary_mart.py",
            "--query",
            "movie-entries",
            "--film-name",
            "Alien",
            "--film-year",
            "1979",
        ],
    )

    args = module.parse_args()

    assert args.query == "movie-entries"
    assert args.film_name == "Alien"
    assert args.film_year == 1979


def test_main_requires_film_name_for_movie_entries(monkeypatch):
    monkeypatch.setattr(module, "parse_args", lambda: _base_args(query="movie-entries"))

    exit_code = module.main()

    assert exit_code == 1


def test_main_builds_movie_entries_sql_with_escaped_title(monkeypatch):
    args = _base_args(
        query="movie-entries",
        film_name="Schindler's List",
        film_year=1993,
    )
    monkeypatch.setattr(module, "parse_args", lambda: args)

    captured = {}

    def fake_run_direct_query(*, args, sql, psql_flags):
        captured["sql"] = sql
        captured["psql_flags"] = psql_flags
        return 0

    monkeypatch.setattr(module, "run_direct_query", fake_run_direct_query)

    exit_code = module.main()

    assert exit_code == 0
    assert "lower(trim(film_name)) = lower(trim('Schindler''s List'))" in captured["sql"]
    assert "film_year = 1993" in captured["sql"]
    assert captured["psql_flags"] == ["-P", "pager=off"]
