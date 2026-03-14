import sys
from typing import Any, cast

from scripts import query_movie_credits as module


def test_parse_args_supports_actor(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["query_movie_credits.py", "--actor", "Toshiro Mifune"])

    args = module.parse_args()

    assert args.actor == "Toshiro Mifune"
    assert args.director is None


def test_query_movies_by_person_returns_actor_field_and_exact_matches(monkeypatch):
    captured = {}

    def fake_scan(*, client, index, query):
        captured["query"] = query
        yield {
            "_source": {
                "primary_title": "High and Low",
                "start_year": 1963,
                "directors_names": "Akira Kurosawa",
                "actors_names": "Toshiro Mifune, Tatsuya Nakadai",
                "dop_names": "Asakazu Nakai",
                "editor_names": "Reiko Kaneko",
                "composer_names": "Masaru Sato",
            }
        }
        yield {
            "_source": {
                "primary_title": "Partial Match",
                "start_year": 1970,
                "directors_names": "Director B",
                "actors_names": "Toshiro Mifune Jr.",
                "dop_names": "DoP B",
                "editor_names": "Editor B",
                "composer_names": "Composer B",
            }
        }

    monkeypatch.setattr(module, "scan", fake_scan)

    rows = module.query_movies_by_person(
        client=cast(Any, object()),
        index="mart_movie_credits",
        person_name="Toshiro Mifune",
        search_field="actors_names",
    )

    assert captured["query"]["_source"][2] == "directors_names"
    assert "actors_names" in captured["query"]["_source"]
    assert rows == [
        {
            "title": "High and Low",
            "year": 1963,
            "director": "Akira Kurosawa",
            "actor": "Toshiro Mifune, Tatsuya Nakadai",
            "dop": "Asakazu Nakai",
            "editor": "Reiko Kaneko",
            "composer": "Masaru Sato",
        }
    ]


def test_main_routes_actor_queries(monkeypatch):
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "director": None,
                "actor": "Toshiro Mifune",
                "cinematographer": None,
                "editor": None,
                "composer": None,
            },
        )(),
    )

    class FakeIndices:
        def exists(self, index):
            return True

    class FakeClient:
        def __init__(self):
            self.indices = FakeIndices()
            self.closed = False

        def close(self):
            self.closed = True

    captured = {}
    client = FakeClient()

    monkeypatch.setattr(module, "create_client", lambda: client)

    def fake_query_movies_by_person(*, client, index, person_name, search_field):
        captured["index"] = index
        captured["person_name"] = person_name
        captured["search_field"] = search_field
        return []

    monkeypatch.setattr(module, "query_movies_by_person", fake_query_movies_by_person)
    monkeypatch.setattr(
        module,
        "render_table",
        lambda rows, label, person_name: captured.update(
            {
                "rows": rows,
                "label": label,
                "render_person_name": person_name,
            }
        ),
    )

    exit_code = module.main()

    assert exit_code == 0
    assert captured["index"] == "mart_movie_credits"
    assert captured["person_name"] == "Toshiro Mifune"
    assert captured["search_field"] == "actors_names"
    assert captured["label"] == "actor"
    assert client.closed is True