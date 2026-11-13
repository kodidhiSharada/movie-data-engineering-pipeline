"""Unit + integration tests for etl.py (no network: OMDb is replaced by a fake session).

Run with:  python -m pytest -q
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etl  # noqa: E402


# -----------------------------------------------------------------------------
# Fake OMDb API
# -----------------------------------------------------------------------------
class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload, self.status_code = payload, status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise etl.requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Answers by IMDb id ('i') or title ('t') from small in-memory tables."""

    def __init__(self, by_id=None, by_title=None, status_code=200, error=None):
        self.by_id, self.by_title = by_id or {}, by_title or {}
        self.status_code, self.error = status_code, error
        self.requests: list[dict] = []

    def get(self, url, params, timeout):
        self.requests.append(params)
        if self.error:
            return FakeResponse({"Response": "False", "Error": self.error}, self.status_code)
        record = self.by_id.get(params.get("i")) or self.by_title.get(params.get("t"))
        if record:
            return FakeResponse({"Response": "True", **record})
        return FakeResponse({"Response": "False", "Error": "Movie not found!"})


def make_client(session, max_calls=100):
    return etl.OmdbClient("test-key", max_calls=max_calls, delay=0, session=session)


def make_movie(**overrides):
    base = dict(movie_id=1, title="Toy Story", raw_title="Toy Story (1995)", release_year=1995,
                decade=1990, imdb_id="tt0114709", tmdb_id=862, genres=["Animation"], alt_titles=[])
    base.update(overrides)
    return etl.Movie(**base)


# -----------------------------------------------------------------------------
# Transform helpers
# -----------------------------------------------------------------------------
@pytest.mark.parametrize("raw, title, year, alts", [
    ("Toy Story (1995)", "Toy Story", 1995, []),
    ("Usual Suspects, The (1995)", "The Usual Suspects", 1995, []),
    ("Seven (a.k.a. Se7en) (1995)", "Seven", 1995, ["Se7en"]),
    ("City of Lost Children, The (Cité des enfants perdus, La) (1995)",
     "The City of Lost Children", 1995, ["La Cité des enfants perdus"]),
    ("Avventura, L' (1960)", "L'Avventura", 1960, []),
    ("Death Note: Desu nôto (2006–2007)", "Death Note: Desu nôto", 2006, []),
    ("Babylon 5", "Babylon 5", None, []),
    ("96 Minutes (2011) ", "96 Minutes", 2011, []),
    ("(500) Days of Summer (2009)", "(500) Days of Summer", 2009, []),
    ("Cry, the Beloved Country (1995)", "Cry, the Beloved Country", 1995, []),
])
def test_parse_title(raw, title, year, alts):
    parsed = etl.parse_title(raw)
    assert (parsed.title, parsed.year, parsed.alt_titles) == (title, year, alts)


def test_split_genres():
    assert etl.split_genres("Adventure|Animation|Adventure") == ["Adventure", "Animation"]
    assert etl.split_genres("(no genres listed)") == []
    assert etl.split_genres("") == []


@pytest.mark.parametrize("func, value, expected", [
    (etl.parse_money, "$191,796,233", 191796233),
    (etl.parse_money, "N/A", None),
    (etl.parse_runtime, "142 min", 142),
    (etl.parse_runtime, "N/A", None),
    (etl.parse_omdb_date, "22 Nov 1995", "1995-11-22"),
    (etl.parse_omdb_date, "N/A", None),
    (etl.parse_year, "2006–2007", 2006),
    (etl.parse_number, "1,234,567", 1234567),
    (etl.format_imdb_id, "0114709", "tt0114709"),
    (etl.format_imdb_id, "", None),
    (etl.decade_of, 1995, 1990),
    (etl.decade_of, None, None),
])
def test_value_parsers(func, value, expected):
    assert func(value) == expected


def test_split_people_removes_roles_and_duplicates():
    assert etl.split_people("Joel Coen, Ethan Coen (co-director), Joel Coen") == ["Joel Coen", "Ethan Coen"]
    assert etl.split_people("N/A") == []


def test_transform_ratings_validates_and_deduplicates():
    rows = [
        {"userId": "1", "movieId": "1", "rating": "4.0", "timestamp": "964982703"},
        {"userId": "1", "movieId": "1", "rating": "5.0", "timestamp": "964982800"},  # newer -> wins
        {"userId": "2", "movieId": "1", "rating": "7.0", "timestamp": "964982703"},  # invalid value
        {"userId": "3", "movieId": "1", "rating": "", "timestamp": "964982703"},     # missing
        {"userId": "4", "movieId": "999", "rating": "3.0", "timestamp": "964982703"},  # orphan
    ]
    assert etl.transform_ratings(rows, {1}) == [(1, 1, 5.0, "2000-07-30 18:46:40")]


def test_transform_omdb_record():
    record = etl.transform_omdb_record(1, {
        "Title": "Fargo", "Year": "1996", "Released": "05 Apr 1996", "Runtime": "98 min",
        "Director": "Joel Coen, Ethan Coen", "BoxOffice": "$24,611,975", "imdbRating": "8.1",
        "imdbVotes": "726,024", "Metascore": "N/A", "Plot": "N/A",
    }, "imdb_id")
    assert record["released_date"] == "1996-04-05"
    assert record["box_office_usd"] == 24611975
    assert record["imdb_votes"] == 726024
    assert record["metascore"] is None and record["plot"] is None
    assert record["directors"] == ["Joel Coen", "Ethan Coen"]


# -----------------------------------------------------------------------------
# API lookup strategy
# -----------------------------------------------------------------------------
def test_lookup_by_imdb_id_first():
    session = FakeSession(by_id={"tt0114709": {"Title": "Toy Story", "Year": "1995"}})
    result = etl.lookup_movie(make_client(session), make_movie())
    assert (result.status, result.match_method, result.api_calls) == ("found", "imdb_id", 1)


def test_lookup_falls_back_to_title():
    session = FakeSession(by_title={"Toy Story": {"Title": "Toy Story", "Year": "1995"}})
    result = etl.lookup_movie(make_client(session), make_movie(imdb_id="tt9999999"))
    assert (result.status, result.match_method) == ("found", "title_year")


def test_lookup_rejects_title_match_with_wrong_year():
    session = FakeSession(by_title={"Emma": {"Title": "Emma", "Year": "2020"}})
    result = etl.lookup_movie(make_client(session), make_movie(title="Emma", imdb_id=None, release_year=1996))
    assert result.status == "not_found"
    assert "year mismatch" in result.error


def test_lookup_uses_alternate_title():
    session = FakeSession(by_title={"Se7en": {"Title": "Se7en", "Year": "1995"}})
    movie = make_movie(title="Seven", imdb_id=None, alt_titles=["Se7en"])
    result = etl.lookup_movie(make_client(session), movie)
    assert (result.status, result.match_method) == ("found", "alt_title")


def test_lookup_not_found():
    result = etl.lookup_movie(make_client(FakeSession()), make_movie())
    assert result.status == "not_found" and result.api_calls == 3  # imdb, title+year, title


def test_invalid_key_is_fatal():
    session = FakeSession(status_code=401, error="Invalid API key!")
    with pytest.raises(etl.OmdbFatalError):
        etl.lookup_movie(make_client(session), make_movie())


def test_server_error_is_recorded_as_error():
    session = FakeSession(status_code=503, error="Service unavailable")
    result = etl.lookup_movie(make_client(session), make_movie())
    assert result.status == "error"


def test_budget_is_enforced():
    client = make_client(FakeSession(), max_calls=1)
    with pytest.raises(etl.ApiBudgetExhausted):
        etl.lookup_movie(client, make_movie())  # needs 3 calls, budget is 1


# -----------------------------------------------------------------------------
# End-to-end: tiny dataset, run twice -> same row counts, no repeated API calls
# -----------------------------------------------------------------------------
def write_dataset(folder: Path) -> None:
    folder.mkdir(parents=True)
    (folder / "movies.csv").write_text(
        "movieId,title,genres\n"
        "1,Toy Story (1995),Adventure|Animation|Children\n"
        "2,\"Big Lebowski, The (1998)\",Comedy|Crime\n"
        "3,Fargo (1996),Comedy|Crime|Drama|Thriller\n"
        "4,Unknown Film (2001),(no genres listed)\n", encoding="utf-8")
    (folder / "links.csv").write_text(
        "movieId,imdbId,tmdbId\n1,0114709,862\n2,0118715,115\n3,0116282,275\n4,9999999,\n",
        encoding="utf-8")
    (folder / "ratings.csv").write_text(
        "userId,movieId,rating,timestamp\n"
        "1,1,4.0,964982703\n1,2,5.0,964982703\n2,2,4.0,964982703\n"
        "2,3,4.5,964982703\n3,4,2.0,964982703\n", encoding="utf-8")


def table_counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        tables = ["movies", "genres", "movie_genres", "ratings", "movie_details",
                  "directors", "movie_directors", "omdb_responses"]
        return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}


def test_pipeline_end_to_end_is_idempotent(tmp_path):
    write_dataset(tmp_path / "data" / etl.MOVIELENS_FOLDER)
    session = FakeSession(by_id={
        "tt0114709": {"Title": "Toy Story", "Year": "1995", "Director": "John Lasseter"},
        "tt0118715": {"Title": "The Big Lebowski", "Year": "1998", "Director": "Joel Coen, Ethan Coen"},
        "tt0116282": {"Title": "Fargo", "Year": "1996", "Director": "Joel Coen, Ethan Coen"},
    })
    config = etl.Config(data_dir=tmp_path / "data", db_path=tmp_path / "test.db", omdb_api_key="x")

    etl.run_pipeline(config, client=make_client(session))
    first = table_counts(config.db_path)
    calls_after_first_run = len(session.requests)

    etl.run_pipeline(config, client=make_client(session))
    assert table_counts(config.db_path) == first  # idempotent: no duplicates
    assert len(session.requests) == calls_after_first_run  # cached: no new API calls

    assert first == {"movies": 4, "genres": 7, "movie_genres": 9, "ratings": 5,
                     "movie_details": 3, "directors": 3, "movie_directors": 5,
                     "omdb_responses": 4}

    with sqlite3.connect(config.db_path) as conn:
        top = conn.execute(
            "SELECT d.director_name, COUNT(*) FROM directors d "
            "JOIN movie_directors md USING (director_id) GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1"
        ).fetchone()
        title = conn.execute("SELECT title FROM movies WHERE movie_id = 2").fetchone()[0]
    assert top == ("Ethan Coen", 2)
    assert title == "The Big Lebowski"
