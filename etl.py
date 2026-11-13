#!/usr/bin/env python3
"""
etl.py - Movie data pipeline: MovieLens CSVs + OMDb API  ->  SQLite.

Pipeline steps
--------------
1. EXTRACT    Read movies.csv, ratings.csv (and links.csv for IMDb IDs) from the
              MovieLens "small" dataset; download it automatically if missing.
2. TRANSFORM  Clean titles, parse release year, split genres, derive decade,
              validate ratings, convert unix timestamps to datetimes.
3. LOAD       Upsert movies / genres / ratings into SQLite (idempotent).
4. EXTRACT    Call the OMDb API for every movie that has not been fetched yet
   (API)      (IMDb ID first, then title-based fallbacks). Raw responses are
              cached in the `omdb_responses` table, so the step is resumable
              and never calls the API twice for the same movie.
5. TRANSFORM  Parse OMDb fields (box office, runtime, dates, directors, ...)
   + LOAD     and rebuild movie_details / directors / movie_directors.

Usage
-----
    python etl.py                      # full run (reads OMDB_API_KEY from env / .env)
    python etl.py --max-api-calls 200  # limit API usage for this run
    python etl.py --skip-api           # CSV-only load, no API calls
    python etl.py --help
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sqlite3
import sys
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# Constants / configuration
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema.sql"
DEFAULT_DATA_DIR = BASE_DIR / "data"
DEFAULT_DB_PATH = BASE_DIR / "movies.db"

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
MOVIELENS_FOLDER = "ml-latest-small"
OMDB_URL = "https://www.omdbapi.com/"

NO_GENRES_MARKER = "(no genres listed)"
VALID_RATINGS = {x / 2 for x in range(1, 11)}  # 0.5, 1.0, ... 5.0
DEFAULT_MAX_API_CALLS = 950  # OMDb free tier allows 1,000 requests per day
COMMIT_EVERY = 50  # commit API results in small batches (crash-safe progress)

log = logging.getLogger("etl")


@dataclass
class Config:
    """Runtime settings, built from CLI arguments and environment variables."""

    data_dir: Path = DEFAULT_DATA_DIR
    db_path: Path = DEFAULT_DB_PATH
    omdb_api_key: str | None = None
    max_api_calls: int = DEFAULT_MAX_API_CALLS
    request_delay: float = 0.1
    skip_api: bool = False
    retry_not_found: bool = False


# =============================================================================
# Small parsing helpers (pure functions -> easy to unit test)
# =============================================================================
def to_int(value: Any) -> int | None:
    """'42' -> 42; '' / None / garbage -> None."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def clean_na(value: Any) -> str | None:
    """OMDb uses the literal string 'N/A' for missing values -> map to None."""
    if value is None:
        return None
    text = str(value).strip()
    return None if text in ("", "N/A") else text


def parse_money(value: Any) -> int | None:
    """'$191,796,233' -> 191796233."""
    text = clean_na(value)
    if text is None:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_number(value: Any) -> int | None:
    """'1,234,567' -> 1234567."""
    return parse_money(value)


def parse_runtime(value: Any) -> int | None:
    """'142 min' -> 142."""
    text = clean_na(value)
    match = re.search(r"\d+", text or "")
    return int(match.group()) if match else None


def parse_omdb_date(value: Any) -> str | None:
    """'22 Nov 1995' -> '1995-11-22' (ISO-8601 date string)."""
    text = clean_na(value)
    if text is None:
        return None
    try:
        return datetime.strptime(text, "%d %b %Y").date().isoformat()
    except ValueError:
        return None


def parse_year(value: Any) -> int | None:
    """'1995' -> 1995, '2006–2007' -> 2006 (series give a range)."""
    match = re.search(r"\d{4}", clean_na(value) or "")
    return int(match.group()) if match else None


def split_people(value: Any) -> list[str]:
    """'Joel Coen, Ethan Coen (co-director)' -> ['Joel Coen', 'Ethan Coen']."""
    text = clean_na(value)
    if text is None:
        return []
    names = []
    for part in text.split(","):
        name = re.sub(r"\s*\([^)]*\)\s*$", "", part).strip()  # drop role notes
        if name and name not in names:
            names.append(name)
    return names


def format_imdb_id(value: Any) -> str | None:
    """links.csv stores '0114709' -> OMDb expects 'tt0114709'."""
    digits = re.sub(r"[^\d]", "", str(value or ""))
    return f"tt{digits.zfill(7)}" if digits else None


def decade_of(year: int | None) -> int | None:
    return (year // 10) * 10 if year else None


def epoch_to_iso(ts: int) -> str:
    """Unix epoch seconds -> 'YYYY-MM-DD HH:MM:SS' (UTC)."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# -----------------------------------------------------------------------------
# Title parsing
# -----------------------------------------------------------------------------
# "(1995)" or a range "(2006–2007)" at the very end of the title
_YEAR_RE = re.compile(r"\((\d{4})(?:\s*[-–]\s*\d{0,4})?\)\s*$")
# Any trailing "( ... )" group, e.g. "(a.k.a. Se7en)" or "(Postino, Il)"
_TRAILING_PAREN_RE = re.compile(r"\s*\(([^()]*)\)\s*$")
# MovieLens stores "Usual Suspects, The" -> we want "The Usual Suspects"
_ARTICLE_RE = re.compile(
    r"^(?P<body>.+), (?P<article>The|A|An|La|Le|Les|L'|Il|El|Los|Las|Die|Der|Das|Den|Det|De)$"
)


def move_article(title: str) -> str:
    """'Usual Suspects, The' -> 'The Usual Suspects'; "Avventura, L'" -> "L'Avventura"."""
    match = _ARTICLE_RE.match(title.strip())
    if not match:
        return title.strip()
    article, body = match.group("article"), match.group("body")
    sep = "" if article.endswith("'") else " "
    return f"{article}{sep}{body}"


@dataclass
class ParsedTitle:
    title: str
    year: int | None
    alt_titles: list[str] = field(default_factory=list)


def parse_title(raw: str) -> ParsedTitle:
    """Split a MovieLens title into clean title, release year and alternate titles.

    'City of Lost Children, The (Cité des enfants perdus, La) (1995)'
        -> title='The City of Lost Children', year=1995,
           alt_titles=['La Cité des enfants perdus']
    """
    text = " ".join((raw or "").split())  # normalise whitespace (some titles end in a space)

    year = None
    match = _YEAR_RE.search(text)
    if match:
        year = int(match.group(1))
        text = text[: match.start()].strip()

    # Peel off trailing parenthesised alternate titles (a.k.a. / original language).
    alt_titles: list[str] = []
    while True:
        match = _TRAILING_PAREN_RE.search(text)
        if not match or match.start() == 0:
            break
        inner = re.sub(r"^a\.k\.a\.\s*", "", match.group(1).strip(), flags=re.IGNORECASE)
        if inner:
            alt_titles.insert(0, move_article(inner))
        text = text[: match.start()].strip()

    return ParsedTitle(title=move_article(text) or raw.strip(), year=year, alt_titles=alt_titles)


def split_genres(value: str | None) -> list[str]:
    """'Adventure|Animation|Children' -> ['Adventure', 'Animation', 'Children']."""
    if not value or value.strip() == NO_GENRES_MARKER:
        return []
    genres: list[str] = []
    for genre in value.split("|"):
        genre = genre.strip()
        if genre and genre != NO_GENRES_MARKER and genre not in genres:
            genres.append(genre)
    return genres


# =============================================================================
# EXTRACT - CSV files
# =============================================================================
def ensure_dataset(data_dir: Path) -> Path:
    """Return the MovieLens folder, downloading and unzipping it if needed."""
    folder = data_dir / MOVIELENS_FOLDER
    if (folder / "movies.csv").exists() and (folder / "ratings.csv").exists():
        return folder

    log.info("MovieLens dataset not found - downloading %s", MOVIELENS_URL)
    response = requests.get(MOVIELENS_URL, timeout=120)
    response.raise_for_status()
    folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        for name in ("movies.csv", "ratings.csv", "links.csv"):
            (folder / name).write_bytes(archive.read(f"{MOVIELENS_FOLDER}/{name}"))
    log.info("Dataset saved to %s", folder)
    return folder


def read_csv(path: Path) -> list[dict[str, str]]:
    """Read a CSV into a list of dicts (empty list if the file is optional and missing)."""
    if not path.exists():
        log.warning("File not found, skipping: %s", path)
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    log.info("Read %s rows from %s", f"{len(rows):,}", path.name)
    return rows


# =============================================================================
# TRANSFORM - CSV data
# =============================================================================
@dataclass
class Movie:
    movie_id: int
    title: str
    raw_title: str
    release_year: int | None
    decade: int | None
    imdb_id: str | None
    tmdb_id: int | None
    genres: list[str]
    alt_titles: list[str]


def transform_movies(movie_rows: Iterable[dict], link_rows: Iterable[dict]) -> list[Movie]:
    """Clean movies.csv and join the IMDb / TMDb IDs from links.csv."""
    links = {to_int(r.get("movieId")): r for r in link_rows}
    movies: dict[int, Movie] = {}
    skipped = 0

    for row in movie_rows:
        movie_id = to_int(row.get("movieId"))
        raw_title = (row.get("title") or "").strip()
        if movie_id is None or not raw_title:
            skipped += 1
            continue

        parsed = parse_title(raw_title)
        link = links.get(movie_id, {})
        movies[movie_id] = Movie(  # dict keyed by id -> duplicate ids keep the last row
            movie_id=movie_id,
            title=parsed.title,
            raw_title=raw_title,
            release_year=parsed.year,
            decade=decade_of(parsed.year),
            imdb_id=format_imdb_id(link.get("imdbId")),
            tmdb_id=to_int(link.get("tmdbId")),
            genres=split_genres(row.get("genres")),
            alt_titles=parsed.alt_titles,
        )

    # Data-quality check: two movieIds pointing at the same IMDb id would be a true
    # duplicate. (Note: 5 title+year pairs repeat in MovieLens, e.g. two different
    # "War of the Worlds (2005)" films - they have different IMDb ids, which is why
    # title+year is NOT used as a key and IMDb id is preferred for API lookups.)
    by_imdb: dict[str, list[int]] = {}
    for m in movies.values():
        if m.imdb_id:
            by_imdb.setdefault(m.imdb_id, []).append(m.movie_id)
    duplicates = {k: v for k, v in by_imdb.items() if len(v) > 1}
    if duplicates:
        log.warning("%s films appear under more than one movieId (same IMDb id): %s",
                    len(duplicates), duplicates)

    no_year = sum(1 for m in movies.values() if m.release_year is None)
    no_genre = sum(1 for m in movies.values() if not m.genres)
    log.info(
        "Movies: %s clean, %s skipped (bad id/title), %s without year, %s without genres",
        f"{len(movies):,}", skipped, no_year, no_genre,
    )
    return list(movies.values())


def transform_ratings(rating_rows: Iterable[dict], valid_movie_ids: set[int]) -> list[tuple]:
    """Validate ratings and convert timestamps. Returns (user_id, movie_id, rating, rated_at)."""
    latest: dict[tuple[int, int], tuple[int, float]] = {}
    invalid = orphans = 0

    for row in rating_rows:
        user_id = to_int(row.get("userId"))
        movie_id = to_int(row.get("movieId"))
        rating = to_float(row.get("rating"))
        ts = to_int(row.get("timestamp"))

        if None in (user_id, movie_id, rating, ts) or rating not in VALID_RATINGS:
            invalid += 1
            continue
        if movie_id not in valid_movie_ids:
            orphans += 1  # rating for a movie we do not know -> would break the FK
            continue

        key = (user_id, movie_id)
        if key not in latest or ts >= latest[key][0]:  # keep the most recent rating
            latest[key] = (ts, rating)

    ratings = [(u, m, r, epoch_to_iso(ts)) for (u, m), (ts, r) in latest.items()]
    log.info(
        "Ratings: %s clean, %s invalid, %s orphaned (unknown movie)",
        f"{len(ratings):,}", invalid, orphans,
    )
    return ratings


# =============================================================================
# TRANSFORM - OMDb response
# =============================================================================
def transform_omdb_record(movie_id: int, payload: dict, match_method: str | None) -> dict:
    """Map a raw OMDb JSON record to a clean `movie_details` row (+ director list)."""
    return {
        "movie_id": movie_id,
        "omdb_title": clean_na(payload.get("Title")),
        "omdb_year": parse_year(payload.get("Year")),
        "rated": clean_na(payload.get("Rated")),
        "released_date": parse_omdb_date(payload.get("Released")),
        "runtime_minutes": parse_runtime(payload.get("Runtime")),
        "plot": clean_na(payload.get("Plot")),
        "language": clean_na(payload.get("Language")),
        "country": clean_na(payload.get("Country")),
        "awards": clean_na(payload.get("Awards")),
        "imdb_rating": to_float(clean_na(payload.get("imdbRating"))),
        "imdb_votes": parse_number(payload.get("imdbVotes")),
        "metascore": to_int(clean_na(payload.get("Metascore"))),
        "box_office_usd": parse_money(payload.get("BoxOffice")),
        "omdb_type": clean_na(payload.get("Type")),
        "match_method": match_method,
        "directors": split_people(payload.get("Director")),
    }


# =============================================================================
# EXTRACT - OMDb API
# =============================================================================
class OmdbFatalError(RuntimeError):
    """Invalid API key or daily limit reached -> stop calling the API for this run."""


class ApiBudgetExhausted(RuntimeError):
    """The per-run --max-api-calls budget is used up."""


class OmdbClient:
    """Thin OMDb wrapper with retries, throttling and a call budget."""

    def __init__(self, api_key: str, max_calls: int, delay: float = 0.1,
                 timeout: float = 10, session: requests.Session | None = None):
        self.api_key = api_key
        self.max_calls = max_calls
        self.delay = delay
        self.timeout = timeout
        self.calls = 0
        self.session = session or self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        # Retry transient failures (429 / 5xx / connection errors) with exponential backoff.
        retry = Retry(total=3, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=("GET",), raise_on_status=False)
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        return session

    def get(self, **params: Any) -> dict:
        """One API request. Returns the JSON dict (Response 'True' or 'False')."""
        if self.calls >= self.max_calls:
            raise ApiBudgetExhausted(f"API call budget of {self.max_calls} reached")
        self.calls += 1
        if self.delay:
            time.sleep(self.delay)  # be polite to a free API

        response = self.session.get(
            OMDB_URL, params={**params, "apikey": self.api_key}, timeout=self.timeout
        )
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise requests.HTTPError(f"Non-JSON response (HTTP {response.status_code})")

        error = (data.get("Error") or "").lower()
        if data.get("Response") == "False" and ("api key" in error or "limit" in error):
            raise OmdbFatalError(data.get("Error"))
        response.raise_for_status()  # other 4xx/5xx -> recorded as 'error', retried next run
        return data


@dataclass
class LookupResult:
    status: str  # found | not_found | error
    match_method: str | None = None
    payload: dict | None = None
    error: str | None = None
    api_calls: int = 0


def year_matches(expected: int | None, omdb_year: Any, tolerance: int = 1) -> bool:
    """Guard against wrong title matches: years must agree (+/- 1 for festival vs release)."""
    found = parse_year(omdb_year)
    return expected is None or found is None or abs(found - expected) <= tolerance


def lookup_movie(client: OmdbClient, movie: Movie) -> LookupResult:
    """Find a movie in OMDb, trying the most reliable strategy first.

    1. IMDb ID from links.csv          (exact - succeeds for almost every movie)
    2. cleaned title + release year
    3. cleaned title only
    4. alternate titles (a.k.a. / original-language) + year
    Title matches are accepted only if the OMDb year is within +/-1 of ours.
    """
    attempts: list[tuple[str, dict]] = []
    if movie.imdb_id:
        attempts.append(("imdb_id", {"i": movie.imdb_id}))
    if movie.release_year:
        attempts.append(("title_year", {"t": movie.title, "y": movie.release_year}))
    attempts.append(("title", {"t": movie.title}))
    for alt in movie.alt_titles:
        attempts.append(("alt_title", {"t": alt, "y": movie.release_year} if movie.release_year else {"t": alt}))

    start_calls = client.calls
    last_error = None
    for method, params in attempts:
        try:
            data = client.get(**params)
        except (OmdbFatalError, ApiBudgetExhausted):
            raise
        except requests.RequestException as exc:  # network error after retries
            return LookupResult("error", error=str(exc)[:500], api_calls=client.calls - start_calls)

        if data.get("Response") == "True":
            if method != "imdb_id" and not year_matches(movie.release_year, data.get("Year")):
                last_error = f"{method}: year mismatch ({data.get('Year')} vs {movie.release_year})"
                continue
            return LookupResult("found", method, data, api_calls=client.calls - start_calls)
        last_error = f"{method}: {data.get('Error')}"

    return LookupResult("not_found", error=last_error, api_calls=client.calls - start_calls)


# =============================================================================
# LOAD - database helpers
# =============================================================================
def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def apply_schema(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    conn.executescript(schema_path.read_text(encoding="utf-8"))


def load_movies(conn: sqlite3.Connection, movies: list[Movie]) -> None:
    """Upsert movies, genres and the movie_genres bridge in ONE transaction."""
    with conn:  # commits on success, rolls back on any exception
        conn.executemany(
            """
            INSERT INTO movies (movie_id, title, raw_title, release_year, decade, imdb_id, tmdb_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (movie_id) DO UPDATE SET
                title        = excluded.title,
                raw_title    = excluded.raw_title,
                release_year = COALESCE(excluded.release_year, movies.release_year),
                decade       = COALESCE(excluded.decade, movies.decade),
                imdb_id      = excluded.imdb_id,
                tmdb_id      = excluded.tmdb_id
            """,
            [(m.movie_id, m.title, m.raw_title, m.release_year, m.decade, m.imdb_id, m.tmdb_id)
             for m in movies],
        )

        all_genres = sorted({g for m in movies for g in m.genres})
        conn.executemany("INSERT OR IGNORE INTO genres (genre_name) VALUES (?)",
                         [(g,) for g in all_genres])
        genre_ids = dict(conn.execute("SELECT genre_name, genre_id FROM genres"))

        # Rebuild the bridge rows of the movies being loaded, so a genre removed in
        # the source is removed here too (a pure INSERT OR IGNORE would leave it).
        conn.executemany("DELETE FROM movie_genres WHERE movie_id = ?",
                         [(m.movie_id,) for m in movies])
        conn.executemany(
            "INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?)",
            [(m.movie_id, genre_ids[g]) for m in movies for g in m.genres],
        )
    log.info("Loaded %s movies and %s genres", f"{len(movies):,}", len(all_genres))


def load_ratings(conn: sqlite3.Connection, ratings: list[tuple]) -> None:
    """Upsert ratings on the natural key (user_id, movie_id) -> no duplicates on re-run."""
    with conn:
        conn.executemany(
            """
            INSERT INTO ratings (user_id, movie_id, rating, rated_at) VALUES (?, ?, ?, ?)
            ON CONFLICT (user_id, movie_id) DO UPDATE SET
                rating   = excluded.rating,
                rated_at = excluded.rated_at
            """,
            ratings,
        )
    log.info("Loaded %s ratings", f"{len(ratings):,}")


def save_lookup(conn: sqlite3.Connection, movie_id: int, result: LookupResult) -> None:
    conn.execute(
        """
        INSERT INTO omdb_responses
            (movie_id, status, match_method, response_json, error_message, api_calls, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (movie_id) DO UPDATE SET
            status        = excluded.status,
            match_method  = excluded.match_method,
            response_json = excluded.response_json,
            error_message = excluded.error_message,
            api_calls     = omdb_responses.api_calls + excluded.api_calls,
            fetched_at    = excluded.fetched_at
        """,
        (movie_id, result.status, result.match_method,
         json.dumps(result.payload) if result.payload else None,
         result.error, result.api_calls, datetime.now(timezone.utc).isoformat(timespec="seconds")),
    )


def movies_needing_lookup(conn: sqlite3.Connection, movies: list[Movie],
                          retry_not_found: bool) -> list[Movie]:
    """Movies with no cached response yet (or a previous error), most-rated first.

    Ordering by popularity means that when the daily API quota runs out, the
    movies that matter most for the analytics are already enriched.
    """
    statuses = ("error", "not_found") if retry_not_found else ("error",)
    done = {
        row[0] for row in conn.execute(
            f"SELECT movie_id FROM omdb_responses WHERE status NOT IN ({','.join('?' * len(statuses))})",
            statuses,
        )
    }
    popularity = dict(conn.execute("SELECT movie_id, COUNT(*) FROM ratings GROUP BY movie_id"))
    pending = [m for m in movies if m.movie_id not in done]
    pending.sort(key=lambda m: (-popularity.get(m.movie_id, 0), m.movie_id))
    return pending


def enrich_from_omdb(conn: sqlite3.Connection, movies: list[Movie], client: OmdbClient,
                     retry_not_found: bool = False) -> dict[str, int]:
    """Call OMDb for every movie still missing details and cache each response."""
    pending = movies_needing_lookup(conn, movies, retry_not_found)
    log.info("OMDb: %s movies need a lookup (budget %s API calls this run)",
             f"{len(pending):,}", client.max_calls)

    counts = {"found": 0, "not_found": 0, "error": 0}
    try:
        for i, movie in enumerate(pending, start=1):
            result = lookup_movie(client, movie)
            save_lookup(conn, movie.movie_id, result)
            counts[result.status] += 1
            if result.status != "found":
                log.debug("Movie %s '%s' -> %s (%s)", movie.movie_id, movie.title,
                          result.status, result.error)
            if i % COMMIT_EVERY == 0:
                conn.commit()
                log.info("OMDb progress: %s/%s looked up, %s API calls used",
                         i, len(pending), client.calls)
    except ApiBudgetExhausted:
        log.warning("API budget reached - remaining movies will be fetched on the next run")
    except OmdbFatalError as exc:
        log.error("OMDb refused the request: %s - stopping API calls for this run", exc)
    finally:
        conn.commit()  # keep everything fetched so far

    log.info("OMDb run summary: %s found, %s not found, %s errors, %s API calls",
             counts["found"], counts["not_found"], counts["error"], client.calls)
    return counts


def load_movie_details(conn: sqlite3.Connection) -> int:
    """Rebuild movie_details / directors / movie_directors from the cached responses.

    A full rebuild from the cache is cheap (no API calls) and always consistent,
    which keeps this step idempotent.
    """
    rows = conn.execute(
        "SELECT movie_id, match_method, response_json FROM omdb_responses WHERE status = 'found'"
    ).fetchall()
    details = [transform_omdb_record(mid, json.loads(raw), method) for mid, method, raw in rows]

    columns = ["movie_id", "omdb_title", "omdb_year", "rated", "released_date", "runtime_minutes",
               "plot", "language", "country", "awards", "imdb_rating", "imdb_votes", "metascore",
               "box_office_usd", "omdb_type", "match_method"]
    with conn:
        conn.execute("DELETE FROM movie_directors")
        conn.execute("DELETE FROM movie_details")
        conn.executemany(
            f"INSERT INTO movie_details ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' * len(columns))})",
            [tuple(d[c] for c in columns) for d in details],
        )

        names = sorted({name for d in details for name in d["directors"]})
        conn.executemany("INSERT OR IGNORE INTO directors (director_name) VALUES (?)",
                         [(n,) for n in names])
        director_ids = dict(conn.execute("SELECT director_name, director_id FROM directors"))
        conn.executemany(
            "INSERT OR IGNORE INTO movie_directors (movie_id, director_id) VALUES (?, ?)",
            [(d["movie_id"], director_ids[n]) for d in details for n in d["directors"]],
        )
        conn.execute("DELETE FROM directors WHERE director_id NOT IN "
                     "(SELECT director_id FROM movie_directors)")

        # Back-fill the release year for titles that had none in MovieLens.
        conn.execute(
            """
            UPDATE movies
            SET release_year = (SELECT omdb_year FROM movie_details d WHERE d.movie_id = movies.movie_id),
                decade       = (SELECT (omdb_year / 10) * 10 FROM movie_details d WHERE d.movie_id = movies.movie_id)
            WHERE release_year IS NULL
              AND EXISTS (SELECT 1 FROM movie_details d
                          WHERE d.movie_id = movies.movie_id AND d.omdb_year IS NOT NULL)
            """
        )
    log.info("Loaded details for %s movies, %s directors", f"{len(details):,}", f"{len(names):,}")
    return len(details)


def log_summary(conn: sqlite3.Connection) -> None:
    """Print row counts and enrichment coverage - a quick data-quality check."""
    tables = ["movies", "genres", "movie_genres", "ratings", "movie_details",
              "directors", "movie_directors", "omdb_responses"]
    log.info("---------------- Load summary ----------------")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("  %-16s %10s rows", table, f"{count:,}")
    total = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0] or 1
    for status, count in conn.execute(
            "SELECT status, COUNT(*) FROM omdb_responses GROUP BY status ORDER BY status"):
        log.info("  OMDb %-11s %10s (%.1f%% of movies)", status, f"{count:,}", 100 * count / total)
    not_fetched = conn.execute(
        "SELECT COUNT(*) FROM movies WHERE movie_id NOT IN (SELECT movie_id FROM omdb_responses)"
    ).fetchone()[0]
    if not_fetched:
        log.info("  OMDb not yet fetched %s - run etl.py again to continue", f"{not_fetched:,}")


# =============================================================================
# Orchestration
# =============================================================================
def run_pipeline(config: Config, client: OmdbClient | None = None) -> None:
    started = time.perf_counter()

    # ---- Extract (CSV) ----
    folder = ensure_dataset(config.data_dir)
    movie_rows = read_csv(folder / "movies.csv")
    rating_rows = read_csv(folder / "ratings.csv")
    link_rows = read_csv(folder / "links.csv")  # optional: provides IMDb IDs for exact API matches

    # ---- Transform (CSV) ----
    movies = transform_movies(movie_rows, link_rows)
    ratings = transform_ratings(rating_rows, {m.movie_id for m in movies})

    # ---- Load (CSV) ----
    conn = connect(config.db_path)
    try:
        apply_schema(conn)
        load_movies(conn, movies)
        load_ratings(conn, ratings)

        # ---- Extract (API) ----
        if config.skip_api:
            log.info("--skip-api set: not calling OMDb")
        elif client is None and not config.omdb_api_key:
            log.warning("OMDB_API_KEY is not set - skipping enrichment. "
                        "Get a free key at https://www.omdbapi.com/apikey.aspx")
        else:
            client = client or OmdbClient(config.omdb_api_key, config.max_api_calls,
                                          delay=config.request_delay)
            enrich_from_omdb(conn, movies, client, config.retry_not_found)

        # ---- Transform + Load (API) - always rebuilt from the cache ----
        load_movie_details(conn)
        log_summary(conn)
    finally:
        conn.close()

    log.info("Pipeline finished in %.1f s -> %s", time.perf_counter() - started, config.db_path)


def load_dotenv(path: Path) -> None:
    """Minimal .env reader (KEY=VALUE lines) so the API key never lives in code."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def parse_args(argv: list[str] | None = None) -> Config:
    parser = argparse.ArgumentParser(description="MovieLens + OMDb -> SQLite ETL pipeline")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR,
                        help="folder holding (or receiving) the MovieLens dataset")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database file")
    parser.add_argument("--max-api-calls", type=int,
                        default=int(os.environ.get("OMDB_MAX_CALLS", DEFAULT_MAX_API_CALLS)),
                        help="maximum OMDb requests for this run (free tier: 1,000/day)")
    parser.add_argument("--delay", type=float, default=0.1, help="seconds between API calls")
    parser.add_argument("--skip-api", action="store_true", help="load CSVs only, no API calls")
    parser.add_argument("--retry-not-found", action="store_true",
                        help="look up again movies that previously returned 'not found'")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return Config(
        data_dir=args.data_dir,
        db_path=args.db,
        omdb_api_key=os.environ.get("OMDB_API_KEY"),
        max_api_calls=args.max_api_calls,
        request_delay=args.delay,
        skip_api=args.skip_api,
        retry_not_found=args.retry_not_found,
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv(BASE_DIR / ".env")
    config = parse_args(argv)
    try:
        run_pipeline(config)
    except Exception:  # top-level guard: log the full traceback, exit non-zero
        log.exception("Pipeline failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
