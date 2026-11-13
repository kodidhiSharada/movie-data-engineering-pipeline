-- =============================================================================
-- schema.sql  -  Movie analytics database (SQLite)
--
-- Design summary
--   * movies           one row per MovieLens movie (cleaned title, year, decade, IDs)
--   * genres           lookup table of distinct genre names
--   * movie_genres     bridge table  -> many-to-many movies <-> genres
--   * ratings          one row per (user, movie) rating  -> fact table
--   * movie_details    1:1 extension of movies with OMDb enrichment fields
--   * directors        lookup table of distinct director names
--   * movie_directors  bridge table  -> many-to-many movies <-> directors
--   * omdb_responses   raw API response cache / staging table (makes the API
--                      step resumable and avoids re-calling the API on re-runs)
--
-- Every statement uses IF NOT EXISTS so the script can be executed on every run
-- of etl.py without error (part of making the pipeline idempotent).
-- =============================================================================

PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------------
-- Core entity: movies (source: movies.csv + links.csv)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movies (
    movie_id      INTEGER PRIMARY KEY,             -- MovieLens movieId (natural key)
    title         TEXT    NOT NULL,                -- cleaned title, e.g. "The Usual Suspects"
    raw_title     TEXT    NOT NULL,                -- original string, e.g. "Usual Suspects, The (1995)"
    release_year  INTEGER,                         -- parsed from title, back-filled from OMDb when missing
    decade        INTEGER,                         -- feature: 1990 for 1990-1999
    imdb_id       TEXT,                            -- "tt0114814" (from links.csv)
    tmdb_id       INTEGER,                         -- from links.csv (nullable, 8 movies have none)
    CHECK (release_year IS NULL OR release_year BETWEEN 1870 AND 2100)
);

CREATE INDEX IF NOT EXISTS idx_movies_release_year ON movies (release_year);
CREATE INDEX IF NOT EXISTS idx_movies_imdb_id      ON movies (imdb_id);

-- -----------------------------------------------------------------------------
-- Genres: normalised out of the pipe-separated "genres" column
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS genres (
    genre_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id  INTEGER NOT NULL REFERENCES movies (movie_id) ON DELETE CASCADE,
    genre_id  INTEGER NOT NULL REFERENCES genres (genre_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_genres_genre ON movie_genres (genre_id);

-- -----------------------------------------------------------------------------
-- Ratings: fact table (source: ratings.csv)
-- A user rates a given movie at most once -> composite primary key.
-- There is no users table because the dataset holds no user attributes;
-- user_id is kept as a plain identifier.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ratings (
    user_id   INTEGER NOT NULL,
    movie_id  INTEGER NOT NULL REFERENCES movies (movie_id) ON DELETE CASCADE,
    rating    REAL    NOT NULL CHECK (rating BETWEEN 0.5 AND 5.0),
    rated_at  TEXT    NOT NULL,                    -- ISO-8601 UTC, converted from unix epoch
    PRIMARY KEY (user_id, movie_id)
);

CREATE INDEX IF NOT EXISTS idx_ratings_movie ON ratings (movie_id);

-- -----------------------------------------------------------------------------
-- OMDb enrichment: 1:1 with movies (only for movies found in the API)
-- Kept in a separate table so that:
--   * movies without an API match do not carry a dozen NULL columns
--   * enrichment can be rebuilt independently of the CSV load
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movie_details (
    movie_id         INTEGER PRIMARY KEY REFERENCES movies (movie_id) ON DELETE CASCADE,
    omdb_title       TEXT,
    omdb_year        INTEGER,
    rated            TEXT,                         -- MPAA rating, e.g. "PG-13"
    released_date    TEXT,                         -- ISO date "YYYY-MM-DD"
    runtime_minutes  INTEGER,
    plot             TEXT,
    language         TEXT,
    country          TEXT,
    awards           TEXT,
    imdb_rating      REAL,
    imdb_votes       INTEGER,
    metascore        INTEGER,
    box_office_usd   INTEGER,                      -- "$191,796,233" -> 191796233
    omdb_type        TEXT,                         -- movie / series / episode
    match_method     TEXT                          -- how the record was matched (imdb_id, title_year, ...)
);

-- -----------------------------------------------------------------------------
-- Directors: OMDb returns "Joel Coen, Ethan Coen" -> normalised many-to-many
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS directors (
    director_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    director_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id     INTEGER NOT NULL REFERENCES movies (movie_id) ON DELETE CASCADE,
    director_id  INTEGER NOT NULL REFERENCES directors (director_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, director_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_directors_director ON movie_directors (director_id);

-- -----------------------------------------------------------------------------
-- Staging / cache table for raw OMDb responses.
-- One row per movie. status:
--   'found'      -> response_json holds the matched record
--   'not_found'  -> every lookup strategy failed (not retried unless asked)
--   'error'      -> network / server error (retried automatically next run)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omdb_responses (
    movie_id       INTEGER PRIMARY KEY REFERENCES movies (movie_id) ON DELETE CASCADE,
    status         TEXT NOT NULL CHECK (status IN ('found', 'not_found', 'error')),
    match_method   TEXT,
    response_json  TEXT,
    error_message  TEXT,
    api_calls      INTEGER NOT NULL DEFAULT 0,
    fetched_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_omdb_responses_status ON omdb_responses (status);

-- -----------------------------------------------------------------------------
-- Convenience view used by the analytical queries
-- -----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS movie_rating_stats AS
SELECT
    m.movie_id,
    m.title,
    m.release_year,
    COUNT(r.rating)          AS num_ratings,
    ROUND(AVG(r.rating), 3)  AS avg_rating
FROM movies m
LEFT JOIN ratings r ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title, m.release_year;
