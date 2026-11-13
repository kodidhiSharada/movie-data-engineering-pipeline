-- =============================================================================
-- queries.sql  -  Analytical questions on the movie database (SQLite)
--
-- Run all of them with:   python run_queries.py
-- Each question has a primary answer and, where the naive reading is
-- misleading, a more robust variant with the reasoning in comments.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1. Which movie has the highest average rating?
-- -----------------------------------------------------------------------------
-- Q1a. Literal answer (all tied movies)
-- Many movies are rated 5.0 by one or two users, so the literal
-- "highest average" is a tie between obscure movies. RANK() shows
-- every tied movie instead of picking one arbitrarily.
SELECT movie_id, title, release_year, num_ratings, avg_rating
FROM (
    SELECT s.*, RANK() OVER (ORDER BY s.avg_rating DESC) AS rnk
    FROM movie_rating_stats s
    WHERE s.num_ratings > 0
)
WHERE rnk = 1
ORDER BY num_ratings DESC, title
LIMIT 10;

-- Q1b. Recommended answer (movies with at least 50 ratings)
-- A minimum number of ratings makes the average statistically meaningful.
SELECT movie_id, title, release_year, num_ratings, avg_rating
FROM movie_rating_stats
WHERE num_ratings >= 50
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 5;

-- Q1c. Alternative: Bayesian / IMDb-style weighted rating
--   WR = (v / (v + m)) * R + (m / (v + m)) * C
--   R = movie average, v = number of ratings, C = global mean, m = 50 (prior weight)
-- Shrinks averages based on few ratings towards the global mean.
WITH global AS (
    SELECT AVG(rating) AS c FROM ratings
)
SELECT s.movie_id, s.title, s.num_ratings, s.avg_rating,
       ROUND((s.num_ratings * s.avg_rating + 50 * g.c) / (s.num_ratings + 50), 3) AS weighted_rating
FROM movie_rating_stats s
CROSS JOIN global g
WHERE s.num_ratings > 0
ORDER BY weighted_rating DESC
LIMIT 5;


-- -----------------------------------------------------------------------------
-- Q2. What are the top 5 movie genres that have the highest average rating?
-- -----------------------------------------------------------------------------
-- Average over all individual ratings of movies in each genre (each rating
-- counts once per genre of its movie). Movies without genres are excluded
-- because they have no row in movie_genres.
SELECT g.genre_name,
       COUNT(DISTINCT mg.movie_id) AS num_movies,
       COUNT(r.rating)             AS num_ratings,
       ROUND(AVG(r.rating), 3)     AS avg_rating
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.genre_id
JOIN ratings r       ON r.movie_id  = mg.movie_id
GROUP BY g.genre_id, g.genre_name
ORDER BY avg_rating DESC
LIMIT 5;

-- Q2b. Alternative: average of per-movie averages (movies with >= 10 ratings)
-- Every movie weighs the same regardless of how many ratings it received.
SELECT g.genre_name,
       COUNT(*)                     AS num_movies,
       ROUND(AVG(s.avg_rating), 3)  AS avg_of_movie_avgs
FROM genres g
JOIN movie_genres mg       ON mg.genre_id = g.genre_id
JOIN movie_rating_stats s  ON s.movie_id  = mg.movie_id
WHERE s.num_ratings >= 10
GROUP BY g.genre_id, g.genre_name
ORDER BY avg_of_movie_avgs DESC
LIMIT 5;


-- -----------------------------------------------------------------------------
-- Q3. Who is the director with the most movies in this dataset?
-- -----------------------------------------------------------------------------
-- Uses the normalised movie_directors bridge, so co-directed films
-- ("Joel Coen, Ethan Coen") count for each director. RANK() keeps ties.
-- Only movies successfully enriched from OMDb have director data.
SELECT director_name, num_movies, avg_rating_of_movies
FROM (
    SELECT d.director_name,
           COUNT(DISTINCT md.movie_id)  AS num_movies,
           ROUND(AVG(s.avg_rating), 3)  AS avg_rating_of_movies,
           RANK() OVER (ORDER BY COUNT(DISTINCT md.movie_id) DESC) AS rnk
    FROM directors d
    JOIN movie_directors md   ON md.director_id = d.director_id
    JOIN movie_rating_stats s ON s.movie_id     = md.movie_id
    GROUP BY d.director_id, d.director_name
)
WHERE rnk <= 5
ORDER BY num_movies DESC, director_name;


-- -----------------------------------------------------------------------------
-- Q4. What is the average rating of movies released each year?
-- -----------------------------------------------------------------------------
-- Average of all ratings given to movies released in that year. release_year
-- comes from the MovieLens title, back-filled from OMDb when it was missing.
SELECT m.release_year,
       COUNT(DISTINCT m.movie_id) AS num_movies,
       COUNT(r.rating)            AS num_ratings,
       ROUND(AVG(r.rating), 3)    AS avg_rating
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;


-- -----------------------------------------------------------------------------
-- Bonus: enrichment coverage (data-quality check)
-- -----------------------------------------------------------------------------
SELECT COALESCE(o.status, 'not fetched yet') AS omdb_status,
       COALESCE(o.match_method, '-')         AS match_method,
       COUNT(*)                              AS num_movies
FROM movies m
LEFT JOIN omdb_responses o ON o.movie_id = m.movie_id
GROUP BY 1, 2
ORDER BY num_movies DESC;

-- Bonus: average rating by decade (uses the engineered `decade` feature)
SELECT m.decade,
       COUNT(DISTINCT m.movie_id) AS num_movies,
       ROUND(AVG(r.rating), 3)    AS avg_rating
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
WHERE m.decade IS NOT NULL
GROUP BY m.decade
ORDER BY m.decade;
