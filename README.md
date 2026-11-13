

# Movie Data Engineering Pipeline | MovieLens + OMDb + SQLite

## Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Python, the MovieLens dataset, and the OMDb API.

The pipeline extracts movie and rating data from MovieLens, transforms and cleans the data, enriches movie information using the OMDb API, and loads the results into a normalized SQLite database.

The project also includes analytical SQL queries to explore movie ratings, genres, release years, and directors.

### Key Features

- Automated MovieLens dataset download and extraction.
- Data cleaning and transformation using Python.
- Movie enrichment through the OMDb API.
- API response caching to avoid unnecessary requests.
- Resumable ETL processing with API request limits.
- Normalized relational database design.
- SQL-based movie analytics.
- Unit and integration tests using pytest.
- Idempotent database loading.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | ETL development and data processing |
| SQLite | Relational database |
| SQL | Data analysis and querying |
| OMDb API | Movie information enrichment |
| MovieLens | Source movie and rating dataset |
| Requests | API communication |
| Pytest | Unit and integration testing |

## Project Structure

```text
Movie-Data-Engineering/
│
├── data/                    # Downloaded MovieLens dataset (not committed)
│
├── tests/
│   └── test_etl.py          # Unit and integration tests
│
├── etl.py                   # Main ETL pipeline
├── schema.sql               # Database schema
├── queries.sql              # Analytical SQL queries
├── run_queries.py           # Executes analytical queries
├── requirements.txt         # Python dependencies
├── .env.example             # API key configuration template
├── .gitignore               # Excluded files
└── README.md                # Project documentation
```

## Data Sources

### 1. MovieLens Dataset

Source: [MovieLens Small Dataset](https://grouplens.org/datasets/movielens/latest/)

The project uses the MovieLens small dataset.

| Dataset | Records |
|---|---:|
| Movies | 9,742 |
| Ratings | 100,836 |
| Movie links | 9,742 |
| Genres | 19 |

The dataset provides movie titles, release years, genres, user ratings, timestamps, IMDb IDs, and TMDb IDs.

### 2. OMDb API

Source: [OMDb API](https://www.omdbapi.com/)

The OMDb API enriches movie records with additional information, including:

- Movie title and release year
- Director
- Actors
- Plot
- Runtime
- IMDb rating
- Box office collection
- Release date

The API response is stored as raw JSON in the database for caching and future reprocessing.

## ETL Pipeline Architecture

```text
                 MovieLens Dataset
                        |
                        v
                 Extract CSV Files
                        |
                        v
              Transform Movie Data
             (Clean Titles, Years, Genres)
                        |
                        v
                 Validate Ratings
                        |
                        v
                  Load into SQLite
            (Movies, Genres, Ratings)
                        |
                        v
                     OMDb API
                        |
                        v
              Cache Raw API Responses
                 (omdb_responses)
                        |
                        v
             Transform OMDb Responses
                        |
                        v
               Load Enriched Tables
       (movie_details, directors, movie_directors)
                        |
                        v
                Analytical SQL Queries
                        |
                        v
                  Movie Analytics
                
                   
```

### 1. Extract

- Downloads the MovieLens dataset automatically if it is not available locally.
- Reads movies, ratings, and links CSV files.
- Retrieves IMDb identifiers for movie enrichment.

### 2. Transform

- Cleans and standardizes movie titles.
- Extracts release years and decades.
- Handles missing and invalid values.
- Validates rating values.
- Removes duplicate user-movie ratings.
- Checks for invalid movie references.
- Converts OMDb values into appropriate database formats.

### 3. Load

- Creates database tables using `schema.sql`.
- Loads movies, genres, movie-genre relationships, and ratings.
- Uses upsert operations to prevent duplicate records.
- Enriches movie information using OMDb responses.
- Stores raw API responses for caching and resumable processing.

## Database Design

The project uses a normalized relational database.

### Main Tables

| Table | Description |
|---|---|
| movies | Stores movie information |
| genres | Stores unique movie genres |
| movie_genres | Connects movies with genres |
| ratings | Stores user ratings |
| movie_details | Stores OMDb-enriched movie information |
| directors | Stores unique director names |
| movie_directors | Connects movies with directors |
| omdb_responses | Stores raw API responses and processing status |

### Database Relationships

- One movie can belong to multiple genres.
- One genre can be associated with multiple movies.
- One movie can have multiple directors.
- One director can direct multiple movies.
- Each user can rate multiple movies.
- Each movie can receive ratings from multiple users.
- Movie details are stored separately from the core movie table.

### Database Quality Checks

The database was validated using SQLite integrity and foreign-key checks.

| Validation | Result |
|---|---|
| Database integrity check | Passed (`ok`) |
| Foreign key validation | Passed (no violations) |
| Duplicate IMDb IDs | None found |
| Invalid rating records | None found |
| Orphan rating records | None found |

## Current Project Results

The following counts were verified in the SQLite database.

| Table | Records |
|---|---:|
| movies | 9,742 |
| ratings | 100,836 |
| genres | 19 |
| movie_genres | 22,050 |
| movie_details | 998 |
| directors | 519 |
| movie_directors | 1,102 |
| omdb_responses | 998 |

**OMDb enrichment status:** 998 movie responses have been cached so far. The remaining movies can be processed through subsequent ETL runs when the daily API quota becomes available.

The pipeline preserves previously fetched responses, allowing enrichment to continue without starting from scratch.

## Analytical SQL Queries

The project includes SQL queries to answer questions such as:

1. What are the highest-rated movies?
2. Which movies have the most user ratings?
3. Which genres have the highest average ratings?
4. How many movies were released each year?
5. Which movies have the highest ratings in each decade?
6. Which users have submitted the most ratings?
7. What is the distribution of movie ratings?
8. Which movies have no ratings?
9. How do MovieLens ratings compare with IMDb ratings?
10. Which directors have the highest average movie ratings?

### Sample Analytical Findings

- Film-Noir has an average rating of approximately 3.92.
- War has an average rating of approximately 3.81.
- Documentary has an average rating of approximately 3.80.
- With at least 50 ratings, *The Shawshank Redemption* has an average rating of approximately 4.429.

These findings are based on the current database and the relevant query conditions.

## Idempotency and API Caching

An important feature of this project is idempotent ETL processing.

Running the pipeline multiple times should not create duplicate records.

This is achieved through:

- Primary keys and unique constraints.
- SQLite upsert operations.
- Rebuilding selected relationship and enrichment tables transactionally.
- Storing OMDb responses in a cache table.
- Skipping previously processed API records.
- Saving progress in batches.

The API enrichment process is resumable, which is useful when working with daily API request limits.

## Challenges and Solutions

### 1. Movie title mismatches

MovieLens and OMDb sometimes use different movie titles.

**Solution:** Used IMDb IDs as the primary lookup method and implemented title-based fallback matching.

### 2. Duplicate movie titles

Different movies can have the same title and release year.

**Solution:** Used IMDb identifiers to distinguish movies and added year validation for title-based matching.

### 3. API request limitations

The free OMDb API plan has a daily request limit.

**Solution:** Implemented API caching, request limits, and resumable processing.

### 4. Missing and inconsistent values

OMDb responses may contain values such as `N/A`, `$1,234`, or `142 min`.

**Solution:** Created transformation functions to convert these values into appropriate database formats.

### 5. Maintaining data consistency

Repeated ETL execution can introduce duplicate records if loading is not handled correctly.

**Solution:** Used primary keys, foreign keys, upserts, transactions, and database validation.

## Installation and Setup

### Prerequisites

- Python 3.10 or later
- Git
- OMDb API key for movie enrichment

### Step 1: Clone the repository

```bash
git clone https://github.com/kodidhiSharada/movie-data-engineering-pipeline.git
cd movie-data-engineering-pipeline
```

### Step 2: Create a virtual environment

```powershell
python -m venv .venv
```

Activate it in Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### Step 3: Install dependencies

```powershell
pip install -r requirements.txt
```

### Step 4: Configure the OMDb API key

Copy the example environment file:

```powershell
Copy-Item .env.example .env
```

Open `.env` and add your API key:

```text
OMDB_API_KEY=5e8c1b29
OMDB_MAX_CALLS=950
```

Get a free API key from [OMDb API](https://www.omdbapi.com/apikey.aspx).

**Security:** Never upload your `.env` file or expose your API key on GitHub.

### Step 5: Run the ETL pipeline

```powershell
python etl.py
```

The pipeline automatically downloads the MovieLens dataset if it is not available locally.

### Step 6: Run analytical SQL queries

```powershell
python run_queries.py
```

### Step 7: Run tests

```powershell
python -m pytest -v
```

## Useful ETL Commands

| Command | Description |
|---|---|
| `python etl.py` | Runs the complete pipeline |
| `python etl.py --skip-api` | Loads MovieLens data without API enrichment |
| `python etl.py --max-api-calls 200` | Limits API calls for the run |
| `python etl.py --retry-not-found` | Retries previously unsuccessful movie lookups |
| `python etl.py -v` | Enables detailed logging |

## Future Improvements

- Implement workflow orchestration using Apache Airflow or Dagster.
- Move from SQLite to PostgreSQL or Snowflake.
- Store raw and transformed data in Azure Data Lake Storage.
- Implement Bronze, Silver, and Gold data layers.
- Use Azure Data Factory for pipeline orchestration.
- Add incremental data processing.
- Implement automated data quality checks.
- Add Docker support and CI/CD integration.
- Explore distributed processing using Apache Spark.

## Learning Outcomes

Through this project, I gained practical experience in:

- Building an end-to-end ETL pipeline using Python.
- Extracting data from CSV files and external APIs.
- Cleaning and transforming structured data.
- Designing normalized relational databases.
- Writing SQL queries for data analysis.
- Handling API limits, retries, and caching.
- Implementing idempotent and resumable data pipelines.
- Validating data integrity and testing ETL workflows.

## Author

**K. Sharada Reddy**

BTech Graduate | Aspiring Data Engineer

Skills: Python | SQL | ETL | SQLite | REST APIs | Data Processing
