```markdown
# StreamSight ETL Pipeline

An end-to-end, Airflow-orchestrated ETL pipeline that ingests, transforms,
and loads the "Most Streamed Spotify Songs 2024" dataset into a PostgreSQL
database for SQL-based analytics — fully containerized with Docker.

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Orchestration | Apache Airflow 2.9.1 |
| Transformation | Pandas |
| Database | PostgreSQL 15 |
| Containerization | Docker + Docker Compose |
| DB Driver | SQLAlchemy + psycopg2 |

## Architecture

```
CSV File → extract.py → transform.py → load.py → PostgreSQL 15

                                                       ↓
                                                       
                                 Airflow DAG (scheduled @daily)
                                 
                          extract_task → transform_task → load_task → verify_task
                          
                                                                           ↓
                                                                           
                                                                     queries.sql
```

## Project Structure

```
streamsight-etl-pipeline/
├── dags/
│   └── spotify_etl_dag.py       # Airflow DAG definition
├── logs/                        # Airflow task logs (git-ignored)
├── data/                        # CSV dataset (git-ignored)
├── extract.py                   # reads CSV into DataFrame
├── transform.py                 # cleans and standardizes data
├── load.py                      # loads into PostgreSQL
├── pipeline.py                  # manual pipeline runner (no Airflow)
├── queries.sql                  # SQL analytics queries
├── docker-compose.yml           # spins up Airflow + PostgreSQL
├── Dockerfile                   # ETL app container
├── .env.example                 # template for environment variables
└── requirements.txt
```

## DAG Overview

The Airflow DAG `spotify_etl_pipeline` runs on a `@daily` schedule with
3 automatic retries (2-minute delay) on any task failure.

```
extract_task → transform_task → load_task → verify_task
```

| Task | Operator | Description |
|---|---|---|
| extract_task | PythonOperator | Reads CSV into DataFrame, pushes to XCom |
| transform_task | PythonOperator | Applies 8 transformation steps, pushes to XCom |
| load_task | PythonOperator | Loads cleaned data into PostgreSQL in batches |
| verify_task | PythonOperator | Queries row count post-load as a data quality gate |

## ETL Stages

### Extract
Reads the raw CSV (4,600 records) into a pandas DataFrame.
Handles encoding and validates file existence before proceeding.

### Transform
Applies 8 transformation steps:
- Normalizes all column names to snake_case
- Drops fully duplicate rows
- Strips commas and converts 14 numeric columns to proper types
- Parses release dates to Python datetime objects
- Extracts release year as a separate integer column
- Fills missing numeric values with 0
- Drops rows missing track name or artist name
- Strips whitespace from all string columns

### Load
Loads the cleaned DataFrame into PostgreSQL using SQLAlchemy.
Inserts in batches of 500 rows using multi-row INSERT for performance.

### Verify
Queries the row count from PostgreSQL post-load.
Raises an error and marks the DAG run as failed if 0 rows are found.

## How to Run

### Prerequisites
- Docker Desktop installed and running

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/kashyapkakadiya/streamsight-etl-pipeline.git
cd streamsight-etl-pipeline

# 2. Create your .env file
cp .env.example .env
# Edit .env and fill in your credentials

# 3. Add your dataset
# Download from Kaggle and place in the data/ folder:
# data/Most Streamed Spotify Songs 2024.csv

# 4. Initialize Airflow (first time only)
docker-compose up airflow-init

# 5. Start all services
docker-compose up
```

### Access the Airflow UI

Open your browser at:
```
http://localhost:8080
```
Login with `admin` / `admin`.

Enable the `spotify_etl_pipeline` DAG and trigger it manually
using the ▶ button, or let it run on its daily schedule.

### Connect to the database

```bash
docker exec -it spotify_postgres psql -U airflow -d airflow
```

Then run any query from `queries.sql`.

### Stop

```bash
# Stop all containers
docker-compose down

# Stop and wipe all volumes
docker-compose down -v
```

## Sample Analytics Queries

```sql
-- Top 10 most streamed songs
SELECT track, artist, spotify_streams
FROM spotify_songs
ORDER BY spotify_streams DESC
LIMIT 10;

-- Top 10 artists by total streams
SELECT artist, SUM(spotify_streams) AS total_streams
FROM spotify_songs
GROUP BY artist
ORDER BY total_streams DESC
LIMIT 10;

-- Songs released per year
SELECT release_year, COUNT(*) AS total_songs
FROM spotify_songs
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year DESC;

-- Average streams by release year
SELECT release_year, ROUND(AVG(spotify_streams)) AS avg_streams
FROM spotify_songs
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year DESC;

-- Most cross-platform songs
SELECT track, artist, spotify_streams, youtube_views
FROM spotify_songs
WHERE spotify_streams > 0 AND youtube_views > 0
ORDER BY (spotify_streams + youtube_views) DESC
LIMIT 10;
```

## Environment Variables

Create a `.env` file in the root (use `.env.example` as template):

```
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=spotify_etl
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
AIRFLOW_UID=50000
```

Never commit your `.env` file — it is listed in `.gitignore`.

## Dataset

Source: [Most Streamed Spotify Songs 2024](https://www.kaggle.com/datasets/nelgiriyewithana/most-streamed-spotify-songs-2024)
Records: 4,600 | Size: 1.05 MB
```
