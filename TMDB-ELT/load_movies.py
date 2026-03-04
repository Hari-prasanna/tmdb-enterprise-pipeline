"""
TMDB Movie Data Pipeline
=========================
Extracts movie data from The Movie Database (TMDB) API and loads it into PostgreSQL.

Pipeline Steps:
1. Fetch base movie listings (discover endpoint)
2. Fetch detailed movie information and credits
3. Load all data into raw PostgreSQL tables
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine

# =============================================================================
# CONFIGURATION
# =============================================================================

TMDB_BASE_URL = "https://api.themoviedb.org/3"
RELEASE_DATE_START = "2026-01-01"
RELEASE_DATE_END = "2026-12-31"
DEFAULT_MAX_PAGES = 20
API_RATE_LIMIT_DELAY = 0.1
PROGRESS_LOG_INTERVAL = 20


def get_database_engine() -> Engine:
    """Create and return a SQLAlchemy database engine."""
    load_dotenv()

    db_config = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "name": os.getenv("DB_NAME"),
    }

    connection_url = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
    )

    return create_engine(connection_url)


def get_api_key() -> str:
    """Load and return the TMDB API key from environment variables."""
    load_dotenv()
    return os.getenv("TMDB_API_KEY")


# =============================================================================
# EXTRACT: Fetch Base Movies
# =============================================================================

def fetch_base_movies(api_key: str, max_pages: int = DEFAULT_MAX_PAGES) -> list[dict]:
    """
    Fetch base movie listings from TMDB discover endpoint.

    Args:
        api_key: TMDB API authentication key
        max_pages: Maximum number of pages to fetch (default: 20)

    Returns:
        List of movie dictionaries from the discover endpoint
    """
    url = f"{TMDB_BASE_URL}/discover/movie"
    movies = []

    print(f"\n🚚 Step 1: Fetching {max_pages} pages of base movies...")

    for page_num in range(1, max_pages + 1):
        params = {
            "api_key": api_key,
            "language": "en-US",
            "primary_release_date.gte": RELEASE_DATE_START,
            "primary_release_date.lte": RELEASE_DATE_END,
            "sort_by": "popularity.desc",
            "page": page_num,
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            movies.extend(response.json().get("results", []))

        time.sleep(API_RATE_LIMIT_DELAY)

    print(f"   ✅ Fetched {len(movies)} base movies")
    return movies


# =============================================================================
# EXTRACT: Fetch Movie Details & Credits
# =============================================================================

def fetch_movie_details(api_key: str, movie_id: int) -> dict | None:
    """Fetch detailed information for a single movie."""
    url = f"{TMDB_BASE_URL}/movie/{movie_id}"
    response = requests.get(url, params={"api_key": api_key})

    if response.status_code == 200:
        return response.json()
    return None


def fetch_movie_credits(api_key: str, movie_id: int) -> dict | None:
    """Fetch cast and crew credits for a single movie."""
    url = f"{TMDB_BASE_URL}/movie/{movie_id}/credits"
    response = requests.get(url, params={"api_key": api_key})

    if response.status_code == 200:
        return response.json()
    return None


def fetch_details_and_credits(
    api_key: str, movie_ids: list[int]
) -> tuple[list[dict], list[dict]]:
    """
    Fetch details and credits for multiple movies.

    Args:
        api_key: TMDB API authentication key
        movie_ids: List of movie IDs to fetch data for

    Returns:
        Tuple of (details_list, credits_list)
    """
    details_list = []
    credits_list = []
    total_movies = len(movie_ids)

    print(f"\n🕵️ Step 2: Fetching details & credits for {total_movies} movies...")

    for count, movie_id in enumerate(movie_ids, start=1):
        if count % PROGRESS_LOG_INTERVAL == 0:
            print(f"   Processing: {count}/{total_movies} movies...")

        details = fetch_movie_details(api_key, movie_id)
        if details:
            details_list.append(details)

        credits = fetch_movie_credits(api_key, movie_id)
        if credits:
            credits_list.append(credits)

        time.sleep(API_RATE_LIMIT_DELAY)

    print(f"   ✅ Collected {len(details_list)} movie details")
    print(f"   ✅ Collected {len(credits_list)} movie credits")

    return details_list, credits_list


# =============================================================================
# LOAD: Push Data to PostgreSQL
# =============================================================================

def serialize_complex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert dict and list columns to JSON strings."""
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    return df


def load_to_postgres(
    data: list[dict], table_name: str, engine: Engine
) -> None:
    """
    Load a list of dictionaries into a PostgreSQL raw table.

    Args:
        data: List of dictionaries to load
        table_name: Target table name in the 'raw' schema
        engine: SQLAlchemy database engine
    """
    if not data:
        print(f"   ⚠️  No data to load for '{table_name}'")
        return

    df = pd.DataFrame(data)
    df = serialize_complex_columns(df)
    df["loaded_at"] = datetime.now(timezone.utc)

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])

    print(f"   📥 Loading {len(df)} rows into 'raw.{table_name}'...")
    df.to_sql(table_name, engine, schema="raw", if_exists="append", index=False)


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_pipeline() -> None:
    """Execute the complete TMDB data extraction and loading pipeline."""
    print("🔐 Loading credentials...")
    api_key = get_api_key()
    engine = get_database_engine()

    # Extract base movies
    base_movies = fetch_base_movies(api_key, max_pages=DEFAULT_MAX_PAGES)
    movie_ids = [movie["id"] for movie in base_movies]

    # Extract details and credits
    movie_details, movie_credits = fetch_details_and_credits(api_key, movie_ids)

    # Load to PostgreSQL
    print("\n📦 Step 3: Loading data into PostgreSQL...")
    load_to_postgres(base_movies, "raw_movies", engine)
    load_to_postgres(movie_details, "raw_movie_details", engine)
    load_to_postgres(movie_credits, "raw_movie_credits", engine)

    print("\n🎉 Pipeline complete! All raw tables have been loaded.")


if __name__ == "__main__":
    run_pipeline()