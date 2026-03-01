import requests
import time
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import json

# ==========================================
# 0. SETUP
# ==========================================
print("🔐 Loading credentials...")
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# ==========================================
# 1. EXTRACT: Table 1 (Discover Movies)
# ==========================================
def fetch_base_movies(api_key, max_pages=5):
    """Fetches the base movies from 2025-2026."""
    url = "https://api.themoviedb.org/3/discover/movie"
    movies = []
    
    print(f"\n🚚 1. Fetching {max_pages} pages of base movies...")
    for page_num in range(1, max_pages + 1):
        params = {
            "api_key": api_key, "language": "en-US",
            "primary_release_date.gte": "2025-01-01",
            "primary_release_date.lte": "2026-12-31",
            "sort_by": "popularity.desc", "page": page_num 
        }
        res = requests.get(url, params=params)
        if res.status_code == 200:
            movies.extend(res.json().get("results", []))
            time.sleep(0.1)
    
    print(f"✅ Picked up {len(movies)} base movies!")
    return movies

# ==========================================
# 2. EXTRACT: Tables 2 & 3 (Details & Credits)
# ==========================================
def fetch_details_and_credits(api_key, movie_ids):
    """Loops through movie IDs to get Budgets and Actors."""
    details_list = []
    credits_list = []
    
    print(f"\n🕵️‍♂️ 2. Fetching Details & Credits for {len(movie_ids)} specific movies...")
    
    # We loop through every single ID we collected in step 1!
    for count, movie_id in enumerate(movie_ids, 1):
        
        # Print a progress update every 20 movies so we know it's not frozen
        if count % 20 == 0:
            print(f"   ...processed {count}/{len(movie_ids)} movies...")

        # -- Get Details (Budget, Revenue, Runtime) --
        det_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        det_res = requests.get(det_url, params={"api_key": api_key})
        if det_res.status_code == 200:
            details_list.append(det_res.json())

        # -- Get Credits (Cast & Crew) --
        cred_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"
        cred_res = requests.get(cred_url, params={"api_key": api_key})
        if cred_res.status_code == 200:
            credits_list.append(cred_res.json())

        time.sleep(0.1) # CRITICAL: Be polite to TMDB's servers
        
    print(f"✅ Collected details for {len(details_list)} movies!")
    print(f"✅ Collected credits for {len(credits_list)} movies!")
    
    return details_list, credits_list

# ==========================================
# 3. LOAD (The Universal Pandas Loader)
# ==========================================
def load_table_to_postgres(data_list, table_name):
    """A reusable function to load any list of dictionaries into a raw table."""
    if not data_list:
        return
        
    df = pd.DataFrame(data_list)
    
    # --- THE FIX: Shrink-wrapping dictionaries into text strings ---
    # We look at every column. If a cell contains a list or a dictionary, 
    # we convert it into a pure JSON text string so Postgres doesn't panic.
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    # ---------------------------------------------------------------

    # Safety check for duplicates if 'id' exists
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'])
        
    print(f"🔨 Loading {len(df)} rows into 'raw.{table_name}'...")
    
    # Pandas will automatically create the table in our 'raw' schema
    df.to_sql(table_name, engine, schema='raw', if_exists='replace', index=False)

# ==========================================
# 4. RUN THE PIPELINE
# ==========================================
if __name__ == "__main__":
    
    # Step 1: Get the base movies
    base_movies = fetch_base_movies(TMDB_API_KEY, max_pages=5)
    
    # Step 2: Extract just the IDs from those movies to power our loop
    movie_ids_to_fetch = [movie['id'] for movie in base_movies]
    
    # Step 3: Get the detailed data using those IDs
    movie_details, movie_credits = fetch_details_and_credits(TMDB_API_KEY, movie_ids_to_fetch)
    
    print("\n📦 3. Pushing everything to PostgreSQL...")
    
    # Step 4: Load all three tables into the raw schema!
    load_table_to_postgres(base_movies, "raw_movies")
    load_table_to_postgres(movie_details, "raw_movie_details")
    load_table_to_postgres(movie_credits, "raw_movie_credits")
    
    print("\n🎉 ALL DONE! Your raw database is fully loaded.")