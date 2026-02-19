import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. SETUP: Load Database Credentials & City List
# ---------------------------------------------------------
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "data_engineering")

# The "Map" for our Delivery Trucks
GERMAN_CITIES = {
    "BERLIN_MAIN": {"lat": 52.5200, "lon": 13.4050},
    "HAMBURG_MAIN": {"lat": 53.5511, "lon": 9.9937},
    "MUNICH_MAIN": {"lat": 48.1351, "lon": 11.5820},
    "COLOGNE_MAIN": {"lat": 50.9375, "lon": 6.9603},
    "FRANKFURT_MAIN": {"lat": 50.1109, "lon": 8.6821},
    "STUTTGART_MAIN": {"lat": 48.7758, "lon": 9.1829},
    "DUSSELDORF_MAIN": {"lat": 51.2277, "lon": 6.7735},
    "LEIPZIG_MAIN": {"lat": 51.3397, "lon": 12.3731},
    "DORTMUND_MAIN": {"lat": 51.5136, "lon": 7.4653},
    "ESSEN_MAIN": {"lat": 51.4556, "lon": 7.0116}
}

# ---------------------------------------------------------
# 2. EXTRACT: Fetch Data for ALL Cities
# ---------------------------------------------------------
def extract_weather_data(days_back=1):
    today = datetime.now()
    end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    print(f"📡 Fetching data from {start_date} to {end_date} for 10 cities...")
    
    # Create an empty box to hold the shipments from all 10 cities
    all_city_data = [] 

    # Loop through the map
    for station_id, coords in GERMAN_CITIES.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # We save BOTH the data AND the name of the city it belongs to
                all_city_data.append({
                    "station_id": station_id,
                    "data": response.json()
                })
        except Exception as e:
            print(f"🚨 Failed to fetch {station_id}: {e}")
            
    return all_city_data # Return the giant box of 10 shipments

# ---------------------------------------------------------
# 3. TRANSFORM: Format & Enrich ALL Cities
# ---------------------------------------------------------
def format_data(all_city_data):
    print("📦 Formatting and enriching data for all cities...")
    
    all_dataframes = [] # A list to hold 10 separate mini-tables
    
    # Unpack the giant box shipment by shipment
    for item in all_city_data:
        # 1. Make a mini-table for just this one city
        df = pd.DataFrame(item['data']['hourly'])
        # 2. Slap the correct station_id sticker on every row of this mini-table
        df['station_id'] = item['station_id'] 
        # 3. Add the mini-table to our pile
        all_dataframes.append(df)
        
    # 4. Mash all 10 mini-tables together into one massive table
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # 5. Format the time column for the database
    combined_df['time'] = pd.to_datetime(combined_df['time'])
    
    return combined_df

# ---------------------------------------------------------
# 4. LOAD: The "Bouncer" Upsert Logic
# ---------------------------------------------------------
def insert_on_conflict_update(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['station_id', 'time'],
        set_={
            'temperature_2m': stmt.excluded.temperature_2m,
            'relative_humidity_2m': stmt.excluded.relative_humidity_2m,
            'wind_speed_10m': stmt.excluded.wind_speed_10m
        }
    )
    conn.execute(stmt)

def load_to_postgres(df):
    print(f"🗄️ Sending {len(df)} rows to the Postgres Bouncer...")
    conn_str = f"postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)
    
    try:
        df.to_sql(name='weather_data', con=engine, schema='raw', if_exists='append', index=False, method=insert_on_conflict_update)
        print("✅ Success! Data Upserted gracefully.")
    except Exception as e:
        print(f"⚠️ Upsert failed: {e}")
        print("🔄 Retrying with basic append...")
        df.to_sql(name='weather_data', con=engine, schema='raw', if_exists='append', index=False)
        print("✅ Basic Load Success!")

# ---------------------------------------------------------
# 5. EXECUTE: The Main Pipeline
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting Weather ELT Pipeline...")
    
    # Let's run just 1 day (yesterday) for testing
    raw_data = extract_weather_data(days_back=1) 
    
    if raw_data and len(raw_data) > 0:
        df_formatted = format_data(raw_data)
        load_to_postgres(df_formatted)
        print("🎉 Pipeline finished successfully!")
    else:
        print("🛑 Shutting down safely. No data fetched.")