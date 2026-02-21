import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# This tells Python to go find the .env file and open the lockbox
load_dotenv()

def load_data_to_db():
    print("📦 Step 1: Reading the CSV file...")
    df = pd.read_csv('luu_inbound_data.csv')
    
    print("🔐 Step 2: Fetching keys from the lockbox...")
    # We grab each piece of the puzzle securely from the .env file
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    print("🗺️ Step 3: Setting the GPS to Port 5433...")
    # We use f-strings to stitch the secure variables together into a URL
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(db_url)
    
    print("🚚 Step 4: Unloading data into Postgres...")
    df.to_sql('inbound_packages', engine, schema='raw', if_exists='append', index=False)
    
    print("✅ Delivery Complete! Data is now securely in the warehouse.")

if __name__ == "__main__":
    load_data_to_db()