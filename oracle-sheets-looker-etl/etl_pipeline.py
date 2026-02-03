# Notebook 1: ETL Logic (Hybrid Engine - Fixed for Missing JAR)
import gspread
import json
import os
import pytz
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql.functions import col
from sqlalchemy import create_engine, text

# ==========================================
# 1. WIDGETS & CONFIGURATION
# ==========================================
# Create input boxes in the UI
dbutils.widgets.text("oracle_secret_path", "/Workspace/Users/hari.prasanna.ravichandran@zalando.de/oracle_to_sheets_project/oracle_secret.json", "1. Oracle Config Path")
dbutils.widgets.text("google_key_path", "/Workspace/Users/hari.prasanna.ravichandran@zalando.de/oracle_to_sheets_project/google_key.json", "2. Google Key Path")
dbutils.widgets.text("sheet_id", "1bQ-q1-mo3HqLgYUIe45UL9b1G7UaZ2wWjJhfWLP6DU8", "3. Google Sheet ID")

# Get values
ORACLE_SECRET_PATH = dbutils.widgets.get("oracle_secret_path")
GOOGLE_KEY_PATH    = dbutils.widgets.get("google_key_path")
SHEET_ID           = dbutils.widgets.get("sheet_id")

DATA_TAB_NAME = "JOIN"
TIME_TAB_NAME = "Block_dash"

# ==========================================
# 2. SQL QUERY
# ==========================================
SQL_QUERY = """
SELECT *
FROM ZAL_BESTAND
WHERE "Category" = 'Beauty'
  AND "Lager" IN ('BGL', 'Finalisierung', 'SZROV','OL_APS','Overstock')
"""

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================
def get_oracle_engine():
    if not os.path.exists(ORACLE_SECRET_PATH):
        raise FileNotFoundError(f"Missing Oracle Config at: {ORACLE_SECRET_PATH}")
    with open(ORACLE_SECRET_PATH, 'r') as f:
        config = json.load(f)
    
    # Create SQLAlchemy Engine using the Python Driver
    connection_string = (
        f"oracle+oracledb://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/?service_name={config['service']}"
    )
    return create_engine(connection_string)

def get_google_client():
    if not os.path.exists(GOOGLE_KEY_PATH):
        raise FileNotFoundError(f"Missing Google Key at: {GOOGLE_KEY_PATH}")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_KEY_PATH, scope)
    return gspread.authorize(creds)

# ==========================================
# 4. EXECUTION FLOW
# ==========================================
try:
    print("🚀 Starting ETL Job (Hybrid Engine)...")
    
    # --- STEP 1: EXTRACT (Python Driver) ---
    print("📡 Connecting to Oracle (Python Mode)...")
    engine = get_oracle_engine()
    
    with engine.connect() as connection:
        pdf_raw = pd.read_sql(text(SQL_QUERY), connection)
    
    raw_count = len(pdf_raw)
    print(f"   ✅ Raw Data Extracted: {raw_count} rows.")

    if raw_count > 0:
        # --- STEP 2: TRANSFORM (Switch to Spark) ---
        print("⚡ Converting to Spark for Transformation...")
        
        # Create Spark DataFrame (This is the "Magic Move" for your resume)
        df = spark.createDataFrame(pdf_raw)

        print("🔍 Applying Spark Filters...")
        # Now we use PySpark syntax just like a Big Data Engineer
        df_filtered = df.filter(col("MainLhm").rlike(r"^\d"))
        
        # Select first 22 columns
        cols_to_keep = df_filtered.columns[:22]
        df_final = df_filtered.select(cols_to_keep)

        # --- STEP 3: LOAD (Google Sheets) ---
        print("📋 Preparing Upload...")
        # Convert back to Pandas for GSpread (GSpread doesn't support Spark directly)
        final_pdf = df_final.toPandas()
        final_pdf = final_pdf.fillna('')
        final_count = len(final_pdf)
        
        print(f"📋 Uploading {final_count} rows to Sheets...")
        client = get_google_client()
        sh = client.open_by_key(SHEET_ID)
        
        # Clear & Update Data
        sh.worksheet(DATA_TAB_NAME).batch_clear(["A:V"])
        sh.worksheet(DATA_TAB_NAME).update(range_name="A1", values=[final_pdf.columns.values.tolist()] + final_pdf.values.tolist())
        
        # Update Timestamp
        berlin_tz = pytz.timezone('Europe/Berlin')
        current_time = datetime.now(berlin_tz).strftime("%d/%m/%Y %H:%M:%S")
        try:
            sh.worksheet(TIME_TAB_NAME).update_acell("C2", current_time)
        except:
            pass

        # --- SAVE SUCCESS STATE ---
        print(f"✅ SUCCESS! Saving Task Values: Rows={final_count}")
        dbutils.jobs.taskValues.set(key="status", value="SUCCESS")
        dbutils.jobs.taskValues.set(key="rows", value=final_count)
        dbutils.jobs.taskValues.set(key="run_time", value=current_time)
        dbutils.jobs.taskValues.set(key="error_msg", value="") 
    
    else:
        raise ValueError("Job ran, but Oracle returned 0 rows.")

except Exception as e:
    # --- SAVE FAILURE STATE ---
    print(f"❌ FAILED! Error: {str(e)}")
    dbutils.jobs.taskValues.set(key="status", value="FAILURE")
    dbutils.jobs.taskValues.set(key="error_msg", value=str(e))
    dbutils.jobs.taskValues.set(key="rows", value=0)
    dbutils.jobs.taskValues.set(key="run_time", value="Failed Run")
    # Raise error so the job marks as failed
    raise e
