# Zalando LUU Returns Logistics Pipeline (Mock ELT)

## 📌 Project Overview
This project is an end-to-end ELT (Extract, Load, Transform) data pipeline simulating Zalando's Local Utility Unit (LUU) returns logistics network. It generates mock logistics data, ingests it into a containerized data warehouse, and uses `dbt` to cleanse, test, and model the data using a Medallion Architecture (Raw -> Staging -> Marts).

## 🏗️ Architecture & Tech Stack
* **Extraction (Python / Pandas / Faker):** Simulates a logistics network handling returns from Fulfillment Centers (FCs). It deliberately injects data anomalies (negative weights, logical timestamp errors, duplicates) to simulate real-world warehouse scanner glitches.
* **Loading (SQLAlchemy / PostgreSQL):** Extracts the mock CSV data and loads it into a `raw` schema inside a Dockerized PostgreSQL database.
* **Transformation (dbt):** Cleanses the data, enforces business rules (e.g., package weight > 0), handles deduplication, and prepares the data for final analytical querying in the `marts` schema.
* **Infrastructure (Docker Compose):** Provides an isolated, reproducible local environment without polluting the host OS.

## 📦 Data Lineage (Medallion Architecture)
1. **Bronze (`raw.inbound_packages`):** Untouched, raw CSV data containing intentional warehouse scanner errors.
2. **Silver (`staging.stg_inbound_packages`):** Cleansed data where data types are cast, column names are standardized, and duplicates are removed.
3. **Gold (`marts.fct_returns_routing`):** Business-level tables ready for BI tools to analyze routing efficiency and defect rates.

## 🚀 How to Run Locally

### 1. Environment Setup
Ensure Docker Desktop is running. Clone the repository and set up your environment variables:
```bash
# Create a virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install pandas sqlalchemy psycopg2-binary faker python-dotenv dbt-core dbt-postgres

# Set up local credentials
cp .env.example .env

### 2. Start the Infrastructure
Spin up the isolated PostgreSQL database (mapped to port 5433 to avoid local conflicts):
```bash
docker-compose up -d

### 3. Extract & Load
Generate the simulated logistics data and load it into the `raw` schema:
```bash
python generate_data.py
python load_data.py

### 3. Transform (dbt)
Navigate to the dbt project and run the models and data tests:
```bash
cd luu_transformations
dbt debug
dbt run
dbt test
