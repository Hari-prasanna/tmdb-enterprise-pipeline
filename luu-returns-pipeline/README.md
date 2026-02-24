# Zalando LUU Returns Logistics Pipeline (Mock ELT)

## 📌 Project Overview
This project is an end-to-end ELT (Extract, Load, Transform) data pipeline simulating Zalando's Local Utility Unit (LUU) returns logistics network. It generates complex, hierarchical mock logistics data (items packed inside pallets), ingests it into a containerized data warehouse, and uses `dbt` to cleanse, test, and model the data into a production-ready Star Schema using a Medallion Architecture (Raw -> Staging -> Marts).

## 🏗️ Architecture & Tech Stack
* **Extraction (Python / Pandas / Faker):** Simulates a logistics network handling returns from Fulfillment Centers (FCs). It deliberately injects data anomalies (negative weights, logical timestamp errors, duplicates) to simulate real-world warehouse scanner glitches.
* **Loading (SQLAlchemy / PostgreSQL):** Extracts the mock CSV data and loads it into a `raw` schema inside a Dockerized PostgreSQL database.
* **Transformation & Modeling (dbt):** Cleanses the data, enforces business rules, handles deduplication, and builds a relational Star Schema ready for BI visualization.
* **Data Quality (dbt tests):** Automated testing enforces Primary Key uniqueness, Foreign Key referential integrity, and accepted value constraints.
* **Infrastructure (Docker Compose):** Provides an isolated, reproducible local environment without polluting the host OS.

## 📦 Data Lineage (Medallion Architecture)
1. **Bronze (`raw.inbound_packages`):** Untouched, raw data containing intentional warehouse scanner errors.
2. **Silver (`staging.stg_inbound_packages`):** A cleansed `view` where data types are cast, strings are standardized, logical time-travel errors are nullified, and duplicate scans are removed.
3. **Gold (`marts` Star Schema):** Business-level physical tables optimized for analytical querying:
   * **`dim_load_carriers`:** The Dimension table (1 row per pallet). Acts as the primary tracker for the physical pallet, including aggregated metrics like `total_items` and `total_weight_kg`.
   * **`fact_processed_items`:** The Fact table (1 row per item). Tracks individual item quality degradation and routing details, utilizing Foreign Keys to link back to the parent load carrier.

---

## 🚀 How to Run Locally

### 1. Environment Setup
Ensure Docker Desktop is running. Clone the repository and set up your environment variables:

```bash
# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install dependencies
pip install pandas sqlalchemy psycopg2-binary faker python-dotenv dbt-core dbt-postgres

# Set up local credentials
cp .env.example .env
```

### 2. Start the Infrastructure
Spin up the isolated PostgreSQL database (mapped to port 5433 to avoid local conflicts):

```bash
docker-compose up -d
```

### 3. Extract & Load
Generate the simulated logistics data and load it into the `raw` schema:

```bash
python generate_data.py
python load_data.py
```

### 4. Transform & Test (dbt)
Navigate to the dbt project folder and run the models and data tests to build the Staging and Marts layers:

```bash
cd luu_transformations

# Verify database connection
dbt debug

# Run staging (dev target)
dbt run

# Run production build (creates physical tables in the 'marts' schema)
dbt run --target prod

# Run data quality tests (Primary/Foreign keys, Not Null, Accepted Values)
dbt test --target prod
```