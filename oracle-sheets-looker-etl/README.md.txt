# Oracle to Google Sheets Automated ETL Pipeline

## 🚀 Project Context: The Challenge
**Situation:** Warehouse operations teams were spending **20–40 minutes per shift** manually downloading >70MB reports from the TGW Infosystem (OLAP portal).

**The Problem:** * **Operational Waste:** Manual data retrieval was repetitive and error-prone.
* **Technical Limits:** Importing these large files (>70MB) into Google Sheets caused severe performance lags and frequently exceeded Google AppScript execution limits.
* **Latency:** Critical dashboards were often stale, delaying decision-making.

## 💡 The Solution
I developed a **Databricks-based ETL solution** using Python that bypasses the manual export process entirely.

* **Direct Access:** Leveraged Oracle DB credentials to query raw inventory data directly.
* **Cloud Processing:** Offloaded data transformation to Databricks (Data Lab) clusters, removing the load from Google Sheets.
* **Automation:** Configured automated Cron jobs to run precisely at shift starts (05:00 and 15:00) to ensure fresh data for incoming teams.
* **Integration:** Pushes processed, lightweight data to Google Sheets to power live **Google Looker Studio** dashboards.

## 📈 Key Results & Business Impact
* **🚀 Performance:** Drastically reduced total data update time from **~20 minutes to under 1 minute**, ensuring immediate availability.
* **⚡ Efficiency:** Eliminated **100% of manual data entry** and file handling, removing human error and operational wait times.
* **🛡️ Reliability:** Solved performance bottlenecks by handling transformations upstream in Python rather than in the spreadsheet.
* **🎯 Critical Operations:** Enabled the **DG Monitor** to reflect real-time data, allowing the team to strictly maintain **Ludwigsfelde Warehouse (LUU)** thresholds below 20 Liters.
* **✅ Transparency:** Implemented automated timestamping on dashboards, giving stakeholders confidence in data freshness for purchasing decisions.

## 🛠️ Technical Implementation
### Core Technologies
* **Python 3.9+** (Logic & Transformation)
* **Databricks Workflows** (Production Scheduling & Compute)
* **Oracle SQL** (Data Extraction)
* **Google Sheets API** (Data Loading)

### Key Features
* **Smart Filtering:** Implements Regex-based filtering (`^\d`) in Python to clean dataset before upload.
* **Zero-Downtime Updates:** Uses `batch_clear(["A:V"])` to wipe raw data without breaking downstream formulas/charts in columns W+.
* **Timezone-Awareness:** Automatically adjusts execution logs and timestamps for `Europe/Berlin` (CET/CEST).
* **Chat Ops:** Integrated Google Chat Webhooks to send "Success/Failure" alerts to the team channel instantly.

## ⚙️ Setup & Configuration
1.  **Environment:** Requires a Databricks Workspace with access to a whitelisted cluster (e.g., `luu-qm-cluster`).
2.  **Credentials:** Place `oracle_secret.json` and `google_key.json` in the secure workspace file path.
3.  **Scheduling:** Deploy using Databricks Jobs with Cron syntax for shift starts:
    ```cron
    0 0 5,15 * * ?  # Runs daily at 05:00 and 15:00 Berlin Time
    ```

---
*This project represents a shift from manual, legacy reporting to modern, automated Data Engineering practices.*