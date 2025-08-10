# 🏥 RCM Project – Revenue Cycle Management Data Pipeline

##  Overview
This project implements an end-to-end **ETL pipeline** for healthcare **Revenue Cycle Management (RCM)**.  
It extracts patient, provider, claims, and transactions data from multiple hospital databases, transforms and cleans it, implements **Slowly Changing Dimensions (SCD Type 2)** for patient history tracking, loads data into a **Star Schema** in **BigQuery**, and visualizes KPIs in a dashboard.

---

##  Project Structure

RCM_PROJECT/
│
├── claims/ # Raw claims data
│
├── config/ # Configuration files
│ ├── bigquery_key.json # BigQuery service account key
│ ├── db_config.py # MySQL DB connection settings
│
├── hospital_dbs/ # Hospital A & B databases
│ ├── hospital_a_db
│ ├── hospital_b_db
│
├── outputs/
│ ├── dim_outputs # Final dimension tables (CSV)
│ ├── extract_outputs # Raw extracted CSVs
│ ├── fact_outputs # Final fact tables (CSV)
│ ├── transformed_outputs # Transformed intermediate CSVs
│
├── scripts/
│ ├── bigquery_reload/ # BigQuery table reload scripts
│ │ ├── reload_all_tables.py
│ │
│ ├── data_extraction/ # Extract hospital data
│ │ ├── extract.py
│ │ ├── poor_patients.py
│ │
│ ├── data_transformation/ # Cleaning & transformation logic
│ │ ├── run_etl.py
│ │ ├── transform_logic.py
│ │
│ ├── dimensional_model/ # Star schema DDLs
│ │ ├── ddl.sql
│ │ ├── fact_transactions.py
│ │
│ ├── scd/ # SCD Type 2 for patients
│ │ ├── run_scd_patients.py
│ │ ├── scd_patients_loader.py
│ │ ├── phase4_cleaning.py
│ │ ├── bigquery_loader.py
│ │
│ ├── test_bigquery.py # Test BQ connection
│ ├── test_connection.py # Test DB connection
│ ├── test_csv.py # Validate CSV structure
│ ├── test_mysql.py # Test MySQL connection
│
├── venv/ # Virtual environment
│
├── extraction.log # ETL execution logs
├── RCM_Dashboard.pdf # Dashboard file
├── requirements.txt # Python dependencies
├── star schema.pdf # Star schema diagram


---

## 🚀 ETL Workflow

### **Phase 1 – Data Extraction**
- Connect to **Hospital A & Hospital B MySQL databases**
- Extract patient, provider, claim, and transaction data
- Save raw data in `outputs/extract_outputs/`

### **Phase 2 – Data Transformation**
- Standardize column names and formats
- Handle missing values, data types, and duplicates
- Store transformed files in `outputs/transformed_outputs/`

### **Phase 3 – Dimensional Model Creation**
- Design **Star Schema**:
  - Dimensions: `dim_patients`, `dim_providers`, `dim_procedures`, `dim_date`
  - Facts: `fact_transactions`, `fact_claims`
- Create tables in MySQL/BigQuery using `ddl.sql`

### **Phase 4 – Data Cleaning**
- Further cleaning before loading into dimensions/facts
- Save cleaned CSVs in `dim_outputs/` and `fact_outputs/`

### **Phase 5 – SCD Type 2 for Patients**
- Track historical changes for patient data (`address`, `phone`, `insurance`)
- Columns added:
  - `effective_date`
  - `expiry_date`
  - `is_current`
  - `version`
- Run with:
  ```bash
  python -m scripts.scd.run_scd_patients

### ** Phase 6 – Load into BigQuery **
Load cleaned dimensions and facts to BigQuery
- python scripts/bigquery_reload/reload_all_tables.py

### ** Phase 7 – Dashboard ***
-Build KPIs & insights in Looker Studio


