# Automated Retail Sales ETL & Data Validation Pipeline

An enterprise-grade batch ETL (Extract, Transform, Load) pipeline built in Python and SQLite designed to ingest raw transactional data, enforce rigorous data quality checks, handle missing/corrupted records, and load clean data into a structured data warehouse.

---

## 🛠️ Tech Stack
* **Language:** Python
* **Data Manipulation & Cleaning:** Pandas
* **Database / Storage:** SQLite (Local Data Warehouse)
* **Pipeline Architecture:** Object-Oriented Programming (OOP), Custom Logging, Exception Handling

---

## 🔄 Pipeline Architecture & Workflow

1. **Extract:** 
   * Ingests raw batch transactional data from source CSV files (`raw_retail_sales.csv`).
2. **Transform (Data Quality & Cleaning):**
   * **Imputation:** Handles missing customer names and uncategorized products.
   * **Schema Enforcement:** Casts data types securely (`pd.to_numeric`, `pd.to_datetime`) and drops unrecoverable corrupt date records.
   * **Feature Engineering:** Calculates total sales revenue (`quantity * price_per_unit`).
   * **Deduplication:** Removes duplicate records to ensure data integrity.
3. **Load:**
   * Establishes a structured relational table schema (`dim_retail_sales`) with primary keys and timestamps.
   * Performs high-efficiency bulk insertions into the SQLite Data Warehouse with error rollback support.

---

## 📁 Project File Structure
```text
Retail-Sales-ETL-Pipeline/
│
├── enterprise_etl_pipeline.py    # Main ETL pipeline script with OOP and logging
├── raw_retail_sales.csv          # Sample raw data source file
└── README.md                     # Project documentation
