# 🛍️ Online Retail ETL Pipeline

A modular, production-grade ETL pipeline to extract, clean, validate, and load **UK-based online retail sales data** into a local MySQL database.

---

## 📦 Project Structure

- **scripts/**
  - `main.py` — Entry point: runs full pipeline via CLI
  - `config.py` — Configuration (DB creds, paths, URLs)
  - `cleaner.py` — Cleans and transforms the dataset
  - `downloader.py` — Downloads data with fallback logic
  - `database.py` — MySQL schema creation and data load
  - `schema.json` — Column validation rules
  - `validation.py` — Schema/type validation logic
- **tests/**
  - `test_cleaner.py` — Unit tests for cleaning logic
  - `test_downloader.py` — Tests for download fallback logic
  - `test_database.py` — Tests for database interactions
- **data/** — Raw and cleaned dataset files
- **logs/** — Execution and error logs


---

## 🚀 Features

- ✅ **Argparse-powered CLI**
- ✅ **Data cleaning with transformation tracking**
- ✅ **Schema validation** via `schema.json`
- ✅ **Incremental loading** based on invoice/timestamp
- ✅ **MySQL auto-schema generation**
- ✅ **Unit testing with `pytest`**
- ✅ **Rich logging (console + file)**

---

## 📊 Data Source

- **[UCI Machine Learning Repository – Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/Online+Retail)**  
- Data: Transactions from a UK-based online store between 2009–2011.

---

## 🧪 Getting Started

### 🔧 Requirements

```bash
pip install -r requirements.txt

Packages used:
pandas, openpyxl
mysql-connector-python
requests, pytest, etc.
< br/>
