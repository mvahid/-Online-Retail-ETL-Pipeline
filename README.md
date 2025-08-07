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

- [UCI Machine Learning Repository – Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/Online+Retail)  
- Data: Transactions from a UK-based online store between 2009–2011.

---

## 🧪 Getting Started

### 🔧 Requirements

```bash
pip install -r requirements.txt
---
```
## Packages used:

 - pandas
 - openpyxl
 - mysql-connector-python
 - requests
 - pytest
 - etc.

## 🛠️ Setup MySQL
```bash
CREATE DATABASE uci_online_retail;
```
## ▶️ Run the Full Pipeline
```bash
python scripts/main.py
```
## 🧰 CLI Options (via argparse)
```bash
python scripts/main.py --help
```
### Available options:
```bash
--input FILE          Custom dataset path
--dry-run             Run transform only (no DB load)
--only-clean          Skip extract/load steps
--only-load           Load previously cleaned data
--mode MODE           Load mode: full | incremental
```
## 📁 Output
 - Cleaned dataset: data/cleaned_*.xlsx
 - Logs: logs/retail_etl.log
 - Loaded table: sales_db.transactions
## 🧪 Testing
 - Run unit tests using:

```bash
python -m pytest scripts/tests/ -v
python -m pytest scripts/tests/test_cleaner.py -v
python -m pytest scripts/tests/test_database.py -v
python -m pytest scripts/tests/test_downloader.py -v
```

Includes tests for:
Data quality validation
Download fallback logic
Mocked database operations

## 👨‍💻 Author
  Vahidahamad Maniyar
  LinkedIn | GitHub
  Aspiring Data Engineer • Power BI Expert • ETL Specialist


📄 License
This project is licensed under the MIT License.



