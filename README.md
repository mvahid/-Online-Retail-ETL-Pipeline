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
requests, pytest, etc.<br/>

---
## 🖥️ Setup MySQL
CREATE DATABASE uci_online_retail
Ensure your scripts/config.py has the correct MySQL credentials.
---
▶️ Run the Full Pipeline
python scripts/main.py

🧰 CLI Options (via argparse)

python scripts/main.py --help
optional arguments:
  --input FILE          Custom dataset path
  --dry-run             Run transform only (no DB load)
  --only-clean          Skip extract/load steps
  --only-load           Load previously cleaned data
  --mode MODE           Load mode: full | incremental

📁 Output
Cleaned dataset: data/cleaned_*.xlsx
Logs: logs/retail_etl.log
Loaded table: sales_db.transactions

🧪 Testing
pytest tests/

Includes:
Data quality validation
Download fallback logic
Mocked database operations

👨‍💻 Author
Vahidahamad Maniyar
LinkedIn | GitHub
Aspiring Data Engineer • Power BI Expert • ETL Specialist

📄 License
This project is licensed under the MIT License.

---

## ✅ Next Steps

You can:

- Copy this into `README.md` at your project root.
- Adjust links (e.g. add GitHub repo link when ready).
- Add a `LICENSE` file (`MIT` recommended if open source).
- (Optional) Add project screenshots or architecture diagram for visual clarity.

Let me know if you want:
- A **badge-based README header** (test status, version, etc.)
- A **project summary paragraph** for your resume or LinkedIn
- A **Dockerfile** to containerize it

You're on 🔥 with this project — well done!

Download fallback logic
Mocked database operations
