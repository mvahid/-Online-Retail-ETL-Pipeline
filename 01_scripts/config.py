"""

Configuration settings for the Online Retail ETL pipeline.

This module contains all configuration parameters including:
- Database connection settings
- Dataset download URLs
- Directory paths for data and logs

Example:
    To use the configuration:
    >>> from config import MYSQL_CONFIG, DATA_DIR
    >>> print(MYSQL_CONFIG['host'])
    'localhost'
"""

"""

Includes database, file paths, and schema configurations with versioning.
"""

"""Configuration with schema validation support."""

"""Configuration with complete schema and table definitions."""
from pathlib import Path
import json

# Database configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root@2025",
    "database": "uci_online_retail"
}

# Dataset configuration
DATASET_URLS = [
    "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
]
DATASET_FILENAME = "OnlineRetail.xlsx"

# Directory paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
LOG_FILENAME = "retail_etl.log"

# Schema configuration
SCHEMA_VERSION = "1.0"
TABLE_NAMES = {
    "products": "products",
    "customers": "customers",
    "transactions": "transactions"
}

# Schema definition
SCHEMA = {
    "required_columns": {
        "raw_data": ["InvoiceNo", "StockCode", "Description", "Quantity", 
                    "InvoiceDate", "UnitPrice", "CustomerID", "Country"],
        "clean_data": ["invoice", "stock_code", "description", "quantity",
                      "invoice_date", "price", "customer_id", "country"]
    },
    "column_types": {
        "invoice": "str",
        "stock_code": "str",
        "customer_id": "str",
        "invoice_date": "datetime64[ns]",
        "quantity": "int64",
        "price": "float64"
    }
}

# Save schema to JSON
SCHEMA_FILE = Path(__file__).parent / "schema.json"
with open(SCHEMA_FILE, 'w') as f:
    json.dump(SCHEMA, f, indent=2)