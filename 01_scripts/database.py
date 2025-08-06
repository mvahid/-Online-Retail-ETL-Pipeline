"""
Database operations for the ETL pipeline.

Provides functionality for:
- MySQL connection management
- Database table creation
- Data loading with batch processing

Example:
    >>> from database import get_connection, create_tables
    >>> conn = get_connection()
    >>> create_tables(conn)
"""

"""Database operations with schema management and bulk loading."""

"""Database operations with incremental loading support."""
import mysql.connector
from datetime import datetime
from config import MYSQL_CONFIG, TABLE_NAMES, SCHEMA_VERSION
from logger import setup_logger
from typing import Optional
import pandas as pd

logger = setup_logger(__name__)

def get_connection():
    """Get managed MySQL connection."""
    return mysql.connector.connect(**MYSQL_CONFIG)
    
def create_tables(conn):
    """Initialize database schema with version tracking."""
    queries = [
        f"""CREATE TABLE IF NOT EXISTS {TABLE_NAMES['products']} (
            stock_code VARCHAR(20) PRIMARY KEY,
            description VARCHAR(255),
            category VARCHAR(100),
            schema_version VARCHAR(10)
        )""",
        f"""CREATE TABLE IF NOT EXISTS {TABLE_NAMES['customers']} (
            customer_id VARCHAR(20) PRIMARY KEY,
            country VARCHAR(50),
            first_purchase_date DATETIME,
            last_purchase_date DATETIME,
            total_spent DECIMAL(12,2),
            total_transactions INT,
            schema_version VARCHAR(10)
        )""",
        f"""CREATE TABLE IF NOT EXISTS {TABLE_NAMES['transactions']} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            invoice VARCHAR(20),
            invoice_date DATETIME,
            customer_id VARCHAR(20),
            stock_code VARCHAR(20),
            quantity INT,
            price DECIMAL(10,2),
            total_amount DECIMAL(12,2),
            country VARCHAR(50),
            schema_version VARCHAR(10),
            FOREIGN KEY (customer_id) REFERENCES {TABLE_NAMES['customers']}(customer_id),
            FOREIGN KEY (stock_code) REFERENCES {TABLE_NAMES['products']}(stock_code),
            INDEX (invoice_date),
            INDEX (customer_id),
            INDEX (stock_code)
        )"""
    ]
    
    with conn.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
    conn.commit()
    logger.info(f"Created tables with schema version {SCHEMA_VERSION}")
    
def get_max_invoice_date(conn) -> Optional[datetime]:
    """Get the most recent invoice date from the database.
    
    Returns:
        datetime: Most recent invoice date, or None if no records exist
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                f"SELECT MAX(invoice_date) FROM {TABLE_NAMES['transactions']}"
            )
            result = cursor.fetchone()[0]
            return result if result else None
        except mysql.connector.Error as err:
            logger.error(f"Error getting max invoice date: {err}")
            return None

def upsert_customers(cursor, customers):
    """Optimized customer upsert with incremental check."""
    # Only update customers with new purchases
    pass  # Implementation would check existing records        

def load_data(conn, df, full_refresh=False):
    """Bulk load data into MySQL with schema versioning."""
    # Early return for empty DataFrames
    if df.empty:
        conn.commit()  # Still commit the transaction
        return  # Skip all processing

    # Verify required columns exist
    required_columns = {'customer_id', 'invoice', 'stock_code', 'invoice_date',
                       'quantity', 'price', 'country', 'description'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Handle incremental loading
    if not full_refresh:
        max_date = get_max_invoice_date(conn)
        if max_date:
            # Ensure proper datetime comparison
            df = df[pd.to_datetime(df['invoice_date']) > max_date]

    # Prepare customer data with proper datetime formatting
    customers = df.groupby('customer_id').agg({
        'country': 'first',
        'invoice_date': ['min', 'max'],
        'total_amount': 'sum',
        'invoice': 'nunique'
    }).reset_index()
    customers.columns = ['customer_id', 'country', 'first_purchase_date', 
                       'last_purchase_date', 'total_spent', 'total_transactions']
    
    # Convert datetime columns to MySQL-compatible strings
    customers['first_purchase_date'] = customers['first_purchase_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    customers['last_purchase_date'] = customers['last_purchase_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    customers['schema_version'] = SCHEMA_VERSION

    # Prepare product data
    products = df.groupby('stock_code').agg({
        'description': 'first'
    }).reset_index()
    products['category'] = products['description'].str.extract(r'([A-Z]+)')[0].fillna('OTHER')
    products['schema_version'] = SCHEMA_VERSION

    # Prepare transaction data with proper datetime formatting
    transactions = df[[
        'invoice', 'invoice_date', 'customer_id',
        'stock_code', 'quantity', 'price',
        'total_amount', 'country'
    ]].copy()
    transactions['invoice_date'] = transactions['invoice_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    transactions['schema_version'] = SCHEMA_VERSION

    # Load in batches
    with conn.cursor() as cursor:
        # Load customers
        cursor.executemany(f"""
            INSERT INTO {TABLE_NAMES['customers']}
            (customer_id, country, first_purchase_date, 
             last_purchase_date, total_spent, total_transactions, schema_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            country = VALUES(country),
            last_purchase_date = VALUES(last_purchase_date),
            total_spent = total_spent + VALUES(total_spent),
            total_transactions = total_transactions + VALUES(total_transactions)
        """, customers.to_records(index=False).tolist())
        
        # Load products
        cursor.executemany(f"""
            INSERT INTO {TABLE_NAMES['products']}
            (stock_code, description, category, schema_version)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            description = VALUES(description),
            category = VALUES(category)
        """, products.to_records(index=False).tolist())
        
        # Load transactions
        cursor.executemany(f"""
            INSERT INTO {TABLE_NAMES['transactions']}
            (invoice, invoice_date, customer_id, stock_code,
             quantity, price, total_amount, country, schema_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, transactions.to_records(index=False).tolist())
    
    conn.commit()
    logger.info(f"Loaded {len(df)} records across {len(TABLE_NAMES)} tables")