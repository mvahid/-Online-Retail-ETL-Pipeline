"""
Data cleaning and transformation module for the ETL pipeline.

Provides functions for:
- Standardizing column names
- Handling missing values
- Data type conversion
- Calculating derived metrics
- Tracking data quality statistics

Example:
    >>> from cleaner import clean_data
    >>> clean_df, metrics = clean_data(raw_df)
"""
"""Comprehensive data cleaning with detailed metrics tracking."""

import pandas as pd
import numpy as np
from logger import setup_logger
from typing import Tuple

logger = setup_logger(__name__)

def clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """Clean and transform raw data with empty DataFrame handling."""
    metrics = {
        'original_rows': len(df),
        'rejected_rows': 0,
        'cleaned_rows': 0,
        'rejection_rate': 0.0,
        'transformations': [],  # Add this list to track transformations
        'missing_values': {},   # Add this dict to track missing values
        'invalid_values': {}    # Add this dict to track invalid values
    }
    
    # Return early if empty DataFrame
    if len(df) == 0:
        logger.warning("Received empty DataFrame - skipping cleaning")
        metrics['cleaned_rows'] = 0
        return df, metrics

    # 1. Standardize column names
    column_mapping = {
    'customerid': 'customer_id',
    'customer_id': 'customer_id',
    'customer': 'customer_id',
    'customer id': 'customer_id',  # Add space variation
    'invoiceno': 'invoice',
    'invoice_no': 'invoice',  # Add underscore variation
    'invoice no': 'invoice',  # Add space variation
    'invoice': 'invoice',
    'stockcode': 'stock_code',
    'stock_code': 'stock_code',
    'stock code': 'stock_code',  # Add space variation
    'invoicedate': 'invoice_date',
    'invoice_date': 'invoice_date',
    'invoice date': 'invoice_date',  # Add space variation
    'unitprice': 'price',
    'unit_price': 'price',  # Add underscore variation
    'unit price': 'price',  # Add space variation
    'price': 'price',
    'quantity': 'quantity',
    'description': 'description',
    'country': 'country'
    }
    
    # Convert all columns to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(r'[^a-z0-9]+', '_', regex=True).str.strip('_')
    
    # Rename columns according to our standard
    renamed_cols = {}
    for col in df.columns:
        if col in column_mapping:
            new_col = column_mapping[col]
            if col != new_col:
                renamed_cols[col] = new_col
    
    if renamed_cols:
        df = df.rename(columns=renamed_cols)
        metrics['transformations'].append(f"Renamed columns: {renamed_cols}")
    
    # Verify required columns exist after renaming
    required_columns = {'customer_id', 'invoice', 'stock_code', 'invoice_date', 'quantity', 'price'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise KeyError(f"Missing required columns: {missing_columns}")
        
    # 2. Handle missing values
    for col in ['customer_id', 'description']:
        if col in df.columns:
            missing = df[col].isna().sum()
            if missing > 0:
                metrics['missing_values'][col] = missing
                if col == 'customer_id':
                    df[col] = df[col].fillna('GUEST').astype(str)
                else:
                    df[col] = df[col].fillna('UNKNOWN')
                metrics['transformations'].append(f"Filled {missing} missing {col}")
    
    # 3. Data type conversions
    if 'invoice_date' in df.columns:
        df['invoice_date'] = pd.to_datetime(df['invoice_date'])
        metrics['transformations'].append("Converted invoice_date to datetime")
    
    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].astype(str)
        metrics['transformations'].append("Converted customer_id to string")
    
    # 4. Remove test transactions
    if 'invoice' in df.columns:
        test_trans = df['invoice'].astype(str).str.startswith(('C', 'A'), na=False)
        if test_trans.any():
            metrics['invalid_values']['test_transactions'] = int(test_trans.sum())
            df = df[~test_trans].copy()
            metrics['transformations'].append("Removed test transactions")
    
    # 5. Validate quantities and prices
    for col, check in [('quantity', lambda x: x <= 0),
                      ('price', lambda x: x <= 0)]:
        if col in df.columns:
            invalid = check(df[col]).sum()
            if invalid > 0:
                metrics['invalid_values'][col] = int(invalid)
                df = df[~check(df[col])].copy()
                metrics['transformations'].append(f"Removed invalid {col} values")
    
    # 6. Calculate derived metrics
    if all(c in df.columns for c in ['quantity', 'price']):
        df['total_amount'] = df['quantity'] * df['price']
        metrics['transformations'].append("Calculated total_amount")
    
    # Calculate metrics safely
    try:
        metrics['rejection_rate'] = round(metrics['rejected_rows'] / metrics['original_rows'], 4)
    except ZeroDivisionError:
        metrics['rejection_rate'] = 0.0
    
    metrics['cleaned_rows'] = len(df)  # Update with final row count
    
    return df, metrics    
        
    
    # Final metrics
    metrics['cleaned_rows'] = int(len(df))
    metrics['rejected_rows'] = metrics['original_rows'] - metrics['cleaned_rows']
    metrics['rejection_rate'] = round(metrics['rejected_rows'] / metrics['original_rows'], 4)
    
    logger.info("Data cleaning complete")
    logger.debug(f"Final schema:\n{df.info()}")
    
    return df, metrics
    
