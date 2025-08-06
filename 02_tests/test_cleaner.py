import pandas as pd
import pytest
from scripts.cleaner import clean_data
from datetime import datetime
import numpy as np

@pytest.fixture
def sample_data():
    """Test data with various quality issues"""
    return pd.DataFrame({
        'InvoiceNo': ['536365', '536366', 'C536367', 'A12345', '536368'],
        'StockCode': ['85123A', '71053', 'POST', 'TEST', 'BANK'],
        'Description': ['WHITE HANGING HEART', None, 'POSTAGE', 'TEST ITEM', 'BANK CHARGES'],
        'Quantity': [6, -1, 1, 0, 1],
        'InvoiceDate': [
            '12/1/2025 14:26',
            '12/1/2025 14:30',
            '12/1/2025 14:32',
            '12/1/2025 14:35',
            '12/1/2025 14:40'
        ],
        'UnitPrice': [2.55, 3.39, 0.0, -1.0, 5.0],
        'CustomerID': ['17850', None, None, '12345', '12346'],
        'Country': ['UK', 'France', 'UK', 'US', 'UK']
    })

def test_clean_data_transformations(sample_data):
    """Test data cleaning transformations"""
    cleaned, metrics = clean_data(sample_data)
    
    # Test column renaming
    assert 'invoice' in cleaned.columns
    assert 'stock_code' in cleaned.columns
    assert 'invoice_date' in cleaned.columns
    
    # Test data type conversions
    assert pd.api.types.is_datetime64_any_dtype(cleaned['invoice_date'])
    assert pd.api.types.is_string_dtype(cleaned['customer_id'])
    
    # Test metrics
    assert 'transformations' in metrics
    assert isinstance(metrics['transformations'], list)

def test_clean_data_metrics(sample_data):
    """Test cleaning metrics calculation"""
    _, metrics = clean_data(sample_data)
    
    assert metrics['original_rows'] == len(sample_data)
    assert metrics['rejected_rows'] >= 0
    assert 0 <= metrics['rejection_rate'] <= 1
    assert metrics['cleaned_rows'] > 0
    assert 'missing_values' in metrics
    assert 'invalid_values' in metrics
    assert 'transformations' in metrics

def test_clean_empty_data():
    """Test handling of empty DataFrame"""
    empty_df = pd.DataFrame()
    cleaned, metrics = clean_data(empty_df)
    
    assert len(cleaned) == 0
    assert metrics['original_rows'] == 0
    assert metrics['rejection_rate'] == 0.0
    assert metrics['cleaned_rows'] == 0

def test_column_name_variations():
    """Test that different column name formats work"""
    variant_data = pd.DataFrame({
        'Customer ID': ['12345'],          # Will become customer_id
        'INVOICE NO': ['536365'],          # Will become invoice
        'Stock Code': ['85123A'],          # Will become stock_code
        'Invoice Date': ['12/1/2025 14:26'], # Will become invoice_date
        'Quantity': [6],                   # Will become quantity
        'Unit Price': [2.55],              # Will become price
        'Country': ['UK'],                 # Will become country
        'Description': ['Test Product']    # Will become description
    })
    
    cleaned, _ = clean_data(variant_data)
    
    # Check standard column names exist
    required_cols = ['customer_id', 'invoice', 'stock_code', 
                    'invoice_date', 'quantity', 'price', 'country']
    assert all(col in cleaned.columns for col in required_cols)
    
    # Verify the data was preserved
    assert cleaned['customer_id'][0] == '12345'
    assert cleaned['invoice'][0] == '536365'
    assert cleaned['price'][0] == 2.55


def test_missing_required_columns():
    """Test error when required columns are missing"""
    incomplete_data = pd.DataFrame({
        'InvoiceNo': ['536365'],
        'StockCode': ['85123A'],
        'Description': ['TEST'],
        'Quantity': [6]
    })
    
    with pytest.raises(KeyError):
        clean_data(incomplete_data)