import pytest
from unittest.mock import Mock, patch, MagicMock
from scripts.database import (
    get_connection,
    create_tables,
    load_data,
    get_max_invoice_date
)
import pandas as pd
from datetime import datetime
import mysql.connector
from scripts.config import TABLE_NAMES

@pytest.fixture
def mock_conn():
    """Fixture for mocked database connection with context manager support"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn

@pytest.fixture
def sample_data():
    """Sample DataFrame matching ALL database schema requirements"""
    data = pd.DataFrame({
        'customer_id': ['12345', '12346'],
        'invoice': ['INV001', 'INV002'],
        'stock_code': ['PROD01', 'PROD02'],
        'invoice_date': [datetime(2025, 1, 1), datetime(2025, 1, 2)],
        'quantity': [2, 3],
        'price': [10.0, 15.0],
        'country': ['US', 'UK'],
        'description': ['Product 1', 'Product 2']
    })
    # Calculate total_amount if needed by your implementation
    data['total_amount'] = data['quantity'] * data['price']
    return data

def test_create_tables(mock_conn):
    """Test table creation SQL execution"""
    with patch('scripts.database.TABLE_NAMES', {
        'products': 'products',
        'customers': 'customers',
        'transactions': 'transactions'
    }):
        create_tables(mock_conn)
    
    cursor = mock_conn.cursor.return_value.__enter__.return_value
    assert cursor.execute.call_count == 3
    
    queries = [call[0][0] for call in cursor.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS products" in q for q in queries)
    assert any("CREATE TABLE IF NOT EXISTS customers" in q for q in queries)
    assert any("CREATE TABLE IF NOT EXISTS transactions" in q for q in queries)
    mock_conn.commit.assert_called_once()

def test_get_max_invoice_date(mock_conn):
    """Test getting maximum invoice date"""
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.return_value = (datetime(2025, 1, 1, 12, 0),)
    
    with patch('scripts.database.TABLE_NAMES', {'transactions': 'transactions'}):
        result = get_max_invoice_date(mock_conn)
    
    assert result == datetime(2025, 1, 1, 12, 0)
    assert "SELECT MAX(invoice_date) FROM transactions" in mock_cursor.execute.call_args[0][0]

def test_get_max_invoice_date_empty(mock_conn):
    """Test getting max date when no records exist"""
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.return_value = (None,)
    
    with patch('scripts.database.TABLE_NAMES', {'transactions': 'transactions'}):
        result = get_max_invoice_date(mock_conn)
    
    assert result is None

def test_load_data(mock_conn, sample_data):
    """Test data loading with valid DataFrame"""
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.return_value = (None,)  # No existing data
    
    with patch('scripts.database.TABLE_NAMES', {
        'products': 'products',
        'customers': 'customers',
        'transactions': 'transactions'
    }):
        load_data(mock_conn, sample_data)
    
    assert mock_cursor.executemany.call_count == 3
    transaction_calls = mock_cursor.executemany.call_args_list[2][0][1]
    assert len(transaction_calls) == 2
    assert transaction_calls[0][0] == 'INV001'
    assert transaction_calls[1][0] == 'INV002'
    mock_conn.commit.assert_called_once()

def test_load_data_incremental(mock_conn, sample_data):
    """Test incremental loading with existing data"""
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    # Set max date to filter out the first record (Jan 1)
    mock_cursor.fetchone.return_value = (datetime(2025, 1, 1, 23, 59, 59),)  # End of Jan 1
    
    with patch('scripts.database.TABLE_NAMES', {
        'products': 'products',
        'customers': 'customers',
        'transactions': 'transactions'
    }), patch('pandas.DataFrame.__gt__') as mock_gt:
        # Mock the comparison to return False for first record, True for second
        mock_gt.return_value = pd.Series([False, True])
        load_data(mock_conn, sample_data, full_refresh=False)
    
    # Verify only the second record was loaded
    transaction_calls = mock_cursor.executemany.call_args_list[2][0][1]
    assert len(transaction_calls) == 1
    assert transaction_calls[0][0] == 'INV002'
    assert transaction_calls[0][1] == '2025-01-02 00:00:00'
    
def test_load_data_empty(mock_conn):
    """Test loading empty DataFrame"""
    # Create properly typed empty DataFrame
    empty_data = pd.DataFrame({
        'customer_id': pd.Series(dtype='str'),
        'invoice': pd.Series(dtype='str'),
        'stock_code': pd.Series(dtype='str'),
        'invoice_date': pd.Series(dtype='datetime64[ns]'),
        'quantity': pd.Series(dtype='int'),
        'price': pd.Series(dtype='float'),
        'country': pd.Series(dtype='str'),
        'description': pd.Series(dtype='str')
    })

    with patch('scripts.database.TABLE_NAMES', {
        'products': 'products',
        'customers': 'customers',
        'transactions': 'transactions'
    }):
        load_data(mock_conn, empty_data)
    
    # With empty DataFrame optimization, we shouldn't create a cursor
    mock_conn.cursor.assert_not_called()
    
    # But we should still commit the transaction
    mock_conn.commit.assert_called_once()
    
def test_load_data_missing_columns(mock_conn):
    """Test error handling for missing required columns"""
    # Create test data missing invoice_date (but has other required columns)
    test_data = pd.DataFrame({
        'customer_id': ['12345'],
        'invoice': ['INV001'],
        'stock_code': ['PROD01'],
        'quantity': [2],
        'price': [10.0],
        'country': ['US'],
        'description': ['Product 1']
        # Missing invoice_date
    })
    
    with patch('scripts.database.TABLE_NAMES', {
        'products': 'products',
        'customers': 'customers',
        'transactions': 'transactions'
    }), patch('scripts.database.get_max_invoice_date') as mock_max_date:
        with pytest.raises(ValueError) as exc_info:
            load_data(mock_conn, test_data)
        
        mock_max_date.assert_not_called()
        error_msg = str(exc_info.value)
        assert "Missing required columns" in error_msg
        assert "invoice_date" in error_msg