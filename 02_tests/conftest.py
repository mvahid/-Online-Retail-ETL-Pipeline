import pytest
import pandas as pd
from datetime import datetime

@pytest.fixture
def sample_raw_data():
    """Sample raw data with various quality issues"""
    return pd.DataFrame({
        'InvoiceNo': ['536365', '536366', 'C536367', None],
        'StockCode': ['85123A', '71053', 'POST', 'TEST'],
        'Description': ['WHITE HANGING HEART', None, 'POSTAGE', 'TEST ITEM'],
        'Quantity': [6, -1, 1, 0],
        'InvoiceDate': [
            datetime(2025, 1, 1),
            datetime(2025, 1, 2),
            datetime(2025, 1, 3),
            None
        ],
        'UnitPrice': [2.55, 3.39, 0.0, -1.0],
        'CustomerID': ['17850', None, None, '12345']
    })