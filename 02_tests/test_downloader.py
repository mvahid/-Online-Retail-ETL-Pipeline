"""Test script for downloader.py"""
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import requests
from downloader import download_dataset, verify_file_integrity
import tempfile
import os

# Test fixtures
@pytest.fixture
def mock_data_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)

@pytest.fixture
def mock_config(mock_data_dir):
    with patch('downloader.DATA_DIR', mock_data_dir), \
         patch('downloader.DATASET_URLS', ['http://good.url/file', 'http://fallback.url/file']), \
         patch('downloader.DATASET_FILENAME', 'test_dataset.dat'):
        yield

# Verification tests
def test_verify_file_integrity_success(mock_data_dir):
    """Test file verification with correct size"""
    test_file = mock_data_dir / "test_file"
    content = b"test content"  # 12 bytes (t=1, e=1, s=1, t=1, space=1, c=1, o=1, n=1, t=1, e=1, n=1, t=1)
    test_file.write_bytes(content)
    assert verify_file_integrity(test_file, expected_size=len(content)) is True

def test_verify_file_integrity_size_mismatch(mock_data_dir):
    """Test file verification with size mismatch"""
    test_file = mock_data_dir / "test_file"
    test_file.write_bytes(b"test content")
    assert verify_file_integrity(test_file, expected_size=10) is False

def test_verify_file_integrity_missing_file(mock_data_dir):
    """Test file verification with missing file"""
    assert verify_file_integrity(mock_data_dir / "nonexistent") is False

def test_verify_file_integrity_empty_file(mock_data_dir):
    """Test file verification with empty file"""
    test_file = mock_data_dir / "empty_file"
    test_file.touch()
    assert verify_file_integrity(test_file) is False

# Download tests
def create_mock_response(content=b'test content', status=200, content_length=None):
    """Helper to create mock response"""
    mock_resp = MagicMock()
    mock_resp.status_code = status
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_content = lambda **kwargs: [content]
    if content_length is None:
        content_length = len(content)
    mock_resp.headers = {'Content-Length': str(content_length)}
    return mock_resp

@patch('downloader.requests.get')
@patch('downloader.requests.head')
def test_successful_download(mock_head, mock_get, mock_data_dir, mock_config):
    """Test successful download on first attempt"""
    content = b"test content"  # 12 bytes
    
    # Setup mock responses
    mock_head.return_value = create_mock_response(content_length=len(content))
    mock_get.return_value = create_mock_response(content)
    
    result = download_dataset()
    assert result == mock_data_dir / 'test_dataset.dat'
    assert result.read_bytes() == content

@patch('downloader.requests.get')
@patch('downloader.requests.head')
def test_fallback_download(mock_head, mock_get, mock_data_dir, mock_config):
    """Test fallback to second URL when first fails"""
    content = b"test content"  # 12 bytes
    
    # First URL fails
    mock_head.side_effect = [
        requests.exceptions.ConnectionError("First URL failed"),
        create_mock_response(content_length=len(content))
    ]
    
    # First GET fails, second succeeds
    mock_get.side_effect = [
        requests.exceptions.ConnectionError("First URL failed"),
        create_mock_response(content)
    ]
    
    result = download_dataset()
    assert result == mock_data_dir / 'test_dataset.dat'
    assert result.read_bytes() == content

@patch('downloader.requests.get')
@patch('downloader.requests.head')
def test_chunked_download(mock_head, mock_get, mock_data_dir, mock_config):
    """Test chunked download works correctly"""
    chunks = [b'chunk1', b'chunk2']
    combined = b''.join(chunks)
    
    # Setup mock responses
    mock_head.return_value = create_mock_response(content_length=len(combined))
    
    mock_response = create_mock_response(content_length=len(combined))
    mock_response.iter_content = lambda **kwargs: chunks
    mock_get.return_value = mock_response
    
    result = download_dataset()
    assert result == mock_data_dir / 'test_dataset.dat'
    assert result.read_bytes() == combined

@patch('downloader.requests.get')
@patch('downloader.requests.head')
def test_retry_mechanism(mock_head, mock_get, mock_data_dir, mock_config):
    """Test retry mechanism works"""
    content = b"test content"  # 12 bytes
    
    # Setup mock responses
    mock_head.return_value = create_mock_response(content_length=len(content))
    
    # First two attempts fail, third succeeds
    mock_get.side_effect = [
        requests.exceptions.Timeout("Timeout"),
        requests.exceptions.ConnectionError("Connection error"),
        create_mock_response(content)
    ]
    
    result = download_dataset()
    assert result == mock_data_dir / 'test_dataset.dat'
    assert result.read_bytes() == content