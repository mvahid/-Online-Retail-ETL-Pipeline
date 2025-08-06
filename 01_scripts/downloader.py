"""
Dataset downloader module for the Online Retail ETL pipeline.

Handles downloading of the retail dataset from multiple sources with:
- Automatic fallback to secondary URLs
- Streaming download for large files
- Progress logging

Example:
    >>> from downloader import download_dataset
    >>> file_path = download_dataset()
"""

"""Dataset downloader with integrity verification."""

import requests
import hashlib
from pathlib import Path
from config import DATA_DIR, DATASET_URLS, DATASET_FILENAME
from logger import setup_logger
import time  # Added for retry delays

logger = setup_logger(__name__)

def verify_file_integrity(file_path, expected_size=None):
    """Verify downloaded file integrity."""
    try:
        if not file_path.exists():
            return False
            
        actual_size = file_path.stat().st_size
        
        # Only check size if expected_size is provided and > 0
        if expected_size is not None and expected_size > 0:
            if actual_size != expected_size:
                logger.warning(f"File size mismatch: {actual_size} vs {expected_size}")
                return False
                
        # Additional check - file shouldn't be empty unless expected_size is 0
        if actual_size == 0 and (expected_size is None or expected_size != 0):
            logger.warning("Downloaded file is empty")
            return False
            
        return True
    except Exception as e:
        logger.warning(f"Verification failed: {str(e)}")
        return False

def download_with_retry(url, file_path, max_retries=3):
    """Download helper with retry logic."""
    for attempt in range(max_retries):
        try:
            # Add timeout to prevent hanging
            response = requests.get(url, stream=True, timeout=(10, 30))
            response.raise_for_status()
            
            # Try to get expected size from headers (fallback to 0)
            expected_size = int(response.headers.get('Content-Length', 0))
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
            
            return expected_size
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            continue
            
    return None

def download_dataset():
    """Download dataset with fallback and integrity check."""
    DATA_DIR.mkdir(exist_ok=True)
    file_path = DATA_DIR / DATASET_FILENAME
    
    for url in DATASET_URLS:
        logger.info(f"Attempting download from {url}")
        
        expected_size = download_with_retry(url, file_path)
        if expected_size is None:
            logger.warning(f"All retries failed for {url}")
            continue
            
        if verify_file_integrity(file_path, expected_size):
            logger.info(f"Verified dataset saved to {file_path}")
            return file_path
        else:
            file_path.unlink(missing_ok=True)
            logger.warning("File verification failed, trying next source")
    
    raise Exception("All download attempts failed")