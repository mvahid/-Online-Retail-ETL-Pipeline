"""
Logging configuration with module-specific logging support.

Provides a centralized logging setup with:
- File and console handlers
- Timestamped log messages
- UTF-8 encoding for log files

Example:
    >>> from logger import setup_logger
    >>> logger = setup_logger()
    >>> logger.info("Test message")
"""

import logging
from config import LOGS_DIR, LOG_FILENAME

def setup_logger(name=__name__):
    """Configure a module-specific logger instance.
    
    Args:
        name (str): Module name (defaults to __name__)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(LOGS_DIR / LOG_FILENAME, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger