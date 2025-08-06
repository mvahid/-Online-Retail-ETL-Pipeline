"""Online Retail ETL Pipeline.

Orchestrates the complete ETL process:
1. Data extraction (download)
2. Data transformation (cleaning)
3. Data loading (to MySQL)

Usage:
    python scripts/main.py [--full-refresh] [--dry-run] [--only-clean]
                          [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from cleaner import clean_data
from config import LOGS_DIR
from database import (create_tables, get_connection, get_max_invoice_date,
                     load_data)
from downloader import download_dataset
from logger import setup_logger

logger = setup_logger(__name__)


def parse_args():
    """Enhanced CLI argument parsing with date validation."""
    parser = argparse.ArgumentParser(
        description='Online Retail ETL Pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Date arguments
    date_group = parser.add_argument_group('Incremental loading')
    date_group.add_argument(
        '--start-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help='Start date for incremental load (YYYY-MM-DD)'
    )
    date_group.add_argument(
        '--end-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        default=datetime.now(),
        help='End date for incremental load (YYYY-MM-DD)'
    )
    
    # Mode arguments
    mode_group = parser.add_argument_group('Processing modes')
    mode_group.add_argument(
        '--full-refresh',
        action='store_true',
        help='Force full data reload'
    )
    mode_group.add_argument(
        '--dry-run',
        action='store_true',
        help='Run pipeline without loading to database'
    )
    mode_group.add_argument(
        '--only-clean',
        action='store_true',
        help='Only clean data without loading to database'
    )
    
    return parser.parse_args()


def get_incremental_date_range(conn, args) -> Optional[Tuple[datetime, datetime]]:
    """Determine date range for incremental load."""
    if args.full_refresh:
        return None
        
    max_date = get_max_invoice_date(conn)
    if args.start_date:
        return (args.start_date, args.end_date)
    elif max_date:
        return (max_date, args.end_date)
    
    return None


def extract() -> Path:
    """Download and extract raw data."""
    logger.info("Starting extraction phase")
    return download_dataset()


def transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """Clean and transform raw data."""
    logger.info("Starting transformation phase")
    return clean_data(df)  # Assumes clean_data returns (df, metrics)


def load(clean_df: pd.DataFrame) -> None:
    """Load transformed data to database."""
    logger.info("Starting load phase")
    conn = get_connection()
    try:
        create_tables(conn)
        load_data(conn, clean_df)
    finally:
        conn.close()


def save_metrics(metrics: dict, output_dir: Path) -> None:
    """Save cleaning metrics to JSON file."""
    metrics_file = output_dir / "cleaning_metrics.json"
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)
    logger.info(f"Saved cleaning metrics to {metrics_file}")


def run_pipeline(args) -> bool:
    """Orchestrate pipeline with proper incremental loading."""
    try:
        # Establish database connection
        conn = get_connection()
        
        # Determine date range for incremental loading
        date_range = get_incremental_date_range(conn, args)
        
        if args.full_refresh:
            logger.info("Running full refresh")
        elif date_range:
            logger.info(f"Incremental loading from {date_range[0]} to {date_range[1]}")
        else:
            logger.info("No existing data found, doing initial load")

        # Extract data
        file_path = extract()
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Apply date filtering if needed
        if date_range:
            df = df[(df['InvoiceDate'] >= date_range[0]) & 
                   (df['InvoiceDate'] <= date_range[1])]
            logger.info(f"Filtered {len(df)} records in date range")
            
            # Check if filtered data is empty
            if len(df) == 0:
                logger.warning("No records found in specified date range")
                return True  # Exit successfully but skip processing

        # Transform data
        clean_df, metrics = transform(df)
        save_metrics(metrics, LOGS_DIR)

        # Load data (unless in dry-run or only-clean mode)
        if not args.dry_run and not args.only_clean:
            load(clean_df)
        elif args.only_clean:
            logger.info("Skipping load phase (--only-clean flag set)")
        else:
            logger.info("Dry run complete - no data was loaded")

        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        return False
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    args = parse_args()
    success = run_pipeline(args)
    sys.exit(0 if success else 1)