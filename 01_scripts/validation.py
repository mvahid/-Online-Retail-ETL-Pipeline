"""Data validation against schema."""
import pandas as pd
import json
from pathlib import Path

def validate_schema(df: pd.DataFrame, stage: str) -> bool:
    """Validate DataFrame against schema."""
    with open(Path(__file__).parent / "schema.json") as f:
        schema = json.load(f)
    
    # Check required columns
    missing = set(schema['required_columns'][stage]) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    # Check column types
    for col, expected_type in schema['column_types'].items():
        if col in df.columns and not pd.api.types.is_dtype(df[col].dtype, expected_type):
            raise TypeError(f"Column {col} should be {expected_type}")
    
    return True