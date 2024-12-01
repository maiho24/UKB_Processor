from pathlib import Path
from typing import List, Union, Set
import polars as pl

def read_field_ids(field_file: Union[str, Path]) -> List[str]:
    """
    Read field IDs from a text file.
    
    Args:
        field_file: Path to text file containing field IDs (one per line)
        
    Returns:
        List of field IDs
    """
    field_path = Path(field_file)
    if not field_path.exists():
        raise FileNotFoundError(f"Field ID file {field_path} not found")
        
    with open(field_path) as f:
        # Remove whitespace and empty lines
        return [line.strip() for line in f if line.strip()]

def get_field_columns(df_columns: List[str], field_ids: Set[str]) -> List[str]:
    """
    Get all column names that correspond to given field IDs.
    
    Args:
        df_columns: List of all column names in DataFrame
        field_ids: Set of field IDs to match
        
    Returns:
        List of column names matching the field IDs
    """
    selected_cols = ['eid']  # Always include eid
    
    for field_id in field_ids:
        field_cols = [col for col in df_columns if col.startswith(f"{field_id}-")]
        selected_cols.extend(field_cols)
        
    return selected_cols

def validate_field_ids(field_ids: List[str]) -> List[str]:
    """
    Validate format of field IDs.
    
    Args:
        field_ids: List of field IDs to validate
        
    Returns:
        List of validated field IDs
        
    Raises:
        ValueError: If any field ID is invalid
    """
    validated = []
    for field_id in field_ids:
        # Remove any whitespace
        field_id = field_id.strip()
        
        # Check if field_id is numeric
        if not field_id.isdigit():
            raise ValueError(f"Invalid field ID: {field_id}. Must be numeric.")
            
        validated.append(field_id)
        
    return validated
    