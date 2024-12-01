import polars as pl
from pathlib import Path
from typing import Union, List, Optional, Set
from .utils import read_field_ids, validate_field_ids

def extract_fields(
    parquet_file: Union[str, Path],
    output_file: Union[str, Path],
    field_ids: Optional[List[str]] = None,
    field_file: Optional[Union[str, Path]] = None
) -> Set[str]:
    """
    Extract specified fields from Parquet file.
    
    Args:
        parquet_file: Path to input Parquet file
        output_file: Path to output CSV file
        field_ids: Optional list of field IDs provided directly
        field_file: Optional path to text file containing field IDs
        
    Returns:
        Set of all field IDs that were processed
        
    Raises:
        ValueError: If neither field_ids nor field_file is provided
        FileNotFoundError: If input files don't exist
    """
    if field_ids is None and field_file is None:
        raise ValueError("Must provide either field_ids or field_file")
        
    parquet_path = Path(parquet_file)
    output_path = Path(output_file)
    
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file {parquet_path} not found")
    
    # Collect all field IDs from both sources
    all_field_ids: Set[str] = set()
    
    if field_ids:
        validated_fields = validate_field_ids(field_ids)
        all_field_ids.update(validated_fields)
        
    if field_file:
        file_fields = read_field_ids(field_file)
        validated_file_fields = validate_field_ids(file_fields)
        all_field_ids.update(validated_file_fields)
    
    # Read Parquet file
    df = pl.scan_parquet(parquet_path)
    
    # Get columns for specified fields
    selected_cols = ['eid']  # Always include eid
    for field_id in all_field_ids:
        field_cols = [col for col in df.columns if col.startswith(f"{field_id}-")]
        selected_cols.extend(field_cols)
    
    # Select columns and write to CSV
    df.select(selected_cols).sink_csv(output_path)
    
    return all_field_ids
    