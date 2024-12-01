import polars as pl
from pathlib import Path
from typing import Union, List, Optional, Set
from .utils import read_field_ids, validate_field_ids

def extract_fields(
    parquet_file: Union[str, Path],
    output_file: Union[str, Path],
    field_ids: Optional[List[str]] = None,
    field_file: Optional[Union[str, Path]] = None,
    remove_empty: bool = False,
    instance: Optional[str] = None
) -> Set[str]:
    """
    Extract specified fields from Parquet file. Always includes the 'eid' field.
    
    Args:
        parquet_file: Path to input Parquet file
        output_file: Path to output CSV file
        field_ids: Optional list of field IDs provided directly
        field_file: Optional path to text file containing field IDs
        remove_empty: If True, removes rows where all extracted fields (excluding eid) are null
        instance: Optional instance identifier (e.g., "1.0", "2.0") to extract specific instances only
        
    Returns:
        Set of all field IDs that were processed (excluding 'eid')
        
    Raises:
        ValueError: If neither field_ids nor field_file is provided
        FileNotFoundError: If input files don't exist
        ValueError: If 'eid' column is not present in the Parquet file
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
    
    # Get schema without warning
    schema = df.collect_schema()
    available_columns = schema.names()
    
    # Check if 'eid' column exists
    if 'eid' not in available_columns:
        raise ValueError("Required 'eid' column not found in Parquet file")
    
    # Get columns for specified fields
    selected_cols = ['eid']
    
    # Then add columns for each field ID
    for field_id in all_field_ids:
        if instance:
            # Look for exact instance match
            field_col = f"{field_id}-{instance}"
            if field_col in available_columns:
                selected_cols.append(field_col)
        else:
            # Get all instances
            field_cols = [col for col in available_columns if col == field_id or col.startswith(f"{field_id}-")]
            selected_cols.extend(field_cols)
    
    # Select columns
    query = df.select(selected_cols)
    
    # Apply empty row filtering if requested
    if remove_empty:
        # Create a condition that checks if all fields (except eid) are empty
        non_eid_cols = [col for col in selected_cols if col != 'eid']
        if non_eid_cols:  # Only apply filter if we have non-eid columns
            query = query.filter(
                pl.any_horizontal([
                    (pl.col(col).is_not_null()) & 
                    (pl.col(col).cast(pl.Utf8).str.strip().str.lengths() > 0)
                    for col in non_eid_cols
                ])
            )
    
    # Collect the DataFrame and write to CSV
    query.collect().write_csv(output_path)
    
    return all_field_ids