import polars as pl
from pathlib import Path
from typing import Union, Optional
import os

def csv_to_parquet(
    input_file: Union[str, Path],
    output_file: Union[str, Path],
    compression: str = "zstd",
    chunk_size: int = 50000
) -> None:
    """
    Convert UKB CSV file to Parquet format optimized for large files.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output Parquet file
        compression: Compression algorithm (zstd, snappy, gzip, none)
        chunk_size: Number of rows in each parquet row group
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file {input_path} not found")
        
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        print(f"Starting conversion of {input_path}")
        
        csv_reader = pl.scan_csv(
            input_path,
            low_memory=True,     # Reduces memory usage
            rechunk=False,       # Prevents memory spikes
            infer_schema_length=10000,  # More accurate schema inference
            ignore_errors=True,  # Continue on parsing errors
            try_parse_dates=True # Automatically parse dates
        )
        
        print("CSV scanning initialized, starting parquet conversion...")
        
        csv_reader.sink_parquet(
            output_path,
            compression=compression,
            row_group_size=chunk_size
        )
        
        print(f"Conversion completed successfully!")
        print(f"Output saved to: {output_path}")
            
    except Exception as e:
        raise RuntimeError(f"Error during conversion: {str(e)}")
        
