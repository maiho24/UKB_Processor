import polars as pl
from pathlib import Path
from typing import Union, Optional
import os

def csv_to_parquet(
    input_file: Union[str, Path],
    output_file: Union[str, Path],
    compression: str = "zstd",
    chunk_size: int = 50000,
    n_threads: Optional[int] = None
) -> None:
    """
    Convert UKB CSV file to Parquet format with configurable CPU usage.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output Parquet file
        compression: Compression algorithm (zstd, snappy, gzip, none)
        chunk_size: Number of rows to process at once
        n_threads: Number of threads to use. If None, uses PBS_NCPUS or 1
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file {input_path} not found")
        
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Determine number of threads
    if n_threads is None:
        # Try to get PBS_NCPUS from environment
        n_threads = int(os.environ.get('PBS_NCPUS', 1))
    
    try:
        print(f"Starting conversion with {n_threads} threads...")
        # Use scan_csv with memory-friendly settings
        csv_reader = pl.scan_csv(
            input_path,
            low_memory=True,
            rechunk=False,
            n_threads=n_threads
        )
        
        # Write to parquet with conservative settings
        csv_reader.sink_parquet(
            output_path,
            compression=compression,
            row_group_size=chunk_size
        )
        print("Conversion completed successfully!")
            
    except Exception as e:
        raise RuntimeError(f"Error during conversion: {str(e)}")
        