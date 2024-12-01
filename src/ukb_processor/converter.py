import polars as pl
from pathlib import Path
from typing import Union, Optional
import psutil
import math

def estimate_chunk_size(file_size: int, num_columns: int) -> int:
    """
    Estimate appropriate chunk size based on available memory and file characteristics.
    """
    # Get available memory (in bytes)
    available_memory = psutil.virtual_memory().available
    
    # Assume each cell takes approximately 8 bytes on average
    # Use 75% of available memory as safety margin
    safe_memory = available_memory * 0.75
    
    # Calculate chunks based on file size and columns
    estimated_rows = safe_memory / (num_columns * 8)
    
    # Round down to nearest 10000
    chunk_size = math.floor(estimated_rows / 10000) * 10000
    
    # Set reasonable bounds
    return max(min(chunk_size, 500000), 10000)

def csv_to_parquet(
    input_file: Union[str, Path],
    output_file: Union[str, Path],
    compression: str = "zstd",
    chunk_size: Optional[int] = None,
    streaming: bool = True
) -> None:
    """
    Convert UKB CSV file to Parquet format with memory optimization.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output Parquet file
        compression: Compression algorithm to use (zstd, snappy, gzip, none)
        chunk_size: Number of rows to process at once (if None, will be estimated)
        streaming: Whether to use streaming mode for very large files
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file {input_path} not found")
        
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Get file size
    file_size = input_path.stat().st_size
    
    if streaming:
        # Use streaming approach for large files
        try:
            # First pass: count columns and sample data
            df_sample = pl.scan_csv(input_path).fetch(1000)
            num_columns = len(df_sample.columns)
            
            # Estimate chunk size if not provided
            if chunk_size is None:
                chunk_size = estimate_chunk_size(file_size, num_columns)
            
            # Create streaming reader
            csv_reader = pl.scan_csv(
                input_path,
                low_memory=True,
                rechunk=False
            )
            
            # Write to parquet with optimized settings
            csv_reader.sink_parquet(
                output_path,
                compression=compression,
                row_group_size=chunk_size,
                use_pyarrow=True,
                compression_level=3,  # Balance between compression and speed
                statistics=False  # Disable statistics for better performance
            )
            
        except Exception as e:
            raise RuntimeError(f"Error during streaming conversion: {str(e)}")
            
    else:
        # Use standard approach for smaller files
        try:
            df = pl.read_csv(input_path)
            df.write_parquet(
                output_path,
                compression=compression,
                row_group_size=chunk_size if chunk_size else 100000
            )
            
        except Exception as e:
            raise RuntimeError(f"Error during standard conversion: {str(e)}")

def get_memory_usage() -> str:
    """Get current memory usage in human-readable format."""
    process = psutil.Process()
    memory_info = process.memory_info()
    return f"{memory_info.rss / 1024 / 1024:.1f} MB"
    