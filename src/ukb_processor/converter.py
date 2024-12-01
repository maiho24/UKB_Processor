import polars as pl
from pathlib import Path
from typing import Union

def csv_to_parquet(
    input_file: Union[str, Path],
    output_file: Union[str, Path],
    compression: str = "zstd",
    chunk_size: int = 100000
) -> None:
    """
    Convert UKB CSV file to Parquet format.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output Parquet file
        compression: Compression algorithm to use (zstd, snappy, gzip, none)
        chunk_size: Number of rows to process at once
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file {input_path} not found")
        
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read and write in chunks to handle large files
    df = pl.scan_csv(input_path)
    df.sink_parquet(
        output_path,
        compression=compression,
        row_group_size=chunk_size
    )
    