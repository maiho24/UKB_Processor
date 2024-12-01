"""
UKB Processor - Tools for processing UK Biobank data files.

Main functionality:
- Convert CSV files to Parquet format
- Extract specific fields from Parquet files
"""

__version__ = "0.1.0"

from .converter import csv_to_parquet
from .extractor import extract_fields
from .utils import read_field_ids

__all__ = [
    "csv_to_parquet",
    "extract_fields",
    "read_field_ids"
]

__author__ = "Mai Ho"
__email__ = "mai.ho@unsw.edu.au"
__description__ = "Tools for processing UK Biobank data files"
