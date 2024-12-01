# UKB Processor

A Python tool for processing UK Biobank data files, with a focus on efficient data conversion and field extraction.

## Installation

Using Conda helps manage dependencies and ensures compatibility across different systems. We recommend using [Miniconda](https://docs.conda.io/en/latest/miniconda.html) to keep your environment lightweight.

### 1. Create and activate a new conda environment:
```bash
conda create -n ukb-processor
conda activate ukb-processor
```

### 2. Clone and install the package:
```bash
git clone https://github.com/yourusername/UKB_Processor.git
cd UKB_Processor
pip install -e .
```

**Note**: Make sure to always activate the environment before using the package:
```bash
conda activate ukb-processor
```

## Command Help

You can view help for any command using the `--help` or `-h` option:

### Main help
```bash
$ ukb-processor --help
Usage: ukb-processor [OPTIONS] COMMAND [ARGS]...

  UK Biobank data processing tools

Options:
  --help  Show this message and exit.

Commands:
  convert  Convert UKB CSV file to Parquet format.
  extract  Extract specified fields from Parquet file.
```

### Convert command help
```bash
$ ukb-processor convert --help
Usage: ukb-processor convert [OPTIONS] INPUT_FILE OUTPUT_FILE

  Convert UKB CSV file to Parquet format.

Arguments:
  INPUT_FILE   Input CSV file  [required]
  OUTPUT_FILE  Output Parquet file  [required]

Options:
  --compression TEXT      Compression algorithm  [default: zstd]
  --chunk-size INTEGER   Chunk size for processing  [default: 100000]
  --help                 Show this message and exit.
```

### Extract command help
```bash
$ ukb-processor extract --help
Usage: ukb-processor extract [OPTIONS] PARQUET_FILE OUTPUT_FILE

  Extract specified fields from Parquet file.

  Fields can be specified either by --fields or --file or both. When both are
  provided, fields from both sources will be combined.

Arguments:
  PARQUET_FILE  Input Parquet file  [required]
  OUTPUT_FILE   Output CSV file  [required]

Options:
  --fields TEXT    Field IDs to extract
  --file PATH     Text file with field IDs
  --help          Show this message and exit.
```

## Usage Examples

### Converting CSV to Parquet

```bash
ukb-processor convert input.csv output.parquet --compression zstd
```

### Extracting Fields

There are three ways to specify which fields to extract:

1. Using command line arguments only:
```bash
ukb-processor extract data.parquet output.csv --fields 31 21022 21001
```

2. Using a text file only:
```bash
ukb-processor extract data.parquet output.csv --file fields.txt
```

3. Using both methods together:
```bash
ukb-processor extract data.parquet output.csv --fields 31 21022 --file fields.txt
```

When using both `--fields` and `--file`:
- Fields from both sources will be combined
- Duplicate fields are automatically removed
- You'll get a notification that fields were combined
- Any overlapping fields will be reported
- A complete list of processed fields will be shown

Example output when using both sources:
```
? Successfully extracted fields to output.csv

Note: Combined fields from both command line and file

Processed fields:
  * 21001
  * 21022
  * 31

The following fields were specified in both sources:
  * 31
```

Format for fields.txt file (one field ID per line):
```
31
21022
21001
```

## Python API

The package can be used programmatically:

```python
from ukb_processor import csv_to_parquet, extract_fields

# Convert CSV to Parquet
csv_to_parquet("ukb_data.csv", "ukb_data.parquet")

# Method 1: Using list of field IDs
extract_fields(
    "ukb_data.parquet",
    "extracted.csv",
    field_ids=["31", "21022"]
)

# Method 2: Using a file containing field IDs
extract_fields(
    "ukb_data.parquet",
    "extracted.csv",
    field_file="fields.txt"
)

# Method 3: Using both sources
extract_fields(
    "ukb_data.parquet",
    "extracted.csv",
    field_ids=["31", "21022"],
    field_file="fields.txt"
)
```