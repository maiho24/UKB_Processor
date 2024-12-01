# UKB Processor

A Python tool for processing UK Biobank data files, with a focus on efficient data conversion and field extraction. This tool facilitates a two-step pipeline:

1. **Convert CSV files to Parquet format**: Ensures fast data loading and efficient storage.
2. **Extract specific fields**: Allows flexible extraction of relevant data from Parquet files, with options for filtering, handling empty rows, and selecting specific instances.

## Deprecation Notice
**Important**: With the introduction of the UK Biobank Research Analysis Platform (RAP), which provides native tools for data analysis and management, this package is no longer officially supported. We recommend using the RAP for working with UK Biobank data whenever possible. This package remains available for legacy purposes and offline workflows.

## Installation

Using Conda helps manage dependencies and ensures compatibility across different systems. We recommend using [Miniconda](https://docs.conda.io/en/latest/miniconda.html) to keep your environment lightweight.

### 1. Create and activate a new conda environment:
```bash
conda create -n ukb-processor
conda activate ukb-processor
```

### 2. Clone and install the package:
```bash
git clone https://github.com/maiho24/UKB_Processor.git
cd UKB_Processor
pip install -e .
```

**Note**: Make sure to always activate the environment before using the package:
```bash
conda activate ukb-processor
```

## Command Help

You can view help for any command using the `--help` or `-h` option:

**Main Help**
```bash
ukb-processor --help
```

**Command-Specific Help**
```bash
ukb-processor convert --help
ukb-processor extract --help
```

## Usage Examples

### General Workflow

1. Convert a UKB CSV file to a Parquet file:
```bash
ukb-processor convert input.csv output.parquet
```

2. Extract specific fields from the Parquet file:
```bash
ukb-processor extract data.parquet output.csv --fields 31 21022 21001
```

### Additional Options

* Define the field IDs in a .txt file (one field ID per line):
```bash
ukb-processor extract data.parquet output.csv --file fields.txt
```

* Use both methods (`--fields` & `--file`) together:
```bash
ukb-processor extract data.parquet output.csv --fields 31 21022 --file fields.txt
```

* Remove rows where all extracted fields are empty (excluding eid):
```bash
ukb-processor extract data.parquet output.csv --fields 31 21022 --remove-empty
```

* Extract specific instances:
```bash
ukb-processor extract data.parquet output.csv --fields 31 21022 --instance 1.0
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

## Contribution
Contributions are welcome! If you find a bug or have ideas for enhancements, feel free to open an issue or submit a pull request.
