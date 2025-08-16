# DNA ETL Pipeline

A genetic data processing pipeline that extracts, transforms, and loads DNA sequences and associated metadata. The pipeline validates input files, processes DNA sequences, sanitizes metadata, and generates structured output files with support for both sequential and concurrent processing modes.

## Features

- **DNA Sequence Analysis**: GC content calculation, codon counting, and longest common subsequence detection
- **Metadata Processing**: Validation, sanitization, and privacy-aware metadata handling
- **Concurrent Processing**: Support for single file, sequential, and concurrent directory processing
- **Comprehensive Validation**: Age validation, date format validation, and data integrity checks

## Project Structure

```
DNA-ETL/
├── Main.py                     # Main pipeline entry point
├── Constants.py                # Configuration constants
├── requirements.txt            # Python dependencies
├── Pipeline/                   # Core ETL components
│   ├── ETLOrchestrator.py     # Main pipeline coordinator
│   ├── InputValidator.py      # Input validation logic
│   ├── DataExtractor.py       # Data extraction from files
│   ├── MetaDataValidator.py   # Metadata validation
│   ├── Loader.py              # Output generation
│   ├── Transform/             # Data transformation modules
│   │   ├── DNAProcessor.py    # DNA sequence processing
│   │   └── MetaDataProcessor.py # Metadata processing
│   └── DataModels/            # Data structure definitions
│       ├── DNAData.py         # DNA data models
│       └── ValidPaths.py      # Path validation models
├── Exceptions/                 # Custom exception handling
│   ├── ValidateExceptions.py  # Validation exceptions
│   ├── LoaderExceptions.py    # Loading exceptions
│   └── StatusCodeTranslator.py # Exception to status code mapping
├── ExampleData/               # Sample data for testing
│   ├── valid_inputs/          # Valid input configurations
│   ├── invalid_inputs/        # Invalid inputs for testing
│   └── participants/          # Sample participant data
└── tests/                     # Test suite
    ├── unit/                  # Unit tests
    ├── integration/           # Integration tests
    └── fixtures/              # Test data fixtures
```

## Quick Start

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd DNA-ETL
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Basic Usage

The pipeline supports three main execution modes:

#### Single File Processing
Processes a single input configuration file through the complete ETL pipeline.
```bash
python Main.py path/to/input_config.json
```

#### Sequential Directory Processing
Processes all JSON files in a directory one after another (default mode for directories).
```bash
python Main.py path/to/input_configs/ sequential
```

#### Concurrent Directory Processing
Processes all JSON files in a directory simultaneously using multiple threads. The number of worker threads is optional - if not specified, the system will automatically determine the optimal number based on your hardware.
```bash
# Let the system choose optimal thread count
python Main.py path/to/input_configs/ concurrent

# Specify custom thread count
python Main.py path/to/input_configs/ concurrent 4
```

#### Example with Sample Data
```bash
# Process a single sample
python Main.py ExampleData/valid_inputs/12ba71a0-30f4-464e-ba1b-9a31ea7d35fc_input.json

# Process all samples sequentially
python Main.py ExampleData/valid_inputs/ sequential

# Process all samples concurrently (auto thread count)
python Main.py ExampleData/valid_inputs/ concurrent

# Process all samples concurrently with 4 threads
python Main.py ExampleData/valid_inputs/ concurrent 4
```

## Input Configuration

Input files are JSON configurations that specify:
- Path to DNA sequence file
- Path to metadata file  
- Path for output file generation

Example input configuration:
```json
{
  "dna_file_path": "path/to/dna_sequences.txt",
  "metadata_file_path": "path/to/metadata.json",
  "output_file_path": "path/to/output/"
}
```

## Error Handling

The pipeline includes comprehensive error handling:

- **Input validation errors**: Invalid paths, missing files, malformed JSON
- **Data validation errors**: Invalid ages, dates, or metadata format
- **Processing errors**: DNA sequence analysis failures
- **Output errors**: File writing permissions, disk space

All errors include descriptive messages and appropriate status codes.