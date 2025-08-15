# DNA ETL Pipeline

A comprehensive genetic data processing pipeline that extracts, transforms, and analyzes DNA sequences and associated metadata. The pipeline provides concurrent processing capabilities, extensive validation, and detailed performance benchmarking tools.

## Features

- **DNA Sequence Analysis**: GC content calculation, codon counting, and longest common subsequence detection
- **Metadata Processing**: Validation, sanitization, and privacy-aware metadata handling
- **Concurrent Processing**: Support for single file, sequential, and concurrent directory processing
- **Comprehensive Validation**: Age validation, date format validation, and data integrity checks
- **Performance Benchmarking**: Built-in tools for testing and comparing processing modes
- **Synthetic Data Generation**: Tools for creating realistic test datasets for development and testing

## Project Structure

```
DNA-ETL/
├── main.py                     # Main pipeline entry point
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
├── benchmark/                 # Performance testing tools
│   ├── benchmark_example.py   # CLI benchmark runner
│   ├── PerformanceBenchmark.py # Core benchmarking engine
│   └── SyntheticDataGenerator.py # Test data generator
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

#### Single File Processing
```bash
python main.py path/to/input_config.json
```

#### Directory Processing
```bash
# Sequential processing (default)
python main.py path/to/input_configs/ sequential

# Concurrent processing
python main.py path/to/input_configs/ concurrent
```

#### Example with Sample Data
```bash
# Process a single sample
python main.py ExampleData/valid_inputs/12ba71a0-30f4-464e-ba1b-9a31ea7d35fc_input.json

# Process all samples sequentially
python main.py ExampleData/valid_inputs/ sequential

# Process all samples concurrently
python main.py ExampleData/valid_inputs/ concurrent
```

## Performance Benchmarking

The pipeline includes comprehensive benchmarking tools to analyze performance across different processing modes.

### Running Benchmarks

Navigate to the benchmark directory and run:

```bash
cd benchmark

# Quick test (2-3 files)
python benchmark_example.py -q

# Medium test (5-20 files)
python benchmark_example.py -m

# Large test (10-50 files)
python benchmark_example.py -l

# Extra large test (20-100 files)
python benchmark_example.py -xl

# Full scaling analysis
python benchmark_example.py -s
```

### Benchmark Features

- **Automatic synthetic data generation** for consistent testing
- **Performance comparison** across single file, sequential, and concurrent modes
- **Detailed timing analysis** with throughput calculations
- **Automatic cleanup** of test data
- **Performance recommendations** based on dataset size

### Sample Benchmark Output

```
PERFORMANCE COMPARISON REPORT
====================================

THROUGHPUT COMPARISON (Files/Second)
Dataset Size    Single File    Sequential     Concurrent     
------------------------------------------------------------
20              17.42          277.07         265.50         
50              18.08          481.40         480.89         
100             17.66          643.68         521.74         

RECOMMENDATIONS
• Sequential processing fastest up to 100 files
• Use concurrent processing for datasets with 50+ files (situational)
• Batch processing is 36x faster than single file processing
```

## Error Handling

The pipeline includes comprehensive error handling:

- **Input validation errors**: Invalid paths, missing files, malformed JSON
- **Data validation errors**: Invalid ages, dates, or metadata format
- **Processing errors**: DNA sequence analysis failures
- **Output errors**: File writing permissions, disk space

All errors include descriptive messages and appropriate status codes.