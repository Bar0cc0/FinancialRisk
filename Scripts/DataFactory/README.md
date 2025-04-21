# Financial DataFactory

A comprehensive ETL pipeline for financial data processing, transformation, and quality management.

## Overview

The Financial DataFactory is a flexible, extensible data processing pipeline designed to transform raw financial datasets into analysis-ready formats that comply with target database schemas. The system implements advanced ETL (Extract, Transform, Load) capabilities specialized for financial data including loans, fraud transactions, market data, and macroeconomic indicators.

## Features

- **Multi-stage Processing Pipeline**
  - Data loading from various file formats (CSV, Excel, JSON, Parquet)
  - Data cleaning and preprocessing
  - Schema transformation
  - Synthetic data generation for missing fields
  - Data validation and quality checks
  - Automated data fixing
  - Quality reporting

- **Domain-Specific Processing**
  - Loan data processing (credit scores, loan terms, payment patterns)
  - Fraud detection data preparation
  - Market data normalization (OHLC consistency)
  - Macroeconomic indicator processing

- **Advanced Data Management**
  - Missing value imputation using KNN
  - Outlier detection and handling
  - Categorical data encoding
  - Date/time standardization
  - Field mapping between source and target schemas
  - Identity column management

- **Performance & Scalability**
  - Caching system for intermediate results
  - Parallel processing capabilities
  - Configurable chunking for large datasets
  - Progress tracking and reporting

## Installation

### Prerequisites

- Python 3.8+
- pip or conda for package management
- SQL Server (for database integration)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/Bar0cc0/FinancialRisk.git
   cd FinancialRisk/Scripts/DataFactory
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   # On Windows
   venv\Scripts\activate
   # On macOS/Linux
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Operation

Run the engine with default configuration:

```bash
python engine.py
```

With custom configuration:

```bash
python engine.py --config custom_config.yaml --root /path/to/project --loglevel DEBUG
```


## Configuration

The system is configured through a YAML file with multiple sections:

```yaml
# Engine parameters
Engine_Parameters:
  input_dir: Datasets/Raw
  output_dir: Datasets/Cooked
  parallel_processing: False
  max_workers: auto

# Dataset definitions
Datasets:
  Loan:
    sources:
      - CreditLoan.csv
    field_mappings:
      - CustomerID: Customer_ID
      - LoanAmount: LoanAmount
```

### Key Configuration Options

- **Dataset Sources**: Define input files for each dataset type
- **Field Mappings**: Map source columns to target schema
- **Processing Parameters**: Configure cleaning, validation and transformation behavior
- **Performance Settings**: Manage parallelization and caching

## Pipeline Steps

1. **Data Loading**: Extract data from source files
2. **Data Cleaning**: Apply text, numeric, datetime, and categorical preprocessing
3. **Schema Transformation**: Transform source data to match target schema
4. **Data Validation**: Validate against schema and data quality rules
5. **Data Fixing**: Apply automatic corrections to identified issues
6. **Report Generation**: Create quality reports for processed datasets

## Extending the Framework

The DataFactory is designed to be extensible through strategy patterns:

- Create new preprocessing strategies in `processors.py`
- Define new validation rules in `validators.py`
- Add new fixing strategies in `fixers.py`
- Implement new transformations in `transformers.py`

### Example: Adding a New Validation Strategy

```python
class YourCustomValidator(ValidationStrategy):
    """Custom validation strategy."""
    
    def _validate_impl(self, df: pd.DataFrame) -> None:
        # Your validation logic here
        if condition_not_met:
            self._add_issue("Custom validation failed", "warning")
        else:
            self._tests_passed += 1
```

## License

MIT License

## Contributors

- Michael Garancher - Initial work and maintenance

## Acknowledgments

- Financial risk modeling best practices
- Data quality and ETL design patterns

---

*For questions and support, please open an issue on the GitHub repository.*
