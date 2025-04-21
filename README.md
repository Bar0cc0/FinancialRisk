# Financial Risk Management System

## Project Overview

The Financial Risk Management System is a comprehensive solution for processing, analyzing, and monitoring financial risk factors across multiple dimensions. This system integrates loan data, credit information, fraud detection patterns, market conditions, and macroeconomic indicators to provide a holistic view of financial risk.   
The DataFactory is a Python-based ETL orchestration engine that powers the data processing pipeline of the Financial Risk Management System, transforming raw financial data from diverse sources into quality-controlled, schema-compliant datasets that seamlessly integrate with the SQL Server warehouse architecture.  
Compliance with IFRS 9 standards is currently under development and available for review.

## Features

- **Multi-dimensional Risk Analysis**
  - Credit risk scoring
  - Fraud risk detection
  - Market risk evaluation
  - Aggregate risk profiling with trend analysis
  - Risk categorization (Low, Medium, High, Very High)
  - Risk trend tracking (Increasing, Stable, Decreasing)

- **Comprehensive Data Processing Pipeline**
  - Data loading from various sources (CSV, Excel, JSON, Parquet)
  - Data cleaning and preprocessing
  - Schema transformation
  - Synthetic data generation for missing fields
  - Quality validation and reporting
  - Data lineage tracking

- **Domain-Specific Processing**
  - Loan data processing (credit scores, loan terms, payment patterns)
  - Fraud detection data preparation
  - Market data normalization
  - Macroeconomic indicator processing

- **Advanced Data Management**
  - Missing value imputation
  - Outlier detection and handling
  - Categorical data encoding
  - Date/time standardization
  - Field mapping between source and target schemas

- **Performance & Reporting**
  - Reporting views and statistics generation
  - Data profiling and quality metrics
  - ETL process logging and monitoring
  - Caching system for intermediate results
  - Parallel processing capabilities

- **Warehouse architecture**
  - **Staging Layer**: Raw data landing and validation
  - **Production Layer**: Structured star schema with fact and dimension tables
  - **Reporting Layer**: Pre-aggregated views for dashboards and reports
  - **ETL Framework**: Tracking and logging of data processing steps


## IFRS 9 Compliance (Beta)
The system includes an IFRS 9 compliance module currently in beta development:

- **Development Status**: Not yet released
- **Implementation Date**: April 2025

IFRS 9 Features
- **Financial Instrument Classification**

- **Automated business model assessment**
	- SPPI (Solely Payments of Principal and Interest) testing
	- Classification into Amortized Cost, FVOCI, or FVPL categories
	- Expected Credit Loss (ECL) Calculation

- **Three-stage impairment model implementation**
	- PD (Probability of Default) modeling
	- LGD (Loss Given Default) calculation
	- EAD (Exposure at Default) tracking
	- Forward-looking economic scenario integration

- **Reporting & Disclosure**
	- Comprehensive ECL movement analysis
	- Stage migration tracking and reporting
	- Impairment dashboards and reporting views


## Installation

### Prerequisites
- SQL Server 16+
- PowerShell 5.1+
- Python 3.10+ with required packages

### Deployment Steps

1. Clone this repository

```powershell
git clone https://github.com/Bar0cc0/FinancialRisk.git
cd FinancialRisk
```

2. Run the deployment script with desired parameters:

```powershell
.\Scripts\DeployFinancialRisk.ps1
```

Optional parameters:
- `-ServerInstance`: SQL Server instance (default: "(local)")
- `-Database`: Target database name (default: "FinancialRiskDB")
- `-ScriptsPath`: Path to script files (default: script directory)
- `-LogPath`: Path for log files (default: parent of scripts/Logs)
- `-DataPath`: Path for data files (default: parent of scripts/Datasets/Cooked)
- `-NotificationEmail`: Email for notifications (default: "dba@financialrisk.com")
- `-DeleteExistingDB`: Whether to delete existing database (default: $true)
- `-SkipPythonCheck`: Skip Python environment check (default: $false)
- `-SkipDataFactory`: Skip DataFactory processing (default: $false)
- `-IsTest`: Run in test mode (default: $false)

### DataFactory Setup

If using the DataFactory component separately:

1. Navigate to DataFactory directory:
```bash
cd Scripts/DataFactory
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run the engine:
```bash
python engine.py --config config.yaml --loglevel INFO
```

## Usage

### Database Setup

The deployment script automatically creates the database schema, including:
- Staging tables for raw data
- Production fact and dimension tables
- Reporting views
- ETL logging tables

### Risk Score Calculation

Update risk scores for all customers:

```sql
EXEC Production.usp_UpdateRiskScores;
```

Refresh reporting views:

```sql
EXEC Production.usp_RefreshReportViews;
```

Query risk profiles:

```sql
SELECT * FROM Production.vRiskDashboard;
```

## Data Model

### Dimension Tables
- DimCustomer
- DimAccount
- DimDate
- Other dimension tables for various attributes

### Fact Tables
- FactLoan: Loan data and payment information
- FactCredit: Credit scores and history
- FactFraud: Fraud detection data
- FactRiskProfile: Aggregated risk scores
- Other fact tables for specific domains

## Risk Models

The system uses multiple risk models:

### Credit Risk
- Based on credit scores, credit utilization, payment history, etc.
- Weighted as 60% of aggregate risk score

### Fraud Risk
- Evaluates potential fraudulent activities
- Weighted as 15% of aggregate risk score

### Market Risk
- Analyzes market conditions impact on risk profiles
- Weighted as 25% of aggregate risk score

### Aggregate Risk
- Combines all risk dimensions with weighted scoring
- Categorized as Low, Medium, High, or Very High
- Tracks trends as Increasing, Stable, or Decreasing

## Testing

Unit tests are available in the `Scripts/Tests` folder:

```sql
USE FinancialRiskDB;
EXEC Tests.Unittests;
```

Tests validate:
- Database schema
- Data integrity
- Risk calculation logic
- ETL processing

## Dependencies

### SQL Server Features
- Database Mail (for notifications)
- SQL Server Agent (for scheduled jobs)

### Python Packages
- See `Scripts/DataFactory/requirements.txt` for Python dependencies

## Configuration

### Database Mail Setup
The deployment script configures SQL Server Database Mail for ETL notifications:
- Profile: "FinancialRisk Profile"
- Account: "FinancialRisk Mail Account"
- Recipient: Specified by `-NotificationEmail` parameter

### SQL Agent Jobs
A production job is created to run the ETL process on a schedule (see `Scripts/Production/SQLAgent_ProductionJob.sql`).

## Project Structure

```
Datasets/
    Raw/                      # Raw input datasets
Reports/
    DataLineage.svg           # Data lineage documentation
    Production_ERD.svg        # Production schema ERD
    Staging_ERD.svg           # Staging schema ERD
Scripts/
    ConfigDatabase.sql        # Database configuration
    DeployFinancialRisk.ps1   # Main deployment script
    DataFactory/              # Python-based data processing
    Production/               # Production SQL scripts
    Staging/                  # Staging SQL scripts
    Tests/                    # Test scripts
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
