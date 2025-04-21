#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Financial Data Validation Framework
===================================

This module provides a comprehensive framework for validating financial datasets against 
schema definitions, data quality standards, and domain-specific business rules. It implements 
a strategy pattern and pipeline architecture for flexible validation and reporting.

The module supports validation of various quality aspects including schema compliance, 
missing values, data types, outliers, duplicates, and domain-specific business rules for 
loan, market, fraud, and macroeconomic data.

Key Components
--------------
- Validation Strategies: Implementations for specific validation tasks
	- SchemaValidationStrategy: Verifies column presence and data types
	- DataQualityValidationStrategy: Checks for missing values, outliers, and duplicates
	- DomainSpecificValidationStrategy: Validates domain-specific business rules
- Strategy Factory: Creates and manages validation strategy instances
- Pipeline Steps: Wraps strategies for execution in a pipeline context
- Validation Pipeline: Orchestrates sequential execution of validation steps
- Validator API: Main entry point for client code to access validation functionality
- DataQualityReporter: Generates comprehensive validation reports with visualizations

Domain-Specific Capabilities
----------------------------
- Loan Data: Credit score ranges, loan-to-income ratios, interest rate verification
- Market Data: Price relationship validation, OHLC consistency
- Fraud Data: Transaction indicator validation
- Macro Data: Economic indicator range validation

Usage Example
-------------
from configuration import Configuration
from validators import Validator
import pandas as pd

# Initialize configuration
config = Configuration()
config.load_from_file("config.yaml")

# Load dataset to validate
df = pd.read_csv("financial_data.csv")

# Validate the dataset using the module API
validator = Validator(config)
results = validator.load_dataset("Loan", df).process()

# Generate validation report
validator.generate_report()
"""

#=================================================

from __future__ import annotations
import threading
import traceback
import matplotlib
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path
import pandas as pd
import json	
from abc import abstractmethod
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
from functools import lru_cache

from interfaces import (
	IDataValidationStrategy, IDataQualityReporter, 
	IConfigProvider, IPipelineStep
)

# Use a non-interactive backend for saving plots
matplotlib.use('Agg')  


#======= 1. Validation Strategies =======
class ValidationStrategy(IDataValidationStrategy):
	"""Base class for all validation strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._issues = []
		self._results = {
			"status": "passed",
			"issues": [],
			"tests_passed": 0,
			"tests_failed": 0,
			"quality_metrics": {}
		}
	
	@property
	def name(self) -> str:
		"""Return the name of the validation strategy."""
		return self.__class__.__name__
	
	def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
		"""Validate the dataframe according to the strategy."""
		self._issues = []
		self._results = {
			"status": "passed",
			"issues": [],
			"tests_passed": 0,
			"tests_failed": 0,
			"quality_metrics": {}
		}
		
		try:
			# Call implementation-specific validation
			self._validate_impl(df)
			
			# Calculate quality metrics
			self._calculate_quality_metrics(df)
			
			# Determine overall status based on issues
			if any(i.get("severity") == "critical" for i in self._issues):
				self._results["status"] = "failed"
			elif any(i.get("severity") == "warning" for i in self._issues):
				self._results["status"] = "warning"
			
			self._results["issues"] = self._issues
			return self._results
		except Exception as e:
			self._logger.error(f"Error in validation strategy {self.name}: {e}")
			traceback.print_exc()
			return {
				"status": "error",
				"issues": [{
					"type": "exception",
					"message": str(e),
					"severity": "critical"
				}],
				"tests_passed": 0, 
				"tests_failed": 1,
				"quality_metrics": {}
			}
	
	@abstractmethod
	def _validate_impl(self, df: pd.DataFrame) -> None:
		"""Implementation-specific validation logic."""
		pass
	
	def _calculate_quality_metrics(self, df: pd.DataFrame) -> None:
		"""Calculate quality metrics specific to this validation strategy."""
		# Default implementation - subclasses should override this
		pass
	
	def _add_issue(self, issue_type: str, message: str, severity: str = "warning", 
				field: Optional[str] = None, details: Any = None) -> None:
		"""Add a validation issue."""
		self._issues.append({
			"type": issue_type,
			"message": message,
			"severity": severity,
			"field": field,
			"details": details
		})
		
		self._results["tests_failed"] += 1
		
	def _test_passed(self) -> None:
		"""Record a passed test."""
		self._results["tests_passed"] += 1


class SchemaValidationStrategy(ValidationStrategy):
	"""Validates that the dataframe conforms to the expected schema."""
	
	def _validate_impl(self, df: pd.DataFrame) -> None:
		"""Validate schema compliance."""
		if df.empty:
			self._add_issue("empty_dataset", "Dataset is empty", "critical")
			return
		
		# Check if all required columns are present
		required_columns = self._get_required_columns()
		missing_columns = [col for col in required_columns if col not in df.columns]
		
		if missing_columns:
			self._add_issue(
				"missing_columns", 
				f"Missing required columns: {', '.join(missing_columns)}", 
				"critical", 
				details=missing_columns
			)
		else:
			self._test_passed()
		
		# Check data types
		expected_types = self._get_column_data_types()
		for col, expected_type in expected_types.items():
			if col in df.columns:
				if not self._check_column_type(df[col], expected_type):
					self._add_issue(
						"type_mismatch",
						f"Column '{col}' expected type {expected_type}, got {df[col].dtype}",
						"warning",
						field=col,
						details={"expected": expected_type, "actual": str(df[col].dtype)}
					)
				else:
					self._test_passed()
	
	@lru_cache(maxsize=32)
	def _get_schema_for_dataset(self, dataset_name: str) -> Dict[str, Dict[str, str]]:
		"""Get schema with caching to avoid repeated config lookups."""
		return self._config.get_schema_for_dataset(dataset_name)

	def _get_required_columns(self) -> List[str]:
		"""Get list of required columns based on dataset type."""
		# This would normally be driven by configuration, but here we're hardcoding for simplicity
		dataset_name = self._config.get_config("current_dataset", "")
		schema = self._get_schema_for_dataset(dataset_name)
	
		if dataset_name and dataset_name in schema:
			return list(schema[dataset_name].keys())
		
		# Default required columns
		return []
	
	def _get_column_data_types(self) -> Dict[str, str]:
		"""Get expected data types for columns."""
		dataset_name = self._config.get_config("current_dataset", "")
		schema = self._get_schema_for_dataset(dataset_name)
		
		if dataset_name and dataset_name in schema:
			return schema[dataset_name]
		
		# Default empty dict
		return {}
	
	def _check_column_type(self, series: pd.Series, expected_type: str) -> bool:
		"""Check if a column has the expected type."""
		# Extract base type without parameters
		base_type = expected_type.split('(')[0].upper() if '(' in expected_type else expected_type.upper()
		
		# Convert SQL types to pandas/numpy types for comparison
		if "INT" in base_type:
			return pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series)
		elif "DECIMAL" in base_type or "NUMERIC" in base_type or "FLOAT" in base_type:
			return pd.api.types.is_float_dtype(series) or pd.api.types.is_numeric_dtype(series)
		elif "DATE" in base_type:
			return pd.api.types.is_datetime64_dtype(series) or pd.api.types.is_datetime64_any_dtype(series)
		elif "VARCHAR" in base_type or "NVARCHAR" in base_type or "CHAR" in base_type:
			return pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series)
		elif "BIT" in base_type:
			return (pd.api.types.is_bool_dtype(series) or 
					(pd.api.types.is_numeric_dtype(series) and series.isin([0, 1, True, False]).all()))
		
		# Default to True for unknown types
		return True


class DataQualityValidationStrategy(ValidationStrategy):
	"""Validates basic data quality aspects like missing values, duplicates, and outliers."""
	
	def _validate_impl(self, df: pd.DataFrame) -> None:
		"""Validate data quality."""
		if df.empty:
			self._add_issue("empty_dataset", "Dataset is empty", "critical")
			return
		
		# Check for missing values
		self._check_missing_values(df)
		
		# Check for duplicates
		self._check_duplicates(df)
		
		# Check for outliers
		self._check_outliers(df)
		
		# Check for data consistency
		self._check_data_consistency(df)
	
	def _check_missing_values(self, df: pd.DataFrame) -> None:
		"""Check for missing values in the dataframe."""
		max_missing_pct = self._config.get_config("max_missing_pct", 0.5)
		
		# Calculate missing percentages for each column
		missing_pct = df.isnull().mean()
		
		# Check for columns with excessive missing values
		problem_columns = missing_pct[missing_pct > max_missing_pct].index.tolist()
		
		if problem_columns:
			details = {col: float(missing_pct[col]) for col in problem_columns}
			self._add_issue(
				"high_missing_values",
				f"High percentage of missing values in {len(problem_columns)} columns",
				"warning",
				details=details
			)
		else:
			self._test_passed()
		
		# Check for rows with excessive missing values
		row_missing_pct = df.isnull().mean(axis=1)
		problem_rows = row_missing_pct[row_missing_pct > max_missing_pct].index.tolist()
		
		if problem_rows and len(problem_rows) / len(df) > 0.1:  # If more than 10% of rows have issues
			self._add_issue(
				"high_missing_rows",
				f"{len(problem_rows)} rows ({(len(problem_rows) / len(df) * 100):.1f}%) have excessive missing values",
				"warning",
				details={"count": len(problem_rows), "percentage": (len(problem_rows) / len(df) * 100)}
			)
		else:
			self._test_passed()
	
	def _check_duplicates(self, df: pd.DataFrame) -> None:
		"""Check for duplicate rows in the dataframe."""
		duplicate_count = df.duplicated().sum()
		
		if duplicate_count > 0:
			self._add_issue(
				"duplicates",
				f"Found {duplicate_count} duplicate rows ({(duplicate_count / len(df) * 100):.1f}% of dataset)",
				"warning",
				details={"count": int(duplicate_count), "percentage": (duplicate_count / len(df) * 100)}
			)
		else:
			self._test_passed()
	
	def _check_outliers(self, df: pd.DataFrame) -> None:
		"""Check for outliers using vectorized operations."""
		outlier_threshold = self._config.get_config("outlier_threshold", 3.0)
		
		# Only check numeric columns
		numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
		
		# Filter out ID columns and boolean-like columns in one pass
		valid_cols = []
		for col in numeric_cols:
			if "id" not in col.lower() and df[col].nunique() > 2 and df[col].isnull().mean() <= 0.5:
				valid_cols.append(col)
		
		if not valid_cols:
			self._test_passed()
			return
		
		# Calculate quartiles in a vectorized operation
		q1 = df[valid_cols].quantile(0.25)
		q3 = df[valid_cols].quantile(0.75)
		iqr = q3 - q1
		
		# Remove columns with zero IQR
		valid_cols = [col for col in valid_cols if iqr[col] > 0]
		if not valid_cols:
			self._test_passed()
			return
		
		# Calculate bounds for all columns at once
		lower_bounds = q1 - outlier_threshold * iqr
		upper_bounds = q3 + outlier_threshold * iqr
		
		# Compute outlier percentages efficiently
		outliers_by_column = {}
		
		for col in valid_cols:
			# Vectorized outlier detection
			outlier_mask = (df[col] < lower_bounds[col]) | (df[col] > upper_bounds[col])
			outlier_count = outlier_mask.sum()
			valid_count = df[col].count()
			
			if valid_count > 0:
				outlier_pct = outlier_count / valid_count
				if outlier_pct > 0.05:  # More than 5% are outliers
					outliers_by_column[col] = {
						"count": int(outlier_count),
						"percentage": outlier_pct * 100,
						"lower_bound": float(lower_bounds[col]),
						"upper_bound": float(upper_bounds[col])
					}
		
		if outliers_by_column:
			self._add_issue(
				"outliers",
				f"Found high outlier percentage in {len(outliers_by_column)} columns",
				"warning",
				details=outliers_by_column
			)
		else:
			self._test_passed()
	
	def _check_data_consistency(self, df: pd.DataFrame) -> None:
		"""Check for data consistency across related fields."""
		# Check for consistent dates
		date_cols = [col for col in df.columns if 'date' in col.lower()]
		
		for col in date_cols:
			if col in df.columns and not df[col].empty and pd.api.types.is_datetime64_any_dtype(df[col]):
				# Check for future dates
				future_dates = (df[col] > datetime.now()).sum()
				if future_dates > 0:
					self._add_issue(
						"future_dates",
						f"Found {future_dates} future dates in column '{col}'",
						"warning",
						field=col,
						details={"count": int(future_dates), "percentage": (future_dates / df[col].count() * 100)}
					)
				else:
					self._test_passed()
		
		# Check for age consistency
		if 'Age' in df.columns and not df['Age'].empty:
			invalid_ages = ((df['Age'] < 18) | (df['Age'] > 100)).sum()
			if invalid_ages > 0:
				self._add_issue(
					"invalid_ages",
					f"Found {invalid_ages} invalid ages (< 18 or > 100)",
					"warning",
					field="Age",
					details={"count": int(invalid_ages), "percentage": (invalid_ages / df['Age'].count() * 100)}
				)
			else:
				self._test_passed()


class DomainSpecificValidationStrategy(ValidationStrategy):
	"""Validates domain-specific business rules for financial data."""
	
	def _validate_impl(self, df: pd.DataFrame) -> None:
		"""Validate domain-specific rules."""
		if df.empty:
			self._add_issue("empty_dataset", "Dataset is empty", "critical")
			return
			
		dataset_name = self._config.get_config("current_dataset", "")
		
		# Apply dataset-specific validations
		if dataset_name == "Loan":
			self._validate_loan_data(df)
		elif dataset_name == "Fraud":
			self._validate_fraud_data(df)
		elif dataset_name == "Market":
			self._validate_market_data(df)
		elif dataset_name == "Macro":
			self._validate_macro_data(df)
	
	def _validate_loan_data(self, df: pd.DataFrame) -> None:
		"""Validate loan-specific data rules."""
		# Validate credit score range
		if 'CreditScore' in df.columns:
			invalid_scores = ((df['CreditScore'] < 300) | (df['CreditScore'] > 850)).sum()
			if invalid_scores > 0:
				self._add_issue(
					"invalid_credit_scores",
					f"Found {invalid_scores} invalid credit scores (not between 300-850)",
					"warning",
					field="CreditScore"
				)
			else:
				self._test_passed()
		
		# Validate loan amount vs income ratio
		if 'LoanAmount' in df.columns and 'AnnualIncome' in df.columns:
			# Check if loan amount is greater than 10x annual income
			high_loan_ratio = ((df['LoanAmount'] > 10 * df['AnnualIncome']) & 
							(df['AnnualIncome'] > 0)).sum()
			if high_loan_ratio > 0:
				self._add_issue(
					"high_loan_to_income_ratio",
					f"Found {high_loan_ratio} loans with amount > 10x annual income",
					"warning",
					details={"count": int(high_loan_ratio)}
				)
			else:
				self._test_passed()
		
		# Validate interest rates
		if 'InterestRate' in df.columns:
			invalid_rates = ((df['InterestRate'] < 0) | (df['InterestRate'] > 100)).sum()
			if invalid_rates > 0:
				self._add_issue(
					"invalid_interest_rates",
					f"Found {invalid_rates} invalid interest rates (not between 0-100%)",
					"warning",
					field="InterestRate"
				)
			else:
				self._test_passed()
	
	def _validate_fraud_data(self, df: pd.DataFrame) -> None:
		"""Validate fraud-specific data rules."""
		# Validate transaction amounts
		if 'TransactionAmount' in df.columns:
			negative_amounts = (df['TransactionAmount'] < 0).sum()
			if negative_amounts > 0:
				self._add_issue(
					"negative_transaction_amounts",
					f"Found {negative_amounts} negative transaction amounts",
					"warning",
					field="TransactionAmount"
				)
			else:
				self._test_passed()
		
		# Validate fraud indicators
		for col in ['IsFraudulent', 'IsOnlineTransaction', 'IsUsedChip', 'IsUsedPIN']:
			if col in df.columns:
				invalid_values = (~df[col].isin([0, 1])).sum()
				if invalid_values > 0:
					self._add_issue(
						"invalid_binary_indicators",
						f"Found {invalid_values} invalid values in {col} (not 0 or 1)",
						"warning",
						field=col
					)
				else:
					self._test_passed()
	
	def _validate_market_data(self, df: pd.DataFrame) -> None:
		"""Validate market-specific data rules."""
		# Validate price columns (always >= 0)
		price_columns = ['OpenValue', 'CloseValue', 'HighestValue', 'LowestValue', 'GoldPrice', 'OilPrice']
		
		for col in price_columns:
			if col in df.columns:
				# Handle null values in the comparison
				negative_prices = (df[col] < 0).fillna(False).sum()
				if negative_prices > 0:
					self._add_issue(
						"negative_prices",
						f"Found {negative_prices} negative values in {col}",
						"warning",
						field=col
					)
				else:
					self._test_passed()
		
		# Validate high/low price relationship
		if 'HighestValue' in df.columns and 'LowestValue' in df.columns:
			# Handle null values in the comparison
			invalid_high_low = (df['HighestValue'] < df['LowestValue']).fillna(False).sum()
			if invalid_high_low > 0:
				self._add_issue(
					"invalid_high_low_relationship",
					f"Found {invalid_high_low} cases where HighestValue < LowestValue",
					"warning"
				)
			else:
				self._test_passed()
		
		# Validate open/close within high/low range
		if all(col in df.columns for col in ['OpenValue', 'CloseValue', 'HighestValue', 'LowestValue']):
			# Create a mask for rows where all required columns have non-null values
			valid_rows = (
				df['OpenValue'].notna() & 
				df['CloseValue'].notna() & 
				df['HighestValue'].notna() & 
				df['LowestValue'].notna()
			)
			
			# Only check rows with complete data
			if valid_rows.any():
				# Now perform the comparison only on valid rows
				out_of_range = (
					(df.loc[valid_rows, 'OpenValue'] > df.loc[valid_rows, 'HighestValue']) | 
					(df.loc[valid_rows, 'OpenValue'] < df.loc[valid_rows, 'LowestValue']) | 
					(df.loc[valid_rows, 'CloseValue'] > df.loc[valid_rows, 'HighestValue']) | 
					(df.loc[valid_rows, 'CloseValue'] < df.loc[valid_rows, 'LowestValue'])
				).sum()
				
				if out_of_range > 0:
					self._add_issue(
						"prices_out_of_range",
						f"Found {out_of_range} cases where Open/Close are outside High/Low range",
						"warning"
					)
				else:
					self._test_passed()
			else:
				# No rows with complete price data
				self._logger.info("No rows with complete price data for Open/Close/High/Low validation")
				self._test_passed()
	
	def _validate_macro_data(self, df: pd.DataFrame) -> None:
		"""Validate macro-economic data rules."""
		# Validate unemployment rate range
		if 'UnemploymentRate' in df.columns:
			invalid_rates = ((df['UnemploymentRate'] < 0) | (df['UnemploymentRate'] > 100)).sum()
			if invalid_rates > 0:
				self._add_issue(
					"invalid_unemployment_rates",
					f"Found {invalid_rates} invalid unemployment rates (not between 0-100%)",
					"warning",
					field="UnemploymentRate"
				)
			else:
				self._test_passed()
		
		# Validate GDP (should be positive)
		if 'GDP' in df.columns:
			negative_gdp = (df['GDP'] <= 0).sum()
			if negative_gdp > 0:
				self._add_issue(
					"negative_gdp",
					f"Found {negative_gdp} cases with zero or negative GDP",
					"warning",
					field="GDP"
				)
			else:
				self._test_passed()
		
		# Validate inflation rate (reasonable range)
		if 'InflationRate' in df.columns:
			extreme_inflation = ((df['InflationRate'] < -20) | (df['InflationRate'] > 100)).sum()
			if extreme_inflation > 0:
				self._add_issue(
					"extreme_inflation_rates",
					f"Found {extreme_inflation} extreme inflation rates (< -20% or > 100%)",
					"warning",
					field="InflationRate"
				)
			else:
				self._test_passed()


class DataQualityReporter(IDataQualityReporter):
	"""Generate comprehensive data quality reports based on validation results."""

	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._dataset_name = None
		self._last_report_key = None  # Track last generated report to prevent duplicates
		
		# Get root directory from configuration instead of calculating it here
		root_dir = Path(self._config.get_config('root_dir'))
		report_dir_name = self._config.get_config('report_dir', 'Reports')
		
		# Construct the report directory path
		if isinstance(report_dir_name, Path):
			self.report_dir = report_dir_name / 'DataFactoryReports'
		else:
			self.report_dir = root_dir / report_dir_name / 'DataFactoryReports'
		
		if not self.report_dir.exists():
			self.report_dir.mkdir(parents=True, exist_ok=True)
		
		# Add a thread lock for matplotlib operations
		self._plot_lock = threading.Lock()
	
	def set_dataset_name(self, dataset_name: str) -> None:
		"""Set the dataset name for reporting."""
		self._dataset_name = dataset_name

	def get_report_dir(self) -> Path:
		"""Get the report directory."""
		return self.report_dir
	
	def generate_report(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> None:
		"""Generate data quality reports based on validation results with deduplication."""
		if not self._dataset_name:
			self._logger.error("Dataset name not set for quality report")
			return

		# Allow override of 'already_generated' when explicitly called
		force_regenerate = validation_results.get('force_regenerate', False)
		already_generated = validation_results.get('report_generated', False)
		
		if already_generated and not force_regenerate:
			self._logger.info(f"Report for {self._dataset_name} already generated, skipping")
			return

		# Create a unique key based on dataset name, validation pass and report_generated flag
		validation_pass = validation_results.get('validation_pass', 0)
		already_generated = validation_results.get('report_generated', False)
		report_key = f"{self._dataset_name}_{validation_pass}"
		
		# Skip if this is the same report we just generated OR if report was already generated
		if self._last_report_key == report_key and not force_regenerate:
			self._logger.info(f"Skipping duplicate report generation for {report_key}")
			return

		# Update tracking 	
		self._last_report_key = report_key
		
		# Set report_generated flag to prevent duplicate generation
		validation_results['report_generated'] = True
		
		# Existing report generation logic continues
		self._log_validation_summary(validation_results)

		# Generate statistical summary report
		if self._config.get_config('statistical_report', False):
			self._generate_statistical_summary(df, validation_results.get('quality_metrics', {}))
			self._logger.info(f"Statistical summary for {self._dataset_name} generated")
		
		# Determine if we should save JSON report
		if self._config.get_config('validation_report', False):
			self._save_validation_json(df, validation_results)
			self._logger.info(f"Validation JSON report for {self._dataset_name} generated")
		
		# If data quality reports are enabled, generate visualizations and summaries
		if self._config.get_config('data_quality_report', False):
			self._generate_quality_reports(df, validation_results)
			self._logger.info(f"Data quality report for {self._dataset_name} generated")
	
	def _log_validation_summary(self, validation_results: Dict[str, Any]) -> None:
		"""Log a summary of validation results."""
		validation_pass = validation_results.get('validation_pass', 0)
		status = validation_results.get('status', 'unknown')
		
		if status == 'passed':
			self._logger.info(f'Validation pass #{validation_pass}: Data validation passed')
		else:
			issue_count = validation_results.get('issues_count', 0)
			quality_score = validation_results.get('dataset_quality_score', 0)
			self._logger.warning(f"Validation pass #{validation_pass}: Data validation {status} with {issue_count} issues")
			self._logger.warning(f"Quality score: {quality_score:.1f}/100.0")
		
			# Log quality metrics if available
			quality_metrics = validation_results.get('quality_metrics', {})
			if quality_metrics:
				metrics_str = ", ".join(f"{k}: {v:.1f}%" for k, v in quality_metrics.items())
				self._logger.debug(f"Quality metrics: {metrics_str}")
	
	def _save_validation_json(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> None:
		"""Save validation results as JSON."""
		if df.empty:
			self._logger.warning("Dataframe is empty, skipping JSON report generation")
			return
		
		try:				
			output_path = self.report_dir / f"{self._dataset_name}_validation_report.json"
			
			# Add dataset info
			validation_results['dataset_name'] = self._dataset_name
			validation_results['row_count'] = len(df)
			validation_results['column_count'] = len(df.columns)
			
			# Save to JSON
			with open(output_path, 'w') as f:
				json.dump(validation_results, f, indent=2, default=str)
			
		except Exception as e:
			self._logger.error(f"Error saving validation results for dataset {self._dataset_name}: {e}")
			traceback.print_exc()

	def _generate_statistical_summary(self, df: pd.DataFrame, quality_metrics: Dict[str, float]) -> None:
		"""Generate statistical summary report."""
		try:
			summary_path = self.report_dir / f"{self._dataset_name}_summary.txt"
			
			with open(summary_path, 'w') as f:
				f.write(f"Dataset: {self._dataset_name}\n")
				f.write(f"Shape: {df.shape}\n\n")
				
				# Write quality metrics
				if quality_metrics:
					f.write("Quality Metrics:\n")
					for metric, value in quality_metrics.items():
						f.write(f"  {metric}: {value:.2f}%\n")
					f.write("\n")
					
				f.write("Data Types:\n")
				for dtype, count in df.dtypes.value_counts().items():
					f.write(f"  {dtype}: {count} columns\n")
					
				f.write("\nDescriptive Statistics:\n")
				stats = df.describe(include='all').transpose()
				f.write(stats.to_string())
				
				# Add additional useful statistics
				f.write("\n\nMissing Values Summary:\n")
				missing_vals = df.isnull().sum()
				missing_pct = df.isnull().mean() * 100
				missing_stats = pd.DataFrame({
					'Missing Count': missing_vals,
					'Missing Percentage': missing_pct
				}).sort_values('Missing Percentage', ascending=False)
				f.write(missing_stats[missing_stats['Missing Count'] > 0].to_string())
				
				f.write("\n\nUnique Values Count:\n")
				unique_vals = {col: df[col].nunique() for col in df.columns}
				sorted_uniques = pd.Series(unique_vals).sort_values(ascending=False)
				f.write(sorted_uniques.to_string())
				
		except Exception as e:
			self._logger.error(f"Error generating statistical summary: {e}")
			traceback.print_exc()

	def _generate_quality_reports(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> None:
		"""Generate all visualizations in a single PDF with thread-safe figure handling."""
		if df.empty:
			self._logger.warning("Dataframe is empty, skipping report generation")
			return
		
		try:
			# Extract data from validation results
			quality_metrics = validation_results.get("quality_metrics", {})
			
			# Get report configuration
			max_columns = self._config.get_config('report_max_columns', 10)
			plot_size = self._config.get_config('report_plot_size', (12, 10))
			
			# Create a single PDF with all visualizations
			pdf_path = self.report_dir / f"{self._dataset_name}_quality_report.pdf"
			
			# Create a fresh figure for each visualization and add to PDF
			with self._plot_lock:
				# Use a dedicated non-interactive backend
				import matplotlib
				matplotlib.use('Agg', force=True)
				
				# Import pyplot only after setting backend
				import matplotlib.pyplot as plt
				from matplotlib.backends.backend_pdf import PdfPages
				
				# Complete isolation approach - create a new PDF
				with PdfPages(pdf_path) as pdf:
					# Title page
					self._add_title_page(pdf, self._dataset_name)
					
					# Individual visualizations with isolated figure handling
					self._add_numeric_distributions(df, pdf, max_columns, plot_size)
					self._add_correlation_matrix(df, pdf, max_columns, plot_size)
					self._add_missing_values_chart(df, pdf, plot_size)
				
			# Generate text-based summaries separately
			self._generate_statistical_summary(df, quality_metrics)	
			
		except Exception as e:
			self._logger.error(f"Failed to generate data quality report for dataset {self._dataset_name}: {e}")
			traceback.print_exc()

	def _add_title_page(self, pdf, dataset_name):
		"""Create a title page for the report."""
		import matplotlib.pyplot as plt
		
		fig = plt.figure(figsize=(8, 6))
		plt.text(0.5, 0.6, f"Data Quality Report", ha='center', fontsize=24)
		plt.text(0.5, 0.5, f"Dataset: {dataset_name}", ha='center', fontsize=18)
		plt.text(0.5, 0.4, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ha='center')
		plt.axis('off')
		pdf.savefig(fig)
		plt.close(fig)

	def _add_numeric_distributions(self, df, pdf, max_columns, plot_size):
		# Import locally to ensure thread safety
		import matplotlib.pyplot as plt
		import numpy as np
		
		# Get numeric columns
		selected_cols = self._config.get_config('report_numeric_columns', None)
		if selected_cols:
			numeric_cols = [col for col in selected_cols if col in df.columns 
						and pd.api.types.is_numeric_dtype(df[col])]
		else:
			numeric_cols = list(df.select_dtypes(include=[np.number]).columns[:max_columns])
		
		# Check for empty list
		if not numeric_cols:
			self._logger.warning("No numeric columns found for distribution plots")
			fig = plt.figure(figsize=(8, 6))
			plt.text(0.5, 0.5, "No numeric columns available for distribution plots", 
					ha='center', va='center')
			pdf.savefig(fig)
			plt.close(fig)
			return
		
		# Calculate grid dimensions
		n_cols = min(3, len(numeric_cols))
		n_rows = (len(numeric_cols) + n_cols - 1) // n_cols
		
		# Create all subplots at once - the key fix
		fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=plot_size, squeeze=False)
		fig.suptitle("Numeric Distributions", y=0.98)
		
		# Plot each column in its own subplot
		for i, col in enumerate(numeric_cols):
			if i >= n_rows * n_cols:  # Safety check
				break
				
			# Calculate row and column indices
			row_idx = i // n_cols
			col_idx = i % n_cols
			
			# Access the correct axis object
			ax = axes[row_idx, col_idx]
			
			# Generate histogram directly with matplotlib
			try:
				data = df[col].dropna().values
				if len(data) > 0:
					# Use plain matplotlib histogram instead of pandas
					ax.hist(data, bins=min(30, len(np.unique(data))))
					ax.set_title(col)
				else:
					ax.text(0.5, 0.5, f"No data for {col}", ha='center', va='center')
			except Exception as e:
				self._logger.error(f"Error plotting {col}: {e}")
				ax.text(0.5, 0.5, f"Error plotting {col}", ha='center', va='center')
		
		# Hide any unused subplots
		for i in range(len(numeric_cols), n_rows * n_cols):
			row_idx = i // n_cols
			col_idx = i % n_cols
			axes[row_idx, col_idx].set_visible(False)
		
		fig.tight_layout(rect=[0, 0, 1, 0.96])  # Leave room for suptitle
		pdf.savefig(fig)
		plt.close(fig)

	def _add_correlation_matrix(self, df, pdf, max_columns, plot_size):
		"""Add correlation matrix to PDF with isolated figure handling."""
		import matplotlib.pyplot as plt
		import numpy as np
		import seaborn as sns
		
		# Get correlation columns
		selected_cols = self._config.get_config('report_correlation_columns', None)
		if selected_cols:
			corr_cols = [col for col in selected_cols if col in df.columns 
					and pd.api.types.is_numeric_dtype(df[col])]
		else:
			corr_cols = list(df.select_dtypes(include=[np.number]).columns[:max_columns])
		
		# Need at least 2 columns for correlation
		if len(corr_cols) < 2:
			self._logger.warning("Not enough numeric columns for correlation matrix")
			fig = plt.figure(figsize=(8, 6))
			plt.text(0.5, 0.5, "Not enough numeric columns for correlation matrix", 
					ha='center', va='center')
			pdf.savefig(fig)
			plt.close(fig)
			return
		
		try:
			# Create fresh figure and axis
			fig = plt.figure(figsize=plot_size)
			ax = fig.add_subplot(111)
			
			# Calculate correlation matrix
			corr = df[corr_cols].corr()
			
			# Create mask for the upper triangle
			mask = np.triu(np.ones_like(corr, dtype=bool))
			
			# Generate heatmap
			sns.heatmap(
				corr, 
				mask=mask, 
				cmap='coolwarm', 
				vmin=-1, 
				vmax=1, 
				annot=self._config.get_config('show_correlation_values', False),
				fmt=".2f", 
				square=True, 
				ax=ax
			)
			
			ax.set_title("Correlation Matrix")
			fig.tight_layout()
			
			# Save and close
			pdf.savefig(fig)
			plt.close(fig)
			
		except Exception as e:
			self._logger.error(f"Error generating correlation matrix: {e}")
			fig = plt.figure(figsize=(8, 6))
			plt.text(0.5, 0.5, f"Error generating correlation matrix:\n{str(e)}", 
					ha='center', va='center')
			pdf.savefig(fig)
			plt.close(fig)

	def _add_missing_values_chart(self, df, pdf, plot_size):
		"""Add missing values chart to PDF with isolated figure handling."""
		import matplotlib.pyplot as plt
		import numpy as np
		
		# Exclude lineage columns
		lineage_columns = ['LoadBatchID', 'LoadDate', 'LastUpdated']
		df_copy = df.drop(columns=lineage_columns, errors='ignore')

		# Calculate missing value percentages
		missing = df_copy.isnull().mean().sort_values(ascending=False)
		missing = missing[missing > 0]
		
		# Check if there are any missing values
		if len(missing) == 0:
			self._logger.info("No missing values in dataset")
			fig = plt.figure(figsize=(8, 6))
			plt.text(0.5, 0.5, "No missing values in dataset", 
					ha='center', va='center')
			pdf.savefig(fig)
			plt.close(fig)
			return
		
		try:
			# Create fresh figure and axis
			fig = plt.figure(figsize=plot_size)
			ax = fig.add_subplot(111)
			
			# Plot missing values as bar chart rather than using pandas
			x = np.arange(len(missing))
			ax.bar(x, missing.values)
			ax.set_xticks(x)
			ax.set_xticklabels(missing.index, rotation=90)
			ax.set_title("Missing Value Percentages")
			ax.set_ylabel("Percent Missing")
			fig.tight_layout()
			
			# Save and close
			pdf.savefig(fig)
			plt.close(fig)
		
		except Exception as e:
			self._logger.error(f"Error generating missing values chart: {e}")
			fig = plt.figure(figsize=(8, 6))
			plt.text(0.5, 0.5, f"Error generating missing values chart:\n{str(e)}", 
					ha='center', va='center')
			pdf.savefig(fig)
			plt.close(fig)


#======= 2. Validation Strategy Factory =======
class DataValidatorFactory:
	"""Factory for creating data validation strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategies = {}
		self._initialize_strategies()
	
	def _initialize_strategies(self) -> None:
		"""Initialize the registry with default strategies."""
		self.register('schema_validation', SchemaValidationStrategy(self._config))
		self.register('data_quality', DataQualityValidationStrategy(self._config))
		self.register('domain_validation', DomainSpecificValidationStrategy(self._config))
	
	def register(self, name: str, strategy: ValidationStrategy) -> None:
		"""Register a validation strategy."""
		if name in self._strategies:
			raise ValueError(f"Strategy {name} already registered")
		self._strategies[name] = strategy
	
	def get_strategy(self, name: str) -> ValidationStrategy:
		"""Get a validation strategy by name."""
		if name not in self._strategies:
			raise ValueError(f"Unknown validation strategy: {name}")
		return self._strategies[name]
	
	def create_schema_validation_strategy(self) -> ValidationStrategy:
		"""Create a schema validation strategy."""
		return SchemaValidationStrategy(self._config)
	
	def create_data_quality_validation_strategy(self) -> ValidationStrategy:
		"""Create a data quality validation strategy."""
		return DataQualityValidationStrategy(self._config)
	
	def create_domain_validation_strategy(self) -> ValidationStrategy:
		"""Create a domain-specific validation strategy."""
		return DomainSpecificValidationStrategy(self._config)


#############################################
#======= 3. Validation Pipeline Steps =======
class ValidationPipelineStep(IPipelineStep):
	"""Base class for validation pipeline steps."""
	
	def __init__(self, strategy: ValidationStrategy):
		self._strategy = strategy
	
	@property
	def strategy(self) -> ValidationStrategy:
		"""Get the validation strategy."""
		return self._strategy
		
	@property
	def name(self) -> str:
		"""Get the name of this pipeline step."""
		return self._strategy.name
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Execute validation strategy and return original dataframe."""
		validation_result = self._strategy.validate(df)
		
		# We don't modify the dataframe, just validate it
		return df


#======= 4. Validation Pipeline =======
class ValidationPipeline:
	"""Pipeline for data validation with run tracking."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._steps: List[ValidationPipelineStep] = []
		self._validation_count = 0
	
	def add_step(self, step: ValidationPipelineStep) -> 'ValidationPipeline':
		"""Add a validation step to the pipeline."""
		self._steps.append(step)
		return self
	
	def process(self, df: pd.DataFrame) -> Dict[str, Any]:
		"""Run validation pipeline with optimized memory usage."""
		self._validation_count += 1
		start_time = datetime.now()
		
		# Log which validation run this is
		self._logger.info(f"Running validation pass #{self._validation_count}")
		
		# Initialize results once, reuse structure
		results = {
			"status": "passed",
			"steps": [],
			"issues": [],
			"quality_metrics": {},
			"validation_pass": self._validation_count,
			"dataset_quality_score": 100.0,
			"report_generated": False,
			"processing_started": start_time
		}
		
		if df.empty:
			results["status"] = "failed"
			results["issues"].append({
				"type": "empty_dataframe",
				"message": "Empty dataframe provided for validation",
				"severity": "critical"
			})
			return results
		
		# Pre-allocate collections with expected size to avoid resizing
		all_issues = []
		combined_metrics = {}
		critical_count = 0
		warning_count = 0
		
		# Process validation steps in batches where possible
		steps_count = len(self._steps)
		for step_idx, step in enumerate(self._steps):
			step_start_time = datetime.now()
			try:
				self._logger.info(f"Executing validation step {step_idx+1}/{steps_count}: {step.name}")
				step_result = step.strategy.validate(df)
				
				# Track step results
				results["steps"].append({
					"name": step.name,
					"status": step_result.get("status", "unknown"),
					"issues_count": len(step_result.get("issues", [])),
					"duration_ms": (datetime.now() - step_start_time).total_seconds() * 1000
				})
				
				# Process issues more efficiently
				issues = step_result.get("issues", [])
				if issues:
					all_issues.extend(issues)
					
					# Count issues by severity in a single pass
					for issue in issues:
						severity = issue.get("severity")
						if severity == "critical":
							critical_count += 1
						elif severity == "warning":
							warning_count += 1
				
				# Update metrics and status
				if "quality_metrics" in step_result:
					combined_metrics.update(step_result["quality_metrics"])
				
				if step_result.get("status") == "failed":
					results["status"] = "failed"
				elif step_result.get("status") == "warning" and results["status"] != "failed":
					results["status"] = "warning"
				
			except Exception as e:
				self._logger.error(f"Error in validation step {step.name}: {e}")
				traceback.print_exc()
				results["status"] = "error"
		
		# Calculate quality score based on issues - do this once
		if all_issues:
			# Apply penalties: 10 points for each critical issue, 2 points for each warning
			penalty = (critical_count * 10) + (warning_count * 2)
			results["dataset_quality_score"] = max(0, 100 - penalty)
		
		results["issues"] = all_issues
		results["issues_count"] = len(all_issues)
		results["quality_metrics"] = combined_metrics
		results["processing_time_ms"] = (datetime.now() - start_time).total_seconds() * 1000
		
		return results  # Return the local results variable, not self._validation_results	

	def reset_validation_count(self) -> None:
		"""Reset validation count (used when starting a new dataset)."""
		self._validation_count = 0


###########################################
#======= 5. Validation module API =======
class Validator:
	"""Main class for validating datasets."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategy_factory = DataValidatorFactory(config_provider)
		self._dataset_name = None
		self._data = None
		self._validation_results = None
		self._validation_count = 0
		self._reporter = DataQualityReporter(config_provider)
	
	def load_dataset(self, dataset_name: str, df: pd.DataFrame) -> Validator:
		"""Load the dataset for validation."""
		self._dataset_name = dataset_name
		self._data = df
		self._config.set_config("current_dataset", dataset_name)
		self._reporter.set_dataset_name(dataset_name)
		self._validation_count = 0  # Reset validation count for new dataset
		self._logger.info(f"Loaded dataset for validation: {dataset_name} with {len(df)} rows and {len(df.columns)} columns")
		return self
	
	def process(self) -> Dict[str, Any]:
		"""Validate the dataset with improved performance."""
		if self._data is None or self._data.empty:
			self._logger.error("No data loaded for validation")
			return {"status": "failed", "message": "No data loaded for validation"}
		
		start_time = datetime.now()
		
		# Increment validation count
		self._validation_count += 1
		self._logger.info(f"Running validation pass #{self._validation_count}")
		
		# Create the validation pipeline
		pipeline = self.create_pipeline()
		
		# Validate the dataset
		self._validation_results = pipeline.process(self._data)
		
		# Add dataset information
		self._validation_results["dataset_name"] = self._dataset_name
		self._validation_results["row_count"] = len(self._data)
		self._validation_results["column_count"] = len(self._data.columns)
		self._validation_results["validation_pass"] = self._validation_count
		
		# Calculate execution time
		execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
		self._validation_results["execution_time_ms"] = execution_time_ms
		self._logger.info(f"Validation process completed in {execution_time_ms:.2f} ms")
		
		return self._validation_results
	
	def generate_report(self) -> None:
		"""Generate a data quality report using the current validation results."""
		if not self._validation_results:
			self._logger.warning("Cannot generate report - no validation results available")
			return
			
		# Check if report is already generated
		if not self._validation_results.get("report_generated", False):
			self._reporter.generate_report(self._data, self._validation_results)
			self._validation_results["report_generated"] = True
			self._logger.info(f"Generated data quality report for {self._dataset_name}")
		else:
			self._logger.info(f"Report already generated for {self._dataset_name}")
	
	def get_results(self) -> Dict[str, Any]:
		"""Get the validation results."""
		return self._validation_results
	
	def get_validation_count(self) -> int:
		"""Get the current validation pass count."""
		return self._validation_count
	
	def reset_validation_count(self) -> None:
		"""Reset the validation pass counter."""
		self._validation_count = 0
		self._logger.info("Reset validation pass counter")
	
	def save_results(self, output_path: Optional[Path] = None) -> bool:
		"""Save validation results to disk."""
		if self._validation_results is None:
			self._logger.error("No validation results to save")
			return False
		
		try:
			report_dir = Path(self._config.get_config('report_dir', 'Reports'))
			
			# Use provided path or default
			if output_path is None:
				output_path = report_dir / f"{self._dataset_name}_validation_report.json"
			
			# Ensure directory exists
			output_path.parent.mkdir(parents=True, exist_ok=True)
			
			# Save the results
			with open(output_path, 'w') as f:
				json.dump(self._validation_results, f, indent=2, default=str)
				
			self._logger.info(f"Saved validation results to {output_path}")
			return True
			
		except Exception as e:
			self._logger.error(f"Error saving validation results: {e}")
			traceback.print_exc()
			return False
	
	def create_pipeline(self) -> ValidationPipeline:
		"""Create a validation pipeline for the dataset."""
		pipeline = ValidationPipeline(self._config)
		
		# Add schema validation step
		pipeline.add_step(ValidationPipelineStep(
			self._strategy_factory.get_strategy('schema_validation')
		))
		
		# Add data quality validation step
		pipeline.add_step(ValidationPipelineStep(
			self._strategy_factory.get_strategy('data_quality')
		))
		
		# Add domain-specific validation step
		pipeline.add_step(ValidationPipelineStep(
			self._strategy_factory.get_strategy('domain_validation')
		))
		
		return pipeline
