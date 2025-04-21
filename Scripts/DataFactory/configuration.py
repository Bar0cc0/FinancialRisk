#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Financial Risk Data Configuration Management
============================================

This module provides a comprehensive configuration management system for the financial data
processing pipeline. It handles loading settings from YAML files, establishing intelligent
defaults, setting up logging, extracting SQL schema definitions, and managing dataset-specific 
configurations.

The module implements the IConfigProvider interface, offering a centralized configuration 
repository with runtime adaptability and environment awareness.

Key Components
--------------
- Configuration Provider: Centralized access to all configuration settings
- Default Configuration Management: Smart defaults with override capability
- Directory Structure Management: Creates and validates required directories
- Logging System: Configurable logging with dataset-aware formatting
- SQL Schema Extraction: Parses SQL files to extract database schemas
- Field Mapping: Maps source dataset fields to target database schemas
- Identity Column Management: Special handling for auto-incrementing columns

Features
--------
- Intelligent project root detection
- YAML configuration loading with multi-document support
- Environment-variable overrides
- Hierarchical configuration merging
- SQL schema parsing with robust error handling
- Logging with dataset context awareness
- Configurable logging levels

Usage Example
-------------
from configuration import Configuration

# Initialize with default settings
config = Configuration()

# Or with custom settings
config = Configuration(
    config_path="custom_config.yaml", 
    root_dir="/path/to/project",
    loglevel="DEBUG"
)

# Access configuration values
input_dir = config.get_config('input_dir')
dataset_mappings = config.get_dataset_mappings()

# Get schema information
loan_schema = config.get_schema_for_dataset('Loan')
"""

#=================================================

import os
import logging
import yaml
import re
import glob
from datetime import datetime
import traceback
from pathlib import Path
from typing import Dict, Any, Optional, List

from interfaces import IConfigProvider


class Configuration(IConfigProvider):
	"""Configuration provider with encapsulated state."""
	
	def __init__(self, config_path: Optional[str] = None, root_dir: Optional[str] = None, loglevel: Optional[str] = None) -> None:
		# Determine project root - allow override or default intelligently
		if root_dir:
			self._root = Path(root_dir).resolve()
		else:
			# Default: Use the project root (2 levels up from script location)
			script_dir = Path(__file__).resolve().parent
			self._root = script_dir.parents[1]  # Go up to FinancialRisk directory
		
		# Store root in config for other components to access
		self._config = {'root_dir': str(self._root)}
		
		# Set config path relative to script dir if not absolute
		if config_path:
			config_path_obj = Path(config_path)
			if not config_path_obj.is_absolute():
				# If relative path provided, make it relative to script dir
				config_path_obj = Path(__file__).parent / config_path_obj
			self._config_path = config_path_obj
		else:
			# Default config in same directory as this script
			self._config_path = Path(__file__).parent / 'config.yaml'
		
		self._logger = None
		
		# Update config with defaults using the established root
		self._config.update(self._load_default_config())
		
		# Set up required directories
		self._initialize_directories()
		
		# Set up logging
		self._initialize_logging(loglevel)
		
		# Load from file if available
		self.load_from_file()
		
		# Check sample size
		if not self._config.get('nsamples'):
			self._logger.warning("No sample size specified, using default")
			self._config['nsamples'] = 1000
		
		# Initialize datasets
		self.datasets = {}

		# Get SQL schema definitions
		self.sql_schemas = self._extract_sql_schemas()

	def _initialize_directories(self) -> List[Path]:
		"""Create required directories."""
		paths_created = []
		
		for key, value in self._config.items():
			# Check if this is a directory path configuration
			if isinstance(value, Path) and key.endswith('_dir'):
				# Clean output directory between runs
				if key == 'output_dir' and value.exists():
					for item in value.iterdir():
						if item.is_file():
							try:
								item.unlink()
							except Exception as e:
								self._logger.error(f"Error deleting file {item}: {e}")
								continue
				
				# Create directory if it doesn't exist
				if not value.exists():
					try:
						value.mkdir(parents=True, exist_ok=True)
						paths_created.append(value)
					except Exception as e:
						self._logger.error(f"Error creating directory {value}: {e}")

		return paths_created
	
	def _initialize_logging(self, loglevel: Optional[str] = None) -> None:
		"""Initialize logging system."""
		log_dir = self._config.get('log_dir', 'Logs')
		if not log_dir.exists():
			log_dir.mkdir(parents=True, exist_ok=True)
			
		log_file = log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_DataFactory.log"
		
		# Create a custom filter to add dataset information to log records
		class DatasetFilter(logging.Filter):
			def __init__(self, config):
				super().__init__()
				self.config = config
				self._last_dataset = None
				
			def filter(self, record):
				dataset = self.config.get_config('current_dataset', '')
				record.dataset = dataset
				
				# Track dataset changes for debugging
				if self._last_dataset != dataset:
					self._last_dataset = dataset
				
				return True
				
			def refresh_dataset(self):
				"""Method to explicitly refresh the dataset information."""
				self._last_dataset = None
		
		# Configure the logger with the new format including dataset
		logging.basicConfig(
			level=logging.INFO,
			format='%(asctime)s - %(levelname)s - [%(dataset)s] - %(message)s',
			handlers=[
				logging.FileHandler(log_file),
				logging.StreamHandler()
			]
		)
		
		self._logger = logging.getLogger(__name__)
		
		# Add our custom filter to the logger
		self._logger.addFilter(DatasetFilter(self))

		# Set logger to debug level for detailed output
		if loglevel and loglevel.upper() == 'DEBUG':
			self._logger.setLevel(logging.DEBUG)
		else:
			self._logger.setLevel(logging.INFO)
	
	def _load_default_config(self) -> Dict[str, Any]:
		"""Load the default configuration."""
		return {
			# Directories
			'input_dir': self._root / 'Datasets/Raw',
			'output_dir': self._root / 'Datasets/Cooked',
			'log_dir': self._root / 'Logs',

			# Multithreaded orchestration settings
			'max_workers': max(1, os.cpu_count() - 1),
			'parallel_processing': False,
			'chunk_size': 1000,

			# Data processing settings
			'nsamples': 100,
			'knn_neighbors': 5,
			'outlier_threshold': 3.0,
			'min_numeric_percent': 0.5,
			'max_missing_pct': 0.5,
			'correlation_threshold': 0.95,
			'normalize_numeric': False,
			'dummy_encode_categorical': True,
			'integer_patterns': ['num_', 'number', 'count', 'qtd', 'qty', '_id', '_nbr', 'age', 'delayed'],
			'binary_patterns': ['employed', 'self-employed'],

			# Checkpointing and reporting
			'checkpointing': False,
			'validate_after_fixing': True,
			'validation_report': True,
			'data_quality_report': True,
			'add_data_lineage': True
		}
	
	def get_logger(self) -> logging.Logger:
		"""Get logger instance."""
		return self._logger
	
	def get_config(self, key: str, default=None) -> Any:
		"""Get a configuration value with optional default."""
		value = self._config.get(key, default)
		
		# Special handling for current_dataset changes to update loggers
		if key == 'current_dataset' and value != self._config.get(key):
			# Force all loggers to refresh their filters when dataset changes
			for handler in logging.root.handlers:
				for filter in handler.filters:
					if hasattr(filter, 'refresh_dataset'):
						filter.refresh_dataset()
		
		return value
	
	def set_config(self, key: str, value: Any) -> None:
		"""Set a configuration value."""
		self._config[key] = value
		
	def get_all_config(self) -> Dict[str, Any]:
		"""Get all configuration values."""
		return self._config.copy()  # Return a copy to prevent direct modification
	
	def load_from_file(self, config_path: Optional[str] = None) -> bool:
		"""Load configuration from a YAML file."""
		path = Path(config_path) if config_path else self._config_path
		if not path.exists():
			if self._logger:
				self._logger.warning(f"Config file not found: {path}. Using defaults.")
			return False
			
		try:
			with open(path, 'r') as f:
				# Load all documents in the YAML file
				all_docs = list(yaml.safe_load_all(f))
				
			# Merge all documents into a single configuration
			file_config = {}
			for doc in all_docs:
				if doc:  # Skip empty documents
					file_config.update(doc)

			# Process paths in Engine_Parameters
			if 'Engine_Parameters' in file_config and isinstance(file_config['Engine_Parameters'], dict):
				for key, value in file_config['Engine_Parameters'].items():
					if isinstance(value, str) and (key.endswith('_dir') or key.endswith('_path')):
						file_config['Engine_Parameters'][key] = self._root / value
						
			# Flatten Engine_Parameters for direct access
			if 'Engine_Parameters' in file_config:
				top_level_config = file_config.pop('Engine_Parameters')
				file_config.update(top_level_config)
			
			# Update config
			self._config.update(file_config)


			if self._logger:
				self._logger.info(f"Loaded configuration from {path}")
			return True
			
		except Exception as e:
			if self._logger:
				self._logger.error(f"Error loading config: {e}")
			return False
	
	def _extract_sql_schemas(self) -> Dict[str, Dict]:
		"""Extract schema definitions from SQL files."""
		schemas = {}
		schema_details = {}
		definition_file = self._config.get('TargetSchema', {}).get('definition_file', '')
		
		# Find SQL definition file using regex pattern if not explicitly provided
		if definition_file:
			sql_path = self._root / definition_file
			self._logger.info(f"Using explicit schema definition file: {sql_path}")
		else:
			# Add pattern for usp_* files specifically
			schema_file_patterns = [
				r'usp_create.*\.sql$',
				r'.*schema.*\.sql$',
				r'.*create.*tables.*\.sql$', 
				r'.*ddl.*\.sql$',
				r'.*database.*structure.*\.sql$',
				r'.*tables.*definition.*\.sql$',
			]
			
			# Search for files matching any of the patterns
			schema_files = []
			for root, _, files in os.walk(self._root):
				for file in files:
					if any(re.search(pattern, file.lower()) for pattern in schema_file_patterns):
						full_path = Path(root) / file
						schema_files.append(full_path)
						self._logger.debug(f"Found potential schema file: {full_path}")
			
			if schema_files:
				# Prioritize files with 'staging' or 'usp' in the name
				staging_files = [f for f in schema_files if 'staging' in f.name.lower() or 'usp' in f.name.lower()]
				if staging_files:
					sql_path = staging_files[0]
				else:
					sql_path = schema_files[0]
				
				self._logger.info(f"Using schema definition file: {sql_path}")
			else:
				self._logger.warning("No SQL schema definition files found in project")
				return self._load_default_schemas()  # Changed from _load_hardcoded_schemas
		
		if not sql_path.exists():
			self._logger.warning(f"SQL schema file not found: {sql_path}")
			return self._load_default_schemas()
		
		try:
			# Read SQL file content
			with open(sql_path, 'r', encoding='utf-8-sig') as f:
				sql_content = f.read()
				
			# Extract table definitions with a more flexible pattern
			# This pattern works for both direct CREATE TABLE statements and those within stored procedures
			table_pattern = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:Staging\.)?(\w+)\s*\((.*?)(?:\)\s*(?:ON\s+\[?PRIMARY\]?|WITH|GO|;))', re.DOTALL | re.IGNORECASE)
			
			for table_match in table_pattern.finditer(sql_content):
				table_name = table_match.group(1)
				table_content = table_match.group(2)
				
				self._logger.debug(f"Found table definition: {table_name}")
				
				table_schema = {}
				table_schema_details = {}
				
				# Process column definitions with improved parsing
				self._parse_column_definitions(table_content, table_schema, table_schema_details)
				
				schemas[table_name] = table_schema
				schema_details[table_name] = table_schema_details
				
				# Diagnostics to verify column extraction
				self._logger.debug(f"Columns extracted for {table_name}: {list(table_schema.keys())}")
			
			if not schemas:
				self._logger.warning("No schema definitions found in SQL file")
				return self._load_default_schemas()
			
			# Store detailed schema info for access by other components
			self.sql_schema_details = schema_details
			
			return schemas
			
		except Exception as e:
			self._logger.error(f"Error parsing SQL schema: {e}")
			traceback.print_exc()
			return self._load_default_schemas()

	def _parse_column_definitions(self, table_content, table_schema, table_schema_details):
		"""Parse column definitions with improved handling of comment lines and column definitions."""
		# First clean up multi-line comments
		table_content = re.sub(r'/\*.*?\*/', '', table_content, flags=re.DOTALL)
		
		# Split the content by line to handle comments properly
		lines = table_content.split('\n')
		processed_content = []
		
		# Process each line to separate comments from column definitions
		for line in lines:
			if '--' in line:
				# Split at comment and keep only the part before it
				code_part = line.split('--')[0].strip()
				if code_part:  # If there's code before the comment
					processed_content.append(code_part)
			else:
				processed_content.append(line)
		
		# Rejoin lines and continue with standard parsing
		clean_content = ' '.join(processed_content)
		
		# Split by comma only when not inside parentheses
		in_parentheses = 0
		start_idx = 0
		column_defs = []
		
		for i, char in enumerate(clean_content):
			if char == '(':
				in_parentheses += 1
			elif char == ')':
				in_parentheses -= 1
			elif char == ',' and in_parentheses == 0:
				# We found a column separator
				col_def = clean_content[start_idx:i].strip()
				if col_def:  # Accept all column definitions initially
					column_defs.append(col_def)
				start_idx = i + 1
		
		# Add the last column
		last_col = clean_content[start_idx:].strip()
		if last_col:
			column_defs.append(last_col)
		
		# Process each column definition individually
		for col_def in column_defs:
			self._process_column_def(col_def, table_schema, table_schema_details)

	def _process_column_def(self, col_def: str, table_schema: Dict, table_schema_details: Dict) -> None:
		"""Process a single column definition with improved parsing."""
		try:
			# Skip comment-only lines
			if col_def.strip().startswith('--'):
				self._logger.debug(f"Skipping comment line: {col_def}")
				return
				
			# Clean up the column definition
			original_def = col_def
			
			# Handle inline comments more carefully
			if '--' in col_def:
				# Split only on the first occurrence of --
				col_def = col_def.split('--', 1)[0].strip()
				
			# Skip if empty after removing comments
			if not col_def:
				return
				
			# Skip single words (likely example values in comments)
			if len(col_def.split()) == 1:
				self._logger.debug(f"Skipping single word that's not a column definition: {col_def}")
				return
			
			# Extract column name using regex to handle brackets and quotes
			col_name_match = re.match(r'^\s*\[?([^\[\]\s,]+)\]?\s+', col_def)
			if not col_name_match:
				self._logger.warning(f"Could not extract column name from: {original_def}")
				return
				
			col_name = col_name_match.group(1).strip('[]"\'`')
			
			# Skip data lineage columns that will be added during import
			lineage_columns = ['DataSourceFile', 'LoadBatchID', 'LoadDate', 'LastUpdated']
			if col_name in lineage_columns:
				self._logger.debug(f"Skipping lineage column: {col_name}")
				return
			
			# Skip if the column name is a SQL keyword
			sql_keywords = ['CREATE', 'TABLE', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'DROP', 'ALTER']
			if col_name.upper() in sql_keywords:
				self._logger.debug(f"Skipping SQL keyword: {col_name}")
				return
			
			# Remove column name from definition to isolate the data type
			remaining = col_def[col_name_match.end():].strip()
			
			# Extract data type with parameters
			data_type_match = re.match(r'([A-Za-z0-9_]+(?:\s*\([^)]*\))?)', remaining)
			if not data_type_match:
				self._logger.warning(f"Could not extract data type from: {original_def}")
				return
				
			data_type = data_type_match.group(1).strip()
			
			# Skip UNIQUEIDENTIFIER columns as they should be system-generated
			if 'UNIQUEIDENTIFIER' in data_type.upper():
				self._logger.debug(f"Skipping UNIQUEIDENTIFIER column: {col_name}")
				return
			
			# For the simplified schema, use the data type
			table_schema[col_name] = data_type
			
			# Log success with more detail for debugging
			self._logger.debug(f"Successfully parsed column: {col_name} ({data_type})")
			
			# For detailed schema, extract all metadata
			details = {
				'data_type': data_type,
				'nullable': 'NOT NULL' not in remaining,
				'identity': 'IDENTITY' in remaining,
				'default': None
			}
			
			# Extract default value if present
			default_match = re.search(r'DEFAULT\s+([^,\s]+)', remaining)
			if default_match:
				details['default'] = default_match.group(1)
				
			table_schema_details[col_name] = details
			
		except Exception as e:
			self._logger.warning(f"Error processing column definition '{col_def}': {e}")
			traceback.print_exc()

	def get_identity_columns(self, dataset_name: str) -> List[str]:
		"""Get identity columns for a specific dataset."""
		if hasattr(self, 'sql_schema_details') and dataset_name in self.sql_schema_details:
			return [
				col_name for col_name, details in self.sql_schema_details[dataset_name].items()
				if details.get('identity', False)
			]
		return []

	def _load_default_schemas(self) -> Dict[str, Dict]:
		"""Fallback to hardcoded schemas if SQL parsing fails."""
		# Hardcoded schemas based on usp_CreateStagingTables.sql
		
		schemas = {
			"Loan": {
				"CustomerID": "INT",
				"LoanID": "NVARCHAR",
				"Age": "INT", 
				"JobTitle": "VARCHAR",
				"EmploymentStatus": "INT",
				"HomeOwnershipStatus": "VARCHAR",
				"MaritalStatus": "VARCHAR",
				"PreviousLoanDefault": "INT",
				"AnnualIncome": "DECIMAL",
				"TotalAssets": "DECIMAL",
				"TotalLiabilities": "DECIMAL",
				"AnnualExpenses": "DECIMAL",
				"MonthlySavings": "DECIMAL",
				"AnnualBonus": "DECIMAL",
				"DebtToIncomeRatio": "DECIMAL",
				"PaymentHistoryYears": "INT",
				"JobTenureMonths": "INT",
				"NumDependents": "INT",
				"LoanType": "NVARCHAR",
				"LoanDate": "DATE",
				"LoanAmount": "DECIMAL",
				"LoanDurationMonths": "INT",
				"InterestRate": "DECIMAL",
				"DelayedPayments": "INT",
				"Balance": "DECIMAL",
				"MonthlyPayment": "DECIMAL",
				"LoanToValueRatio": "DECIMAL",
				"PaymentStatus": "NVARCHAR",
				"AccountID": "NVARCHAR",
				"AccountType": "NVARCHAR",
				"PaymentBehavior": "NVARCHAR",
				"AccountBalance": "DECIMAL",
				"CreditScore": "INT",
				"NumBankAccounts": "INT",
				"NumCreditCards": "INT",
				"NumLoans": "INT",
				"NumCreditInquiries": "INT",
				"CreditUtilizationRatio": "DECIMAL",
				"CreditHistoryLengthMonths": "INT",
				"CreditMixRatio": "DECIMAL"
			},
			"Fraud": {
				"TransactionID": "INT",
				"CustomerID": "INT",
				"TransactionDate": "DATETIME",
				"TransactionTypeName": "NVARCHAR",
				"DistanceFromHome": "FLOAT",
				"DistanceFromLastTransaction": "FLOAT",
				"RatioToMedianTransactionAmount": "FLOAT",
				"IsOnlineTransaction": "INT",
				"IsUsedChip": "INT", 
				"IsUsedPIN": "INT",
				"IsFraudulent": "INT"
			},
			"Market": {
				"MarketID": "INT",
				"MarketDate": "DATE",
				"MarketName": "NVARCHAR",
				"OpenValue": "DECIMAL",
				"CloseValue": "DECIMAL",
				"HighestValue": "DECIMAL",
				"LowestValue": "DECIMAL",
				"Volume": "DECIMAL",
				"InterestRate": "FLOAT",
				"ExchangeRate": "FLOAT",
				"GoldPrice": "DECIMAL",
				"OilPrice": "DECIMAL",
				"VIX": "FLOAT",
				"TEDSpread": "FLOAT",
				"EFFR": "FLOAT"
			},
			"Macro": {
				"MacroID": "INT",
				"ReportDate": "DATE",
				"CountryName": "NVARCHAR",
				"UnemploymentRate": "DECIMAL",
				"GDP": "DECIMAL",
				"DebtRatio": "DECIMAL",
				"DeficitRatio": "DECIMAL",
				"InflationRate": "DECIMAL",
				"ConsumerPriceIndex": "DECIMAL",
				"HousePriceIndex": "DECIMAL"
			}
		}
		
		# Set default identity columns information
		self.sql_schema_details = {
			"Market": {"MarketID": {"identity": True}},
			"Macro": {"MacroID": {"identity": True}}
		}

		return schemas

	def get_field_mappings_for_dataset(self, dataset_name: str) -> List[Dict]:
		"""Get the field mappings for a specific dataset."""
		datasets_config = self._config.get('Datasets', {})
		if dataset_name in datasets_config:
			return datasets_config[dataset_name].get('field_mappings', [])
		return []

	def get_dataset_mappings(self) -> Dict[str, Dict[str, Any]]:
			"""Get all dataset definitions and their mappings."""
			return self.get_config('Datasets', {})
	
	def get_schema_for_dataset(self, dataset_name: str) -> Dict:
		"""Get the SQL schema for a specific dataset."""
		return self.sql_schemas.get(dataset_name, {})