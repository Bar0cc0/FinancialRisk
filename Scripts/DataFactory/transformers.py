#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Schema Transformation and Synthetic Data Generation
===================================================

This module provides a framework for transforming raw financial datasets to match target SQL 
database schemas and generating high-quality synthetic data for missing fields.

The module supports:
- Transformation of source data fields to match SQL data types and constraints
- Field mapping from diverse input schemas to standardized target schemas
- Synthetic data generation with domain-specific knowledge
- Preservation of relationships between related fields
- Handling of identity columns and unique identifiers

Key Components
--------------
- TransformationStrategy: Base strategy class for all transformers
- SchemaStrategy: Maps source fields to target schema and identifies gaps
- DataGenerationStrategy: Generates realistic synthetic data for missing fields
- TransformerStrategyFactory: Creates and manages transformation strategies
- TransformationPipeline: Orchestrates sequential transformation steps
- Transformer: Main API class for end-to-end transformations

Integration Points
------------------
This module integrates with:
- Configuration system for schema definitions and field mappings
- Logging framework for tracking transformations
- Pandas DataFrames for data manipulation

Usage Example
-------------
from configuration import Configuration
from transformers import Transformer
import pandas as pd

# Initialize configuration
config = Configuration()
config.load_from_file("config.yaml")

# Load source data
df = pd.read_csv("raw_market_data.csv")

# Transform data to match target schema using the module API
transformer = Transformer(config)
transformed_df = transformer.load_dataset("Market", df).process()
"""

#=================================================

from __future__ import annotations
from abc import abstractmethod
from functools import lru_cache
from pathlib import Path
import pandas as pd
import numpy as np
import random
import re
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


from interfaces import IDataTransformationStrategy, IConfigProvider, IPipelineStep



#======== 1. Transformation Strategies ==========
class TransformationStrategy(IDataTransformationStrategy):
	"""Base class for data transformation strategies."""

	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
	
	@property
	def name(self) -> str:
		"""Return the name of this strategy instance."""
		return self.__class__.__name__
	
	@abstractmethod
	def transform(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Transform the dataframe and return the result."""
		pass

	def _log_changes(self, change_count: int, message: str) -> None:
		"""Log changes if there were any."""
		if change_count > 0:
			self._logger.debug(f"{self.name}: {change_count} {message}")


class SchemaStrategy(TransformationStrategy):
	"""Transform data to match target schema field types and identify fields for generation."""
	
	def __init__(self, config_provider: IConfigProvider, dataset_name: str):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._dataset_name = dataset_name

		# Track fields that need generation
		self._fields_for_generation = []
	
	@property
	def fields_for_generation(self) -> List[str]:
		"""Get the list of fields that need synthetic data generation."""
		return self._fields_for_generation

	def transform(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Transform the dataframe to match the target schema and identify fields for generation."""
		if df.empty:
			return df
		try:		
			# Get target schema and field mappings
			target_schema: Dict[str, str] = self._config.get_schema_for_dataset(self._dataset_name)
			field_mappings: List[Dict[str, str]] = self._config.get_field_mappings_for_dataset(self._dataset_name)
			
			# Create a case-insensitive column lookup dictionary
			column_lookup = {col.lower(): col for col in df.columns}
			
			# Reset fields for generation list
			self._fields_for_generation = []
			
			if not target_schema:
				self._logger.warning(f"No target schema defined for {self._dataset_name}")
				return df
			
			# Create a new dataframe with the target schema
			result_df = pd.DataFrame(index=df.index)
			
			# Track transformation counts
			transformed_count = 0
			generation_needed_count = 0
			
			# Log for debugging
			self._logger.debug(f"Field mappings for {self._dataset_name}: {field_mappings}")
			self._logger.debug(f"Target schema for {self._dataset_name}: {target_schema}")
			
			# Get identity columns before processing fields to avoid inconsistency
			identity_columns = self._config.get_identity_columns(self._dataset_name)
			uniqueid_columns = [col for col in df.columns if col.lower() == 'loadbatchid']
			columns_to_skip = identity_columns + uniqueid_columns
			
			# Process each field in the target schema
			for target_field, sql_type in target_schema.items():
				try:
					# Skip identity and uniqueidentifier columns completely
					if target_field in columns_to_skip:
						self._logger.debug(f"Skipping IDENTITY/UNIQUEIDENTIFIER column: {target_field}")
						continue
						
					source_field = None
					matched_column = None
					
					# Find source field mapping if provided
					if field_mappings:
						for mapping in field_mappings:
							if isinstance(mapping, dict) and target_field in mapping:
								source_field = mapping[target_field]
								break
					
					# Try case-insensitive matching if source field is specified
					if source_field:
						# First try exact match
						if source_field in df.columns:
							matched_column = source_field
						# Then try case-insensitive match
						elif source_field.lower() in column_lookup:
							matched_column = column_lookup[source_field.lower()]
							self._logger.debug(f"Case-insensitive match: '{source_field}' -> '{matched_column}'")
					
					# Debug for specific columns
					if target_field in ['CloseValue', 'HighestValue', 'LowestValue']:
						self._logger.debug(f"Source field for {target_field}: {source_field}")
						self._logger.debug(f"Matched column: {matched_column}")
						if matched_column:
							self._logger.debug(f"Sample data: {df[matched_column].head(3)}")
					
					# Transform field if we found a match
					if matched_column:
						result_df[target_field] = self._convert_to_sql_type(df, matched_column, sql_type)
						transformed_count += 1
					else:
						# Mark for data generation
						self._fields_for_generation.append(target_field)
						self._logger.debug(f"Marking field {target_field} for generation")
						generation_needed_count += 1
						result_df[target_field] = pd.Series([None] * len(df), index=df.index)
					
				except Exception as e:
					self._logger.error(f"Error transforming field {target_field}: {e}")
					traceback.print_exc()
					result_df[target_field] = pd.Series([None] * len(df), index=df.index)
			
			# Log details of identity columns
			if columns_to_skip:
				self._logger.debug(f"Skipped {len(columns_to_skip)} IDENTITY/UNIQUEIDENTIFIER columns in {self._dataset_name} dataset: {columns_to_skip}")
			
			# Save fields for generation in configuration for DataGenerationStrategy to access
			self._config.set_config(f"{self._dataset_name}_synthetic_fields", self._fields_for_generation)
			
			self._logger.info(f"Schema transformation complete: {transformed_count} fields transformed, " +
							f"{generation_needed_count} fields marked for generation")
			return result_df
			
		except Exception as e:
			self._logger.error(f"Error in schema transformation: {e}")
			traceback.print_exc()
			# Return original dataframe as fallback
			return df

	@staticmethod
	@lru_cache(maxsize=256)
	def _extract_sql_type_parameters(sql_type: str) -> Dict[str, Any]:
		"""Extract parameters from SQL type definition strings."""
		result = {
			'base_type': sql_type.split('(')[0] if '(' in sql_type else sql_type,
			'max_length': None,
			'precision': None,
			'scale': None
		}
		
		# Extract parameters if present in parentheses
		if "(" in sql_type and ")" in sql_type:
			try:
				param_section = sql_type.split('(')[1].split(')')[0]
				
				# Handle different parameter patterns
				if ',' in param_section:  # e.g. DECIMAL(10,2)
					parts = param_section.split(',')
					result['precision'] = int(parts[0].strip())
					if len(parts) > 1:
						result['scale'] = int(parts[1].strip())
				else:  # e.g. VARCHAR(50)
					result['max_length'] = int(param_section.strip())
			except (ValueError, IndexError):
				pass
				
		return result

	def _convert_to_sql_type(self, df: pd.DataFrame, source_field: str, sql_type: str) -> pd.Series:
		"""Transform a field according to its SQL type."""
		try:
			# Get the source data
			source_data = df[source_field]

			# Extract parameters
			params = self._extract_sql_type_parameters(sql_type)
			
			# Special handling for ID fields that need to preserve relationships
			if ('id' in source_field.lower() or 'id' in sql_type.lower()) and 'INT' in sql_type:
				# For ID fields, maintain uniqueness while converting to integer
				if pd.api.types.is_object_dtype(source_data) or pd.api.types.is_string_dtype(source_data):
					# Create a dictionary mapping unique values to sequential integers
					unique_values = source_data.dropna().unique()
					id_mapping = {val: (i + np.random.randint(10000, 99999)) for i, val in enumerate(unique_values)}
					
					# Use the mapping to convert to integers while preserving relationships
					return source_data.map(id_mapping).fillna(0).astype(int)
			
			# Handle UNIQUEIDENTIFIER SQL type
			if "UNIQUEIDENTIFIER" in params['base_type']:
				# Check if source data already has valid UUID format
				if pd.api.types.is_string_dtype(source_data):
					# Try to validate existing UUIDs
					import uuid
					try:
						# Sample a few values to check if they're valid UUIDs
						sample = source_data.dropna().head(5)
						valid_uuids = all(
							uuid.UUID(str(val), version=4) and True
							for val in sample if pd.notna(val)
						)
						if valid_uuids and len(sample) > 0:
							return source_data
					except (ValueError, AttributeError):
						pass  # Not valid UUIDs, generate new ones
				
				# Generate new UUIDs
				import uuid
				return pd.Series([str(uuid.uuid4()) for _ in range(len(df))])

			# Transform based on SQL type
			if "VARCHAR" in params['base_type'] or "NVARCHAR" in params['base_type']:
				# Convert to string and handle nulls
				result = source_data.astype(str).fillna("")
				
				# Truncate if max_length is specified
				if params['max_length']:
					result = result.str.slice(0, params['max_length'])
				
				return result
				
			elif "INT" in params['base_type'] or "INTEGER" in params['base_type']:
				return pd.to_numeric(source_data, errors='coerce').fillna(0).astype(int)
				
			elif "DECIMAL" in params['base_type'] or "NUMERIC" in params['base_type'] or "FLOAT" in params['base_type']:
				# Extract precision and scale if specified
				precision = params['precision']
				scale = params['scale']
				
				# Convert to numeric and handle nulls
				result = pd.to_numeric(source_data, errors='coerce').fillna(0.0)
				
				# Apply rounding if scale is specified - PATCHED: Always round to specified SQL scale
				if scale is not None:
					# Log warning for values with higher precision than target schema allows
					if result.astype(str).str.contains(r'\.\d{' + str(scale+1) + ',}').any():
						self._logger.warning(f"Column {source_field} contains values with more decimal places than the target SQL type {sql_type} allows. Values will be rounded.")
					result = result.round(scale)
				
				# For safety, always round decimal values to prevent precision issues
				# This ensures we never exceed database column definitions
				if 'DebtRatio' in source_field or 'ratio' in source_field.lower():
					result = result.round(2)  # Ensure ratio fields use 2 decimal places
					
				return result
					
			elif "DATE" in params['base_type'] or "DATETIME" in params['base_type'] or "TIMESTAMP" in params['base_type']:
				# Special case for year-only report_date field
				if source_field == 'report_date':
					try:
						# Convert year values to proper dates (with month=01, day=01)
						return pd.to_datetime(source_data.astype(int), format='%Y')
					except:
						pass
				# Default handling for other date formats
				return pd.to_datetime(source_data, errors='coerce').fillna(datetime.now())
				
			elif "BIT" in params['base_type'] or "BOOLEAN" in params['base_type']:
				# Handle various boolean representations
				if source_data.dtype == bool:
					return source_data.fillna(False)
				else:
					# Convert truthy/falsy values to boolean
					truthy = ['true', 't', 'yes', 'y', '1', 'on', 'enable', 'enabled']
					falsy = ['false', 'f', 'no', 'n', '0', 'off', 'disable', 'disabled']
					
					def parse_bool(val):
						if pd.isna(val):
							return False
						if isinstance(val, (int, float)):
							return bool(val)
						if isinstance(val, str):
							val = val.lower().strip()
							if val in truthy:
								return True
							if val in falsy:
								return False
						return bool(val)
					
					return source_data.map(parse_bool)
			else:
				# Default to string for unknown types
				self._logger.warning(f"Unknown SQL type: {sql_type}, using string conversion")
				return source_data.astype(str).fillna("")
				
		except Exception as e:
			self._logger.error(f"Error transforming field {source_field} to SQL type {sql_type}: {e}")
			traceback.print_exc()
			# Return empty series on error
			return pd.Series([None] * len(df), index=df.index)

	def _handle_sql_identity_columns(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
		"""Remove columns that are defined as IDENTITY or UNIQUEIDENTIFIER in SQL schema."""
		# Get identity columns from config
		identity_columns = self._config.get_identity_columns(dataset_name)
		
		# Also check for uniqueidentifier columns that need removal
		uniqueid_columns = [col for col in df.columns if col.lower() == 'loadbatchid']
		
		# Combine columns to remove
		columns_to_remove = [col for col in identity_columns + uniqueid_columns if col in df.columns]
		
		if columns_to_remove:
			original_cols = df.shape[1]
			df = df.drop(columns=columns_to_remove)
			self._logger.debug(f"Removed {len(columns_to_remove)} IDENTITY/UNIQUEIDENTIFIER columns from {dataset_name} dataset: {columns_to_remove}")
			self._logger.debug(f"Columns reduced from {original_cols} to {df.shape[1]}")
		
		return df


class DataGenerationStrategy(TransformationStrategy):
	"""Generate realistic synthetic data based on schema definitions and field semantics."""

	# Class-level cache shared across all instances of DataGenerationStrategy
	_global_id_cache = {}
	_scanned_directories = set()
	_global_account_registry = set()
	
	def __init__(self, config_provider: IConfigProvider, dataset_name: str = None):
		"""Initialize with configuration and optional dataset name."""
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._dataset_name = dataset_name
		self._domain_patterns = self._load_domain_patterns()
		
		# Ensure ID cache is initialized
		if not hasattr(DataGenerationStrategy, '_id_field_inventory'):
			DataGenerationStrategy._id_field_inventory = {}
		
		# Local copy of generated IDs for this instance
		self._generated_account_ids = set()
			
	def _initialize_id_inventory(self) -> None:
		"""Scan all cooked files once and build an inventory of available ID fields."""
		# Check if cross-dataset ID relationships are enabled
		maintain_relationships = self._config.get_config('maintain_id_relationships', True)
		if not maintain_relationships:
			return
			
		# Get directory to scan
		output_dir = Path(self._config.get_config('output_dir'))
		output_dir_str = str(output_dir)
		
		# Skip if we've already scanned this directory
		if output_dir_str in self._scanned_directories:
			return
			
		self._logger.info(f"Building ID field inventory from {output_dir}")
		
		try:
			# Get all relevant field names to look for
			relationship_fields = self._config.get_config('id_relationship_fields', [])
			id_pattern = re.compile(r'.*id$', re.IGNORECASE)  # Fields ending with 'id'
			
			# Look for all cooked CSV files
			cooked_files = list(output_dir.glob("*_cooked.csv"))
			for csv_path in cooked_files:
				try:
					# First just read the header row to get column names
					with open(csv_path, 'r') as f:
						header = f.readline().strip().split(',')
					
					# Identify ID fields in this file
					id_fields = []
					for field in header:
						if relationship_fields and field in relationship_fields:
							id_fields.append(field)
						elif id_pattern.match(field):
							id_fields.append(field)
					
					if id_fields:
						# If we found ID fields, now read just those columns
						df = pd.read_csv(csv_path, usecols=id_fields)
						for field in id_fields:
							# Extract unique non-zero values
							unique_values = df[field].dropna().unique()
							unique_values = unique_values[unique_values != 0].tolist()
							
							if unique_values:
								# Add to inventory with dataset source
								if field not in self._id_field_inventory:
									self._id_field_inventory[field] = []
								
								self._id_field_inventory[field].append({
									'source': csv_path.name,
									'values': unique_values,
									'count': len(unique_values)
								})
								self._logger.debug(f"Found {len(unique_values)} unique {field} values in {csv_path.name}")
								
				except Exception as e:
					self._logger.warning(f"Error processing {csv_path} for ID inventory: {e}")
			
			# Mark this directory as scanned
			self._scanned_directories.add(output_dir_str)
			if len(self._id_field_inventory) > 0:
				self._logger.info(f"ID field inventory built with {len(self._id_field_inventory)} fields")
			
		except Exception as e:
			self._logger.warning(f"Error building ID field inventory: {e}")
	
	@lru_cache(maxsize=64)
	def _load_existing_id_values(self, field_name: str) -> Optional[List]:
		"""Load existing ID values from other datasets if available."""
		# Check if cross-dataset ID relationships are enabled
		maintain_relationships = self._config.get_config('maintain_id_relationships', True)
		if not maintain_relationships:
			return None
			
		# Check if this field is in the list of fields to maintain relationships for
		relationship_fields = self._config.get_config('id_relationship_fields', [])
		if relationship_fields and field_name not in relationship_fields:
			return None
		
		# Check global cache first
		if field_name in self._global_id_cache:
			return self._global_id_cache[field_name]
			
		# Initialize inventory if needed
		if not hasattr(self, '_id_field_inventory') or not self._id_field_inventory:
			self._initialize_id_inventory()
			
		# Get values from inventory
		if field_name in self._id_field_inventory:
			# Combine all values from all sources for this field
			all_values = []
			for source_info in self._id_field_inventory[field_name]:
				if source_info['source'] != f"{self._dataset_name}_cooked.csv":  # Skip current dataset
					all_values.extend(source_info['values'])
			
			if all_values:
				# Store in global cache and return
				self._global_id_cache[field_name] = all_values
				return all_values
		
		return None

	@classmethod
	@lru_cache(maxsize=1)
	def _load_domain_patterns(cls) -> Dict[str, List[str]]:
		"""Load common field naming patterns for different domains."""
		return {
			# Financial fields
			'price': ['price', 'amount', 'cost', 'value', 'fee', 'balance'],
			'percentage': ['rate', 'percent', 'ratio', 'yield', 'return', 'interest', 'discount'],
			'currency': ['currency', 'curr', 'iso_code'],
			'transaction': ['transaction'],
			
			# Market data
			'market': ['market', 'exchange', 'index', 'ticker', 'symbol'],
			'stock': ['open', 'close', 'high', 'low', 'volume', 'adj'],
			'volatility': ['volatility', 'deviation', 'var', 'risk'],
			
			# Risk analysis
			'risk': ['risk', 'score', 'rating', 'grade', 'level'],
			'probability': ['probability', 'likelihood', 'chance'],
			'status': ['status', 'state', 'flag', 'indicator'],
			
			# Time fields
			'date': ['date', 'time', 'day', 'month', 'year', 'quarter'],
			
			# Personal data
			'person': ['name', 'customer', 'client', 'user', 'person'],
			'contact': ['email', 'phone', 'address', 'contact'],
			
			# Identifiers
			'id': ['id', 'key', 'code', 'number', 'no', 'uuid', 'guid'],
			
			# Descriptive
			'category': ['category', 'type', 'class', 'group', 'segment']
		}
	
	def transform(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Generate synthetic data for fields marked for generation in the dataframe."""
		if df.empty:
			return df
			
		try:
			# Use instance dataset name or get from config
			dataset_name = self._dataset_name or self._config.get_config("current_dataset", "")
			if not dataset_name:
				self._logger.warning("No dataset name specified for data generation")
				return df
				
			# Get schema for target dataset
			schema = self._config.get_schema_for_dataset(dataset_name)
			if not schema:
				self._logger.warning(f"No schema found for dataset {dataset_name}")
				return df
			
			# Get list of fields that need synthetic data generation
			fields_for_generation = self._config.get_config(f"{dataset_name}_synthetic_fields", [])
			if not fields_for_generation:
				self._logger.info("No fields marked for synthetic data generation")
				return df
			
			# Track values for related field generation
			generated_values = {}
			row_count = len(df)
			
			# Generate synthetic data in two passes:
			# First pass: Handle independent fields
			for field_name in fields_for_generation:
				if field_name in schema:
					sql_type = schema[field_name]
					field_domain = self._infer_field_domain(field_name)
					
					self._logger.debug(f"Generating data for {field_name} ({sql_type}, domain: {field_domain})")
					
					# Generate data with domain knowledge
					df[field_name] = self._generate_data(
						row_count=row_count,
						field_name=field_name,
						sql_type=sql_type,
						domain=field_domain
					)
					
					# Store values for potential use in related field generation
					generated_values[field_name] = df[field_name]
				else:
					self._logger.warning(f"Field {field_name} not found in schema, skipping generation")
			
			# Second pass: Process inter-related fields (e.g., ensuring high > open > low)
			self._post_process_related_fields(df, generated_values)
			
			# Ensure uniqueness for composite key fields
			df = self._ensure_unique_combinations(df)

			# Ensure unique CustomerID + Date combinations to prevent PK violations
			df = self._ensure_customer_date_uniqueness(df)
			
			return df
		except Exception as e:
			self._logger.error(f"Error in data generation: {e}")
			traceback.print_exc()
			return df
	
	@lru_cache(maxsize=128)
	def _infer_field_domain(self, field_name: str) -> str:
		"""Infer the domain of a field based on its name."""
		field_lower = field_name.lower()
		
		# Check each domain pattern
		for domain, patterns in self._domain_patterns.items():
			for pattern in patterns:
				if pattern in field_lower:
					return domain
		
		# Default domain
		return 'generic'
	
	def _generate_data(self, row_count: int, field_name: str, 
					sql_type: str, domain: str) -> pd.Series:
		"""Generate realistic data based on field type and domain."""
		try:
			# Handle different SQL types with domain knowledge
			if "VARCHAR" in sql_type or "NVARCHAR" in sql_type:
				return self._generate_text_data(row_count, field_name, sql_type, domain)
				
			elif "INT" in sql_type:
				return self._generate_integer_data(row_count, field_name, domain)
				
			elif "DECIMAL" in sql_type or "NUMERIC" in sql_type or "FLOAT" in sql_type:
				return self._generate_decimal_data(row_count, field_name, sql_type, domain)
				
			elif "DATE" in sql_type:
				return self._generate_date_data(row_count, field_name, domain)
				
			elif "DATETIME" in sql_type or "TIMESTAMP" in sql_type:
				return self._generate_datetime_data(row_count, field_name, domain)
				
			elif "BIT" in sql_type or "BOOLEAN" in sql_type:
				return self._generate_boolean_data(row_count, field_name, domain)
				
			else:
				self._logger.warning(f"Unknown SQL type: {sql_type}, using generic generation")
				return pd.Series([f"{field_name}_{i}" for i in range(row_count)])
				
		except Exception as e:
			self._logger.error(f"Error generating data for {field_name}: {e}")
			return pd.Series([None] * row_count)
	
	def _post_process_related_fields(self, df: pd.DataFrame, generated_values: Dict[str, pd.Series]) -> None:
		"""Ensure consistency between related fields like High/Low prices."""
		try:
			# Compile regex patterns for field name matching
			stock_price_fields = re.compile(r'^(Open|Close|Highest|Lowest)Value$')
			start_date_pattern = re.compile(r'start.*date', re.IGNORECASE)
			end_date_pattern = re.compile(r'end.*date', re.IGNORECASE)
			duration_pattern = re.compile(r'duration|months$', re.IGNORECASE)
			inflation_pattern = re.compile(r'inflation', re.IGNORECASE)
			index_pattern = re.compile(r'(^index|index$|\bindex\b|price.*index)', re.IGNORECASE)
			ratio_pattern = re.compile(r'ratio|rate|percent', re.IGNORECASE)
			interest_pattern = re.compile(r'interest.*rate', re.IGNORECASE)
			
			# Handle stock price relationships
			price_fields = [col for col in df.columns if stock_price_fields.match(col)]
			if len(price_fields) >= 4:
				# Ensure High >= Open, Close >= Low, High >= Low with reasonable bounds
				high = df['HighestValue']
				low = df['LowestValue']
				open_val = df['OpenValue']
				close = df['CloseValue']

				if all(col in df.columns for col in ['OpenValue', 'CloseValue', 'HighestValue', 'LowestValue']):
					# First ensure High > Low
					invalid_high_low = df['HighestValue'] < df['LowestValue']
					if invalid_high_low.any():
						temp = df.loc[invalid_high_low, 'HighestValue'].copy()
						df.loc[invalid_high_low, 'HighestValue'] = df.loc[invalid_high_low, 'LowestValue']
						df.loc[invalid_high_low, 'LowestValue'] = temp
				
				# Calculate reasonable bounds based on average price
				avg_price = (high + low + open_val + close) / 4
				max_deviation = 0.1 * avg_price  # Max 10% deviation
				
				# Get the max and min for each row with constraints
				df['HighestValue'] = np.minimum(
					np.maximum.reduce([high, open_val, close]),
					avg_price + max_deviation  # Upper bound
				)
				
				df['LowestValue'] = np.maximum(
					np.minimum.reduce([low, open_val, close]),
					avg_price - max_deviation  # Lower bound
				)
			
			# Handle date relationships (start_date < end_date)
			for start_field in [col for col in df.columns if start_date_pattern.search(col)]:
				end_field_name = start_date_pattern.sub('end_date', start_field.lower())
				matching_end_fields = [col for col in df.columns if col.lower() == end_field_name]
				
				if matching_end_fields:
					end_field = matching_end_fields[0]
					# Ensure end_date >= start_date
					if pd.api.types.is_datetime64_dtype(df[start_field]):
						# Add random days (1 to 30) to start date
						df[end_field] = df[start_field] + pd.to_timedelta(
							np.random.randint(1, 30, size=len(df)), unit='d')
			
			# Handle percentage relationships
			pct_fields = [col for col in df.columns if ratio_pattern.search(col.lower())]
			for field in pct_fields:
				# Skip specific fields that shouldn't be treated as ratios
				if duration_pattern.search(field.lower()):
					continue

				# Skip interest rates which should not be normalized
				if interest_pattern.search(field.lower()):
					continue
				
				# Skip inflation and index fields which are not normalized
				if pd.api.types.is_numeric_dtype(df[field]):
					# Skip inflation which can be negative
					if inflation_pattern.search(field.lower()) or index_pattern.search(field.lower()):
						continue
						
					# Handle ratio fields
					if ratio_pattern.search(field.lower()) and not df[field].isna().all():
						non_null_mask = ~df[field].isna()
						if non_null_mask.any():
							# Get existing values
							values = df.loc[non_null_mask, field]
							
							# Check if normalization is needed (any value outside 0-1)
							if (values > 1).any() or (values < 0).any():
								# Get min and max for normalization
								min_val = values.min()
								max_val = values.max()
								
								# Only normalize if we have a valid range
								if max_val > min_val:
									self._logger.debug(f"Normalizing {field} values to range [0,1]")
									# Apply normalization formula: (x - min) / (max - min)
									df.loc[non_null_mask, field] = ((values - min_val) / (max_val - min_val)).round(2)
								else:
									# If all values are the same, set to 0.5
									df.loc[non_null_mask, field] = 0.5
					else:
						# For non-ratio percentage fields, ensure they're between 0-100
						df[field] = df[field].clip(0, 100)
			
			# Handle loan data relationships
			if all(field in df.columns for field in ['LoanAmount', 'LoanDurationMonths']):
				# Ensure loan duration is a reasonable integer value (minimum 3 months)
				if pd.api.types.is_numeric_dtype(df['LoanDurationMonths']):
					# If it's been normalized to 0-1 range, restore to sensible values
					if df['LoanDurationMonths'].max() <= 1.0:
						# Convert to common loan durations with minimum of 3 months
						loan_durations = [3, 6, 12, 24, 36, 48, 60]
						df['LoanDurationMonths'] = pd.qcut(df['LoanDurationMonths'], 
														len(loan_durations), 
														labels=loan_durations).astype(int)
						self._logger.debug("Fixed LoanDurationMonths to use standard loan terms")
					else:
						# Ensure minimum duration if already integer values
						df['LoanDurationMonths'] = df['LoanDurationMonths'].clip(3, None)
						
				# Calculate reasonable monthly payment if needed
				if 'MonthlyPayment' in df.columns:
					# Calculate monthly payment using simple amortization
					if 'InterestRate' in df.columns:
						# P = L[r(1+r)^n]/[(1+r)^n-1] where:
						# P = monthly payment, L = loan amount, r = monthly interest rate, n = number of payments
						monthly_rate = df['InterestRate'] / 100 / 12  # Convert annual rate to monthly
						num_payments = df['LoanDurationMonths']
						
						df['MonthlyPayment'] = df.apply(
							lambda row: row['LoanAmount'] * (row['InterestRate'] / 100 / 12) * 
										(1 + row['InterestRate'] / 100 / 12) ** row['LoanDurationMonths'] / 
										((1 + row['InterestRate'] / 100 / 12) ** row['LoanDurationMonths'] - 1)
							if row['InterestRate'] > 0 and row['LoanDurationMonths'] > 0
							else row['LoanAmount'] / row['LoanDurationMonths'],
							axis=1
						)
						
						# Handle any potential NaN or Inf values
						df['MonthlyPayment'] = df['MonthlyPayment'].replace([np.inf, -np.inf], np.nan)
						df['MonthlyPayment'] = df['MonthlyPayment'].fillna(
							df['LoanAmount'] / df['LoanDurationMonths']
						)
					
						# Round to 2 decimal places
						df['MonthlyPayment'] = df['MonthlyPayment'].round(2)
			
			# Enforce precision for all ratio fields
			for field in df.columns:
				field_lower = field.lower()
				if 'ratio' in field_lower or 'rate' in field_lower:
					if pd.api.types.is_numeric_dtype(df[field]):
						self._logger.debug(f"Enforcing 2 decimal places precision for {field} (SQL compatibility)")
						df[field] = df[field].round(2)
						
				# Special handling for GDP field to ensure it's integer
				if field == 'GDP' and pd.api.types.is_numeric_dtype(df[field]):
					df[field] = df[field].round().astype('Int64')
					
		except Exception as e:
			self._logger.error(f"Error in post-processing related fields: {e}")
			traceback.print_exc()

	def _generate_text_data(self, row_count: int, field_name: str, sql_type: str, domain: str) -> pd.Series:
		"""Generate realistic text data based on domain."""
		# Extract max length if specified
		max_length = 50  # Default
		if "(" in sql_type and ")" in sql_type:
			try:
				max_length = int(sql_type.split('(')[1].split(')')[0])
			except (ValueError, IndexError):
				pass
				
		# Special handling for AccountID
		if field_name == 'AccountID' or (field_name.lower().endswith('id') and 'account' in field_name.lower()):
			self._logger.debug(f"Generating unique AccountIDs")
			# Generate unique account IDs
			return pd.Series([self._generate_unique_account_id() for _ in range(row_count)])
				
		# Generate based on domain
		if domain == 'currency':
			# ISO currency codes
			currencies = ['USD']
			return pd.Series(np.random.choice(currencies, size=row_count))
			
		elif domain == 'market':
			markets = ['NASDAQ']
			return pd.Series(np.random.choice(markets, size=row_count))
			
		elif domain == 'status':
			statuses = ['On Time', 'Late', 'Defaulted']
			return pd.Series(np.random.choice(statuses, size=row_count))
			
		elif domain == 'category':
			categories = ['Corporate', 'Personal', 'Checking', 'Savings', 'Investment', 'Fund']
			return pd.Series(np.random.choice(categories, size=row_count))
		
		elif domain == 'transaction':
			transactions = ['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'Refund']
			return pd.Series(np.random.choice(transactions, size=row_count))
		
		elif domain =='id':
			prefix = field_name[:3].upper()  # Use prefix from field name
			return pd.Series([f"{prefix}{np.random.randint(100000, 999999)}" for _ in range(row_count)])
			
		elif domain == 'person':
			first_names = ['John', 'Emma', 'Michael', 'Sophia', 'William', 'Olivia', 
						'James', 'Ava', 'Robert', 'Isabella']
			last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 
						'Miller', 'Wilson', 'Taylor', 'Anderson']
			
			if max_length < 15:  # Just last name
				return pd.Series(np.random.choice(last_names, size=row_count))
			else:  # Full name
				return pd.Series([
					f"{np.random.choice(first_names)} {np.random.choice(last_names)}" 
					for _ in range(row_count)
				])
				
		else:
			# Generic strings with some variety
			import string
			word_starts = list(string.ascii_uppercase)
			word_continues = list(string.ascii_lowercase)
			
			return pd.Series([
				''.join([
					np.random.choice(word_starts)] + 
					[np.random.choice(word_continues) for _ in range(min(max_length - 1, 
																np.random.randint(3, 10)))]
				) for _ in range(row_count)
			])
	
	def _generate_integer_data(self, row_count: int, field_name: str, domain: str) -> pd.Series:
		"""Generate realistic integer data based on domain."""
		field_lower = field_name.lower()
		
		# Check if this is an ID field that should reference other datasets
		if domain == 'id' or field_lower.endswith('id'):
			# Try to load existing values for this ID field from other datasets
			existing_values = self._load_existing_id_values(field_name)
			
			if existing_values and len(existing_values) > 0:
				# We have existing values to sample from
				self._logger.debug(f"Reusing existing {field_name} values from other datasets")
				
				# If we have enough values, sample without replacement
				if len(existing_values) >= row_count:
					return pd.Series(np.random.choice(existing_values, size=row_count, replace=False))
				# Otherwise, sample with replacement to fill all rows
				else:
					return pd.Series(np.random.choice(existing_values, size=row_count, replace=True))
			else:
				# Sequential IDs starting from a random offset
				start = np.random.randint(1000, 9999)
				return pd.Series(range(start, start + row_count))
			
		# Generate volume data
		elif domain == 'stock' and 'volume' in field_lower:
			# Trading volumes typically follow log-normal distribution
			return pd.Series(np.random.lognormal(mean=10, sigma=1, size=row_count).astype(int))
			
		# Generate count data
		elif any(term in field_lower for term in ['count', 'num', 'qty']):
			# Smaller counts with right-skewed distribution
			return pd.Series(np.random.geometric(p=0.2, size=row_count))
			
		# Generate score/rating data
		elif domain == 'risk' or domain == 'score' or 'score' in field_lower:
			# Normally distributed scores around a center point
			if 'percent' in field_lower:
				# 0-100 scale
				return pd.Series(np.random.normal(loc=70, scale=15, size=row_count)
							.clip(0, 100).astype(int))
			else:
				# 1-10 scale
				return pd.Series(np.random.normal(loc=5, scale=2, size=row_count)
							.clip(1, 10).round().astype(int))
		
		# Default: random integers in a reasonable range
		else:
			return pd.Series(np.random.randint(1, 1000, size=row_count))
	
	def _generate_decimal_data(self, row_count: int, field_name: str, sql_type: str, domain: str) -> pd.Series:
		"""Generate realistic decimal data based on domain."""
		# Extract scale if specified
		scale = 2  # Default 2 decimal places
		if "(" in sql_type and ")" in sql_type:
			try:
				parts = sql_type.split('(')[1].split(')')[0].split(',')
				if len(parts) >= 2:
					scale = int(parts[1].strip())
			except (ValueError, IndexError):
				pass
		
		# Special handling for specific fields to ensure SQL compatibility
		field_lower = field_name.lower()
		if any(term in field_lower for term in ['ratio', 'rate', 'percent']):
			# Ensure ratio fields never exceed 2 decimal places regardless of SQL type
			scale = min(scale, 2)
			
		# Force integer values for GDP field
		if field_name == 'GDP':
			values = np.random.randint(100000, 10000000, size=row_count)
			return pd.Series(values)
			
		# Price values
		if domain == 'price':
			# Log-normal distribution for prices (right-skewed)
			if 'price' in field_name.lower():
				values = np.random.lognormal(mean=4.0, sigma=0.5, size=row_count)
				return pd.Series(np.round(values, scale))
			# Money amounts
			else:
				values = np.random.lognormal(mean=6.0, sigma=1.2, size=row_count)
				return pd.Series(np.round(values, scale))
				
		# Stock market data
		elif domain == 'stock':
			# For HighestValue and LowestValue, ensure proper relationship
			if 'high' in field_name.lower():
				# Generate reasonable stock prices (log-normal distribution)
				base_price = 50.0
				values = np.random.lognormal(mean=4.0, sigma=0.5, size=row_count)
				# Store for reference when generating lowest values
				self._highest_values = values
				return pd.Series(np.round(values, scale))
			elif 'low' in field_name.lower():
				# If highest values were generated, make lows a percentage lower
				if hasattr(self, '_highest_values') and self._highest_values is not None:
					# Make lows 1-15% lower than highs
					discount = np.random.uniform(0.01, 0.15, size=row_count)
					values = self._highest_values * (1 - discount)
					return pd.Series(np.round(values, scale))
				
		# Percentage or rate values
		elif domain == 'percentage':
			if 'ratio' in field_name.lower():
				# Ratios between 0-1, guaranteeing SQL DECIMAL(18,2) compatibility
				values = np.random.beta(2, 5, size=row_count)  # Right-skewed beta distribution
				return pd.Series(np.round(values, scale))
			else:
				# Percentages between 0-100
				values = np.random.beta(2, 5, size=row_count) * 100
				return pd.Series(np.round(values, scale))
				
		# Risk metrics
		elif domain == 'risk' or domain == 'volatility':
			# Usually small positive values
			values = np.abs(np.random.normal(0.05, 0.02, size=row_count))
			return pd.Series(np.round(values, scale))
			
		# Interest rates
		if 'interestrate' in field_name.lower():
			# Generate interest rates within the valid range (0-30%)
			# Use beta distribution for a realistic right-skewed distribution
			# (most loans have lower rates, few have very high rates)
			values = np.random.beta(2, 5, size=row_count) * 29.0 + 1.0
			return pd.Series(values)


		# Generic numeric data
		else:
			values = np.random.uniform(0, 100, size=row_count)
			return pd.Series(np.round(values, scale))
		
	def _generate_unique_account_id(self, prefix='ACC', length=9):
		"""Generate a unique account ID that doesn't exist in the global registry."""
		while True:
			# Generate random numeric part with leading zeros
			numeric_part = str(random.randint(100000, 999999)).zfill(6)
			account_id = f"{prefix}{numeric_part}"
			
			# Check if it's unique across all instances
			if account_id not in DataGenerationStrategy._global_account_registry:
				# Add to both local and global registries
				self._generated_account_ids.add(account_id)
				DataGenerationStrategy._global_account_registry.add(account_id)
				return account_id

	def _generate_date_data(self, row_count: int, field_name: str, domain: str) -> pd.Series:
		"""Generate realistic date data based on domain."""
		field_lower = field_name.lower()
		today = pd.Timestamp.now().normalize()
		
		# Historical dates (for reports, transactions)
		if any(term in field_lower for term in ['date', 'created', 'transaction']):
			# Past dates, weighted toward recent (last 2 years)
			start_date = today - pd.Timedelta(days=365*2)
			# Exponential distribution - more recent dates are more common
			days_ago = np.random.exponential(scale=180, size=row_count).astype(int)
			days_ago = np.minimum(days_ago, 365*2)  # Cap at 2 years
			return pd.Series([today - pd.Timedelta(days=int(days)) for days in days_ago])
			
		# Future dates (for maturities, expirations)
		elif any(term in field_lower for term in ['maturity', 'expiry', 'due']):
			# Future dates, 1-10 years out
			days_ahead = np.random.randint(365, 365*10, size=row_count)
			return pd.Series([today + pd.Timedelta(days=days) for days in days_ahead])
			
		# Report dates or as-of dates
		elif 'report' in field_lower or 'as_of' in field_lower:
			# Monthly or quarterly dates
			start_date = today - pd.Timedelta(days=365*5)  # 5 years of history
			# Generate month-end or quarter-end dates
			month_offsets = np.random.randint(0, 60, size=row_count)  # 60 months = 5 years
			dates = [
				pd.Timestamp(year=start_date.year + (start_date.month + offset - 1) // 12,
						month=((start_date.month + offset - 1) % 12) + 1,
						day=1) for offset in month_offsets
			]
			# Convert to month-end
			return pd.Series([date + pd.offsets.MonthEnd(0) for date in dates])
			
		# Default: random dates within reasonable range
		else:
			# Past 5 years
			start_date = today - pd.Timedelta(days=365*5)
			days_range = (today - start_date).days
			random_days = np.random.randint(0, days_range, size=row_count)
			return pd.Series([start_date + pd.Timedelta(days=days) for days in random_days])
	
	def _generate_datetime_data(self, row_count: int, field_name: str, domain: str) -> pd.Series:
		"""Generate realistic datetime data based on domain."""
		# Get dates first
		dates = self._generate_date_data(row_count, field_name, domain)
		
		field_lower = field_name.lower()
		
		# Add times based on context
		if 'transaction' in field_lower or 'timestamp' in field_lower:
			# Transactions happen during business hours, with some after-hours
			hours = np.random.choice([9, 10, 11, 12, 13, 14, 15, 16], size=row_count, 
								p=[0.1, 0.15, 0.15, 0.2, 0.15, 0.1, 0.1, 0.05])
			minutes = np.random.randint(0, 60, size=row_count)
			seconds = np.random.randint(0, 60, size=row_count)
		else:
			# Generic times
			hours = np.random.randint(0, 24, size=row_count)
			minutes = np.random.randint(0, 60, size=row_count)
			seconds = np.random.randint(0, 60, size=row_count)
		
		# Combine dates and times
		return pd.Series([
			pd.Timestamp(
				year=date.year, month=date.month, day=date.day,
				hour=hour, minute=minute, second=second
			)
			for date, hour, minute, second in zip(dates, hours, minutes, seconds)
		])
	
	def _generate_boolean_data(self, row_count: int, field_name: str, domain: str) -> pd.Series:
		"""Generate realistic boolean data based on domain."""
		field_lower = field_name.lower()
		
		# Default probability
		true_prob = 0.5
		
		# Adjust probability based on field meaning
		if any(term in field_lower for term in ['active', 'valid', 'available']):
			true_prob = 0.8  # Most records are active/valid
		elif any(term in field_lower for term in ['deleted', 'failed', 'error']):
			true_prob = 0.1  # Few records are deleted/failed
		elif any(term in field_lower for term in ['verified', 'premium']):
			true_prob = 0.4  # Some records are verified/premium
		elif domain == 'flag':
			true_prob = 0.2  # Flags are typically rare
		
		# Generate with appropriate probability
		return pd.Series(np.random.choice([True, False], size=row_count, p=[true_prob, 1-true_prob]))

	def _ensure_unique_combinations(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Ensure uniqueness for specific field combinations based on dataset type."""
		try:
			dataset_name = self._dataset_name or self._config.get_config("current_dataset", "")
			
			# Apply dataset-specific uniqueness rules
			if dataset_name == 'Macro':
				# For Macro data, ensure ReportDate + CountryName combinations are unique
				if 'ReportDate' in df.columns and 'CountryName' in df.columns:
					# Check if we have duplicates
					duplicates = df.duplicated(subset=['ReportDate', 'CountryName'], keep=False)
					dup_count = duplicates.sum()
					
					if dup_count > 0:
						self._logger.warning(f"Found {dup_count} duplicate date/country combinations in Macro data. Keeping first occurrence only.")
						
						# Keep first occurrence of each unique combination
						df = df.drop_duplicates(subset=['ReportDate', 'CountryName'], keep='first')
			
			# Add similar uniqueness rules for other datasets as needed
			
			return df
			
		except Exception as e:
			self._logger.error(f"Error ensuring unique combinations: {e}")
			traceback.print_exc()
			return df

	def _ensure_customer_date_uniqueness(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Ensure Customer data has unique CustomerID + Date combinations."""
		if 'CustomerID' not in df.columns:
			return df
			
		try:
			# For customer data, we need to ensure no duplicate CustomerID + Date combinations
			if self._dataset_name == 'Customer':
				date_col = next((col for col in df.columns if 'Date' in col), None)
				
				if date_col:
					# Check if we have duplicates
					duplicates = df.duplicated(subset=['CustomerID', date_col], keep=False)
					dup_count = duplicates.sum()
					
					if dup_count > 0:
						self._logger.warning(f"Found {dup_count} duplicate CustomerID/Date combinations. Aggregating to ensure uniqueness.")
						
						# Group by CustomerID and date, aggregate financial metrics
						agg_dict = {
							'AnnualIncome': 'max',           # Use max income 
							'TotalAssets': 'sum',            # Sum assets
							'TotalLiabilities': 'sum',       # Sum liabilities
							'AnnualExpenses': 'sum',         # Sum expenses
							'MonthlySavings': 'mean',        # Average monthly savings
							'AnnualBonus': 'sum',            # Sum bonuses
							'DebtToIncomeRatio': 'mean',     # Average ratio
							'PaymentHistoryYears': 'max',    # Use max history
							'JobTenureMonths': 'max',        # Use max tenure
							'NumDependents': 'max'           # Use max dependents
						}
						
						# Only include columns that exist in the DataFrame
						agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}
						
						# Group by ID and date, aggregate other columns
						df = df.groupby(['CustomerID', date_col]).agg(agg_dict).reset_index()
						
						self._logger.info(f"Successfully aggregated data to {len(df)} unique customer records")
				
			return df
			
		except Exception as e:
			self._logger.error(f"Error ensuring customer uniqueness: {e}")
			traceback.print_exc()
			return df
		

#======== 2. Strategy Factory ==========
class TransformerStrategyFactory:
	"""Factory for creating transformation strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategies = {}
	
	def initialize_strategies(self, dataset_name: str) -> None:
		"""Initialize the registry with default strategies."""
		# Clear existing strategies if any
		self._strategies = {}
		
		# Create strategies with dataset name
		self.register('Schema_transformation', SchemaStrategy(self._config, dataset_name))
		self.register('Data_generation', DataGenerationStrategy(self._config))
	
	def register(self, name: str, strategy: TransformationStrategy) -> None:
		"""Register a transformation strategy."""
		if name in self._strategies:
			self._logger.warning(f"Strategy {name} already registered, overwriting")
		self._strategies[name] = strategy
	
	def get_strategy(self, name: str) -> TransformationStrategy:
		"""Get a transformation strategy by name."""
		if name not in self._strategies:
			raise ValueError(f"Unknown transformation strategy: {name}")
		return self._strategies[name]



#####################################################
#======== 3. Transformation Pipeline Steps ==========
class TransformationPipelineStep(IPipelineStep):
	"""Base class for preprocessing pipeline steps."""
	
	def __init__(self, strategy: TransformationStrategy):
		self._strategy = strategy
	
	@property
	def name(self) -> str:
		"""Get the name of this pipeline step."""
		return self._strategy.name
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process the data using the strategy."""
		return self._strategy.transform(df)


#======== 4. Transformation Pipeline ==========
class TransformationPipeline:
	"""Pipeline for data transformation."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._steps: List[IPipelineStep] = []
	
	def add_step(self, step: TransformationPipelineStep) -> TransformationPipeline:
		"""Add a transformation step to the pipeline."""
		self._steps.append(step)
		return self
	
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process data through the transformation pipeline."""
		result = df

		# Add steps counting logic
		steps_count = len(self._steps)
		for step_idx, step in enumerate(self._steps):
			step_start_time = datetime.now()
			try:
				self._logger.info(f"Applying transformation step {step_idx+1}/{steps_count}: {step.name}")
				result = step.execute(result)
				
				# Track execution time
				step_duration_ms = (datetime.now() - step_start_time).total_seconds() * 1000
				self._logger.debug(f"Step {step.name} completed in {step_duration_ms:.2f} ms")
				
				if result is None or result.empty:
					self._logger.error(f"Step {step.name} returned empty result")
					return pd.DataFrame()
					
			except Exception as e:
				self._logger.error(f"Error in transformation step {step.name}: {e}")
				traceback.print_exc()
				
		return result


##############################################
#======== 5. Transformer module API ==========
class Transformer:
	"""Main class for transforming datasets."""

	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategy_factory = TransformerStrategyFactory(config_provider)
		self._dataset_name = None
		self._data = None
		self._results = None
	
	def load_dataset(self, dataset_name: str, df: pd.DataFrame) -> Transformer:
		"""Load the dataset for transformation.
		
		Args:
			dataset_name: Name of the dataset
			df: Dataframe to transform
		"""
		self._dataset_name = dataset_name
		self._data = df
		self._config.set_config('current_dataset', dataset_name)
		
		# Initialize strategies with dataset context
		self._strategy_factory.initialize_strategies(dataset_name)
		
		self._logger.info(f"Loaded dataset for transformation: {dataset_name} with {len(df)} rows and {len(df.columns)} columns")
		return self
	
	def process(self) -> pd.DataFrame:
		"""Transform the dataset by applying all transformation steps."""
		if not self._dataset_name or self._data is None:
			self._logger.error("Dataset not loaded")
			return pd.DataFrame()
			
		start_time = datetime.now()
		self._logger.info(f"Starting transformation for {self._dataset_name}")
		
		# Create the transformation pipeline
		pipeline = self.create_pipeline()
		
		# Process the dataset
		transformed_data = pipeline.process(self._data)
		
		execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
		
		# Store results
		self._results = {
			"dataset_name": self._dataset_name,
			"row_count": len(transformed_data),
			"column_count": len(transformed_data.columns),
			"execution_time_ms": execution_time_ms
		}
		
		self._logger.info(f"Transformation completed in {execution_time_ms:.2f} ms")
		return transformed_data
	
	def get_results(self) -> Dict[str, Any]:
		"""Get the transformation results and metadata."""
		return self._results
	
	def save_results(self, output_path: Optional[Path] = None) -> bool:
		"""Save transformed data to disk."""
		if self._data is None or self._data.empty:
			self._logger.error("No transformed data to save")
			return False
			
		try:
			output_dir = Path(self._config.get_config('output_dir'))
			
			# Use provided path or default
			if output_path is None:
				output_path = output_dir / f"{self._dataset_name}_transformed.csv"
			
			# Ensure output directory exists
			output_path.parent.mkdir(parents=True, exist_ok=True)
			
			# Save the data
			self._data.to_csv(output_path, index=False)
			self._logger.info(f"Saved transformed data to {output_path}")
			
			return True
			
		except Exception as e:
			self._logger.error(f"Error saving transformed data: {e}")
			traceback.print_exc()
			return False
	
	def create_pipeline(self) -> TransformationPipeline:
		"""Create a transformation pipeline for the dataset."""
		pipeline = TransformationPipeline(self._config)
		
		# Add schema transformation step
		pipeline.add_step(TransformationPipelineStep(
			self._strategy_factory.get_strategy('Schema_transformation')
		))
		
		# Add data generation step
		pipeline.add_step(TransformationPipelineStep(
			self._strategy_factory.get_strategy('Data_generation')
		))
		
		return pipeline
	