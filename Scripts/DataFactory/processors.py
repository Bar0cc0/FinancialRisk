#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Financial Data Preprocessing Framework
======================================

This module provides an extensible framework for preprocessing financial datasets using
strategy pattern implementation. It supports a wide range of preprocessing operations
including text cleaning, numeric normalization, date/time parsing, categorical encoding,
and dataset concatenation.

The module implements a multi-stage processing pipeline that can be configured and extended
for different financial data preprocessing requirements.

Key Components
--------------
- Preprocessing Strategies: Implementations for specific data cleaning and transformation tasks
- Strategy Factory: Creates and manages preprocessing strategy instances
- Pipeline Steps: Wraps strategies for execution in a pipeline context
- Preprocessing Pipeline: Orchestrates sequential execution of preprocessing steps
- Preprocessor API: Main entry point for client code to access preprocessing functionality

Preprocessing Capabilities
-------------------------
- Text Cleaning: Clean special characters, convert string columns to numeric
- Numeric Processing: Imputation, normalization, outlier handling, type conversion
- DateTime Processing: Parse date strings, standardize duration values
- Categorical Processing: Encode binary columns, one-hot encoding, multi-value parsing
- Data Concatenation: Combine multiple datasets horizontally or vertically
- Cleanup Operations: Remove duplicates, handle missing values, drop unnecessary columns

Usage Example
-------------
from configuration import Configuration
from processors import Preprocessor
import pandas as pd

# Initialize configuration
config = Configuration()
config.load_from_file("config.yaml")

# Create preprocessor and load dataset
preprocessor = Preprocessor(config)
df = pd.read_csv("financial_data.csv")

# Process the dataset using the module API
processed_df = preprocessor.load_dataset("FinancialData", df).process()

# Save the processed data
preprocessor.save_results()
"""

#=================================================

from __future__ import annotations
import traceback
import re
from abc import abstractmethod
from pathlib import Path
from functools import lru_cache
from typing import List, Dict, Optional, Tuple, Any, Union
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.impute import KNNImputer

from interfaces import IConfigProvider, IPipelineStep, IDataPreprocessingStrategy



#======= 1. Preprocessing Strategies =======
class PreprocessingStrategy(IDataPreprocessingStrategy):
	"""Base class for preprocessing strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
	
	@property
	def name(self) -> str:
		"""Return the name of this strategy instance."""
		return self.__class__.__name__
	
	@abstractmethod
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Preprocess the dataframe according to the strategy."""
		pass
	
	def _log_changes(self, change_count: int, message: str) -> None:
		"""Log changes if there were any."""
		if change_count > 0:
			self._logger.debug(f"{self.name}: {change_count} {message}")


class TextCleaningStrategy(PreprocessingStrategy):
	"""Clean text data."""
	
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Clean text data in the dataframe."""
		if df.empty:
			return df
			
		df_copy = df.copy()
		
		# Handle duplicate column names
		if df_copy.columns.duplicated().any():
			df_copy.columns = self._handle_duplicate_column_names(df_copy.columns)
		
		# Clean special characters from string columns
		df_copy = self._clean_special_characters(df_copy)
		
		# Convert string columns to numeric where possible
		df_copy = self._convert_to_numeric(df_copy)
		
		return df_copy
	
	def _handle_duplicate_column_names(self, columns):
		"""Handle duplicate column names."""
		new_cols = []
		seen = {}
		
		for col in columns:
			if col in seen:
				seen[col] += 1
				new_cols.append(f"{col}_{seen[col]}")
			else:
				seen[col] = 0
				new_cols.append(col)
				
		self._logger.info(f"Renamed {sum(columns.duplicated())} duplicate column names")
		return new_cols
		
	def _clean_special_characters(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Clean special characters from string columns using vectorized operations."""
		cleaned_count = 0
		
		# Get all string columns at once
		str_cols = df.select_dtypes(include=['object']).columns
		
		for col in str_cols:
			# Use vectorized string operations
			if df[col].dtype == object:
				# Replace underscores
				if '_' in df[col].iloc[0:100].astype(str).str.cat():
					df[col] = df[col].str.replace('_', '')
					cleaned_count += len(df)
				
				# Non-printable characters check and replacement
				mask = df[col].astype(str).str.contains('[^\x20-\x7E]', na=False)
				if mask.any():
					df.loc[mask, col] = pd.NA
					cleaned_count += mask.sum()
		
		# Log changes
		if cleaned_count > 0:
			self._log_changes(cleaned_count, 'values with special characters cleaned')
			
		return df
	
	def _convert_to_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Convert string columns to numeric where possible."""
		min_numeric_percent = self._config.get_config('min_numeric_percent', 0.5)
		converted_count = 0
		
		for col in df.select_dtypes(include=['object']).columns:
			numeric_series = pd.to_numeric(df[col], errors='coerce')
			non_na_pct = numeric_series.notna().mean()
			
			if non_na_pct >= min_numeric_percent:
				df[col] = numeric_series
				converted_count += 1
		
		if converted_count > 0:
			self._log_changes(converted_count, "columns converted from string to numeric")
			
		return df


class NumericPreprocessingStrategy(PreprocessingStrategy):
	"""Handle numeric data preprocessing tasks."""
	
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Run the complete numeric preprocessing pipeline."""
		if df.empty:
			return df
			
		df_copy = df.copy()
		
		# Get numeric columns
		df_num = df_copy.select_dtypes(include=[np.number])
		if df_num.empty:
			self._logger.warning("No numeric columns found for processing")
			return df_copy
			
		# Impute missing values
		df_num_imputed = self._impute_missing_values(df_copy)
		if df_num_imputed is None:
			self._logger.error("Imputation failed, skipping further numeric processing")
			return df_copy
				
		# Normalize if requested
		normalize = self._config.get_config('normalize_numeric', False)
		if normalize:
			df_num_imputed = self._normalize_data(df_num_imputed)
		
		# Round to 2 decimal places
		rounding = self._config.get_config('decimal_rounding', 2)
		if rounding > 0:
			df_num_imputed = df_num_imputed.round(rounding)

		# Replace columns in main dataframe
		try:
			# Drop original numeric columns
			df_copy = df_copy.drop(columns=[col for col in df_copy.columns if col in df_num.columns])
			# Add back processed columns
			df_copy = pd.concat([df_copy, df_num_imputed], axis=1)
			self._logger.debug("Successfully updated dataframe with processed numeric columns")
		except Exception as e:
			self._logger.error(f"Error when updating dataframe with processed features: {e}")
		
		# Convert float to integer where appropriate
		df_copy = self._convert_float_to_integer(df_copy)
		
		# Drop duplicated columns
		df_copy = self._drop_duplicated_columns(df_copy)
		
		return df_copy

	@staticmethod
	@lru_cache(maxsize=128)
	def _get_optimal_knn_params(sample_size: int, missing_ratio: float) -> dict:
		"""Get optimal KNN parameters based on dataset characteristics."""
		if sample_size < 20:
			return {'use_knn': False, 'n_neighbors': None}
		if sample_size < 100:
			return {'use_knn': True, 'n_neighbors': min(3, sample_size // 4)}
		elif sample_size < 1000:
			return {'use_knn': True, 'n_neighbors': min(5, sample_size // 100)}
		else:
			# For large datasets, limit neighbors to improve performance
			return {'use_knn': True, 'n_neighbors': min(10, int(sample_size ** 0.5 / 10))}
			
	def _impute_missing_values(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
		"""
		Optimized imputation using smarter KNN implementation.
		- For binary columns: uses random imputation (unchanged)
		- For non-binary columns: uses feature selection and optimized KNN parameters
		"""
		# Get numeric columns
		df_num = df.select_dtypes(include=[np.number])
		
		if df_num.empty:
			return None

		try:
			# Calculate sample size and missing ratio
			sample_size = len(df_num)
			missing_ratio = df_num.isnull().mean().mean()
			
			# Get optimal parameters using cached function
			knn_params = self._get_optimal_knn_params(sample_size, missing_ratio)
	
			# Remove columns with all NaN values
			df_num = df_num.dropna(axis=1, how='all')
			
			# Replace infinity values
			df_num = df_num.replace([np.inf, -np.inf], np.nan)
			
			# Create a copy to store results
			data_imputed = df_num.copy()
			
			# Separate binary and non-binary columns
			binary_cols = []
			non_binary_cols = []
			
			for col in df_num.columns:
				unique_vals = df_num[col].dropna().unique()
				if len(unique_vals) == 2:
					binary_cols.append(col)
				else:
					non_binary_cols.append(col)
			
			# Step 1: Handle binary columns with random imputation
			if binary_cols:
				self._logger.debug(f"Found binary columns {binary_cols}: using random imputation")
				for col in binary_cols:
					# Get the two unique values (typically 0 and 1)
					values = list(df_num[col].dropna().unique())
					# Create mask for missing values
					mask = df_num[col].isna()
					if mask.any():
						# Randomly assign 0 or 1 to missing values
						random_choices = np.random.choice(values, size=mask.sum())
						data_imputed.loc[mask, col] = random_choices
			
			# Step 2: KNN imputation for non-binary columns
			if non_binary_cols:
				# Only process columns that actually have missing values
				cols_with_missing = [col for col in non_binary_cols if df_num[col].isna().any()]
				
				if cols_with_missing:
					
					# Adjust KNN parameters based on dataset size
					if knn_params['use_knn']:
						n_neighbors = knn_params['n_neighbors']
					else:
						# For very small datasets, use simpler imputation
						for col in cols_with_missing:
							data_imputed[col] = data_imputed[col].fillna(data_imputed[col].median())
						self._logger.debug(f"Using median imputation for small dataset (n={sample_size})")
						# If KNN is not used, return the imputed dataframe
						return data_imputed
					
					# Continue with KNN imputation for larger datasets
					# Use correlation-based feature selection
					feature_subset = []
					correlation_threshold = self._config.get_config('correlation_threshold', 0.3)
					
					for col in cols_with_missing:
						# Find correlated features to use for KNN distance calculation
						corr_matrix = df_num[non_binary_cols].corr().abs()
						if col in corr_matrix.columns:
							# Get correlated columns above threshold (excluding self-correlation)
							corr_cols = corr_matrix[col].drop(col) if col in corr_matrix[col].index else corr_matrix[col]
							corr_cols = corr_cols[corr_cols > correlation_threshold].index.tolist()
							feature_subset.extend(corr_cols)
					
					# Ensure we include columns with missing values in the feature set
					feature_subset = list(set(feature_subset + cols_with_missing))
					
					# If no good correlations, fall back to using all non-binary columns
					if not feature_subset:
						feature_subset = non_binary_cols
					
					# Apply KNN imputation
					imputer = KNNImputer(n_neighbors=n_neighbors)
					
					# Use feature subset for imputation distance calculation
					imputed_array = imputer.fit_transform(df_num[feature_subset])
					
					# Update only columns that had missing values
					for i, col in enumerate(feature_subset):
						if col in cols_with_missing:
							data_imputed[col] = imputed_array[:, i]
				
				if len(cols_with_missing) > 0:
					self._logger.info(f"Successfully imputed missing values in {len(cols_with_missing)} numeric columns")
			
			return data_imputed
				
		except Exception as e:
			self._logger.error(f'Error during imputation: {e}')
			traceback.print_exc()
			return None

	@staticmethod
	@lru_cache(maxsize=32)
	def _should_normalize_cached(column_name: str, skewness: float) -> bool:
		"""Determine if a column should be normalized."""
		# Check for columns that typically shouldn't be normalized
		lower_name = column_name.lower()
		if any(pattern in lower_name for pattern in ('id', 'count', 'num_', 'flag', 'indicator', 'binary')):
			return False
			
		# Normalize if distribution is skewed
		return abs(skewness) > 1.0
	
	def _should_normalize(self, df: pd.DataFrame, column: str) -> bool:
		"""Determine if a column should be normalized."""		
		# Calculate skewness
		try:
			skewness = df[column].skew()
		except:
			skewness = 0
			
		# Use cached function for the decision
		return self._should_normalize_cached(column, skewness)

	@staticmethod
	@lru_cache(maxsize=128)
	def _get_std(df: pd.DataFrame) -> float:
		"""Calculate the standard deviation of the numeric columns."""
		return df.std().replace(0.0, 1.0) # Avoid division by zero

	def _normalize_data(self, df_num: pd.DataFrame) -> pd.DataFrame:
		"""Normalize numeric columns using z-score."""
		if df_num.empty:
			return df_num
		
		normalized_count = 0
		try:
			# Get standard deviation of numeric columns
			std = self._get_std(df_num)

			# Normalize using z-score
			for col in df_num.columns:
				if self._should_normalize(df_num, col):
					df_num[col] = (df_num[col] - df_num[col].mean()) / std[col] 
					normalized_count += 1

			self._log_changes(normalized_count, "numeric columns normalized")	
			return df_num
			
		except Exception as e:
			self._logger.error(f"Error normalizing data: {e}")
			return df_num
	
	def _convert_float_to_integer(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Convert float columns to integer where appropriate."""
		df_copy = df.copy()
		
		# Get all float columns
		float_cols = df_copy.select_dtypes(include=['float']).columns.tolist()
		
		if not float_cols:
			return df_copy
		
		integer_patterns = self._config.get_config('integer_patterns', 
												['num_', 'number', 'count', 'qtd', 'qty', '_id', '_nbr', 'age', 'delayed'])
		converted_count = 0
		
		for col in float_cols:
			# Check if column name suggests it should be integer
			should_be_int = any(pattern in col.lower() for pattern in integer_patterns)
			
			# Check if values are whole numbers
			try:
				is_whole = np.allclose(df_copy[col].dropna(), df_copy[col].dropna().round(), rtol=1e-05, atol=1e-08)
				if should_be_int or is_whole:
					df_copy[col] = df_copy[col].fillna(0).astype(int)
					converted_count += 1
			except Exception as e:
				self._logger.warning(f"Error checking if {col} contains whole numbers: {e}")
		
		self._log_changes(converted_count, "float columns converted to integer")
		return df_copy
	
	def _drop_duplicated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Drop highly correlated or identical numeric columns while preserving mapped fields."""
		df_copy = df.copy()
		
		if df_copy.empty:
			return df_copy
			
		original_columns = len(df_copy.columns)
		
		# Get the current dataset name
		dataset_name = self._config.get_config("current_dataset", "")
		
		# Get field mappings for the current dataset
		columns_to_preserve = set()
		try:
			datasets_config = self._config.get_config('Datasets', {})
			if dataset_name in datasets_config:
				field_mappings = datasets_config[dataset_name].get('field_mappings', [])
				# Extract source column names that need to be preserved
				for mapping in field_mappings:
					if isinstance(mapping, dict):
						# Add all source field values to the preserve list
						columns_to_preserve.update(mapping.values())
		except Exception as e:
			self._logger.warning(f"Error getting field mappings: {e}")
		
		# Remove None values from the set
		columns_to_preserve = {col for col in columns_to_preserve if col is not None}
		
		# Log preserved columns for debugging
		if columns_to_preserve:
			self._logger.debug(f"Preserving mapped columns during redundancy check: {columns_to_preserve}")
		
		# Step 1: Drop columns with the exact same name (but preserve mapped ones)
		duplicate_cols = df_copy.columns[df_copy.columns.duplicated()].tolist()
		cols_to_drop = [col for col in duplicate_cols if col not in columns_to_preserve]
		
		if cols_to_drop:
			df_copy = df_copy.drop(columns=cols_to_drop)
			self._log_changes(len(cols_to_drop), "duplicate named columns dropped")
		

		# Step 2: Find numeric columns with the same content or high correlation
		correlation_threshold = self._config.get_config('correlation_threshold', 0.95)
		to_drop = []
		columns = list(df_copy.select_dtypes(include=['number']).columns)
		
		# Use hashing for initial fast duplicate detection
		column_hashes = {}
		for col in columns:
			if col not in columns_to_preserve:
				# Create a hash of the column values
				col_hash = hash(tuple(df_copy[col].fillna(0).values))
				if col_hash in column_hashes:
					# Found duplicate by hash
					to_drop.append(col)
				else:
					column_hashes[col_hash] = col
					
				try:
					# For remaining columns, check correlations
					remaining_cols = [c for c in columns if c not in to_drop and c not in columns_to_preserve]
					# Only compute correlations if we have enough columns
					if len(remaining_cols) > 1:
						# Compute correlation matrix once
						corr_matrix = df_copy[remaining_cols].corr().abs()
						
						# Get pairs with high correlation
						for i in range(len(remaining_cols)):
							col1 = remaining_cols[i]
							for j in range(i+1, len(remaining_cols)):
								col2 = remaining_cols[j]
								if corr_matrix.loc[col1, col2] > correlation_threshold:
									to_drop.append(col2)
				except Exception:
					# Skip columns that can't be correlated
					pass
		
		# Drop the redundant columns but preserve mapped ones
		final_to_drop = [col for col in to_drop if col not in columns_to_preserve]
		
		if final_to_drop:
			df_copy = df_copy.drop(columns=final_to_drop)
			self._log_changes(len(final_to_drop), "redundant or highly correlated columns dropped")
			
			# Log any columns that were preserved despite being redundant
			preserved_redundant = [col for col in to_drop if col in columns_to_preserve]
			if preserved_redundant:
				self._logger.info(f"Preserved {len(preserved_redundant)} redundant columns due to field mappings: {preserved_redundant}")
		
		# Summary
		columns_removed = original_columns - len(df_copy.columns)
		if columns_removed > 0:
			self._logger.info(f"Total columns removed: {columns_removed}")
		
		return df_copy


class DateTimePreprocessingStrategy(PreprocessingStrategy):
	"""Handle date and duration data preprocessing tasks."""
	
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process all date/time columns in the dataframe."""
		if df.empty:
			return df
			
		df_copy = df.copy()
		
		# Parse date columns
		df_copy = self._parse_dates(df_copy)
		
		# Parse duration strings
		df_copy = self._parse_duration_strings(df_copy)
		
		return df_copy
	
	def _extract_numbers(self, duration_string: str) -> Optional[List[str]]:
		"""Extract numbers from a string."""
		if isinstance(duration_string, str):
			# Define the regex pattern to extract numbers and their units
			pattern = r'(\d+)\s*(year|yr|years|month|months|mo|week|weeks|wk|day|days|d)'
			numbers: List[str] = re.findall(pattern, duration_string)
			return numbers
		return None
	
	def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Parse date columns safely."""
		df_copy = df.copy()
		processed_count = 0
		
		for col in df_copy.select_dtypes(include=['object']).columns:
			col_lower = col.lower()
			
			# CASE 1: Regular dates (e.g. "2023-01-15")
			if 'date' in col_lower or 'time' in col_lower:
				try:
					# Try to parse as datetime
					df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
					valid_dates = df_copy[col].notna().sum()
					
					# Keep only if majority is valid dates
					if valid_dates > 0.5 * len(df_copy):
						processed_count += 1
					else:
						# Revert to original if few valid dates
						df_copy[col] = df[col]
				except Exception as e:
					self._logger.warning(f"Error parsing dates in column {col}: {e}")
		
		self._log_changes(processed_count, "date columns processed")
		return df_copy
	
	def _parse_duration_strings(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Parse duration strings into normalized numeric values with unit indication."""
		df_copy = df.copy()
		processed_count = 0
		renamed_cols = {}  # Track renamed columns: {original_name: new_name}
		
		# Define conversion factors where each value is [months, days]
		conversion_factors = {
			'year': [12, 365],     # [months, days]
			'years': [12, 365],
			'yr': [12, 365],
			'month': [1, 30.4],    # 1 month = 30.4 days (average)
			'months': [1, 30.4],
			'mo': [1, 30.4],
			'week': [0.25, 7],     # 1 week = 0.25 months / 7 days
			'weeks': [0.25, 7],
			'wk': [0.25, 7],
			'day': [0.0329, 1],    # 1 day = 0.0329 months / 1 day
			'days': [0.0329, 1],
			'd': [0.0329, 1]
		}
		
		MONTHS_IDX = 0
		DAYS_IDX = 1
		
		for col in df_copy.select_dtypes(include=['object']).columns:
			col_lower = col.lower()
			
			# Determine if column likely contains duration data
			if any(term in col_lower for term in ['duration', 'period', 'term', 'history', 'age', 'time']):
				try:
					# Determine conversion type based on column name
					use_days_index = DAYS_IDX if 'day' in col_lower else MONTHS_IDX
					unit_type = "days" if use_days_index == DAYS_IDX else "months"
					processed_values = set()  # Track processed values to avoid redundant work
					col_processed_count = 0
					
					# Check content for hint of appropriate unit
					for val in df_copy[col].dropna().head(100).values:  # Sample first 100 values
						if isinstance(val, str) and 'day' in val.lower():
							use_days_index = DAYS_IDX
							unit_type = "days"
							break
						elif isinstance(val, str) and any(unit in val.lower() for unit in ['month', 'year']):
							use_days_index = MONTHS_IDX
							unit_type = "months"
							break
					
					# Process each unique value in the column
					for val in df_copy[col].dropna().unique():
						if val in processed_values:
							continue
						
						str_val = str(val).lower()
						# Extract numbers and units from duration string
						numbers = self._extract_numbers(str_val)
						
						if numbers and len(numbers) > 0:
							total_duration = 0
							
							# Calculate total duration using the appropriate index
							for num, unit in numbers:
								num = int(num)
								if unit in conversion_factors:
									total_duration += num * conversion_factors[unit][use_days_index]
							
							# Replace original value with total duration
							df_copy[col] = df_copy[col].replace(val, round(total_duration))
							processed_values.add(val)
							col_processed_count += 1
					
					# Add unit suffix to column name if values were processed
					if col_processed_count > 0:
						if not (col.lower().endswith('_months') or col.lower().endswith('_days')):
							new_col_name = f"{col}_{unit_type.capitalize()}"
							renamed_cols[col] = new_col_name
							processed_count += col_processed_count
				
				except Exception as e:
					self._logger.warning(f"Error processing duration in column {col}: {e}")
		
		# Rename columns with appropriate unit suffixes
		if renamed_cols:
			df_copy = df_copy.rename(columns=renamed_cols)
			self._logger.info(f"Renamed {len(renamed_cols)} columns with unit suffixes: {', '.join(renamed_cols.values())}")
		
		self._log_changes(processed_count, "duration values processed")
		return df_copy


class CategoricalPreprocessingStrategy(PreprocessingStrategy):
	"""Handle categorical data preprocessing tasks."""
	
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process all categorical columns with appropriate encoding."""
		if df.empty:
			return df
			
		df_copy = df.copy()

		# Step 1: Parse multi-value columns and explode them into separate rows
		try:
			# Identify columns containing comma-separated values
			multi_cat_cols = [col for col in df_copy.select_dtypes(include=['object']).columns 
							if df_copy[col].str.contains(',', na=False).any()]
			
			processed_count = 0
			
			# Process each multi-value column
			for col in multi_cat_cols:
				# Parse and explode the column into separate rows
				df_copy[col] = self._parse_multi_value_column(df_copy, col)
				processed_count += 1
			
			self._log_changes(processed_count, f"multi-value columns processed")

		except Exception as e:
			self._logger.error(f"Error during multi-value column processing: {e}")
			traceback.print_exc()
		

		# Step 2: Handle binary columns (yes/no, true/false) and one-hot encoding
		is_onehot_encode = self._config.get_config('onehot_encode_categorical', False)

		if not is_onehot_encode:
			# Binarize columns with only two unique values (like yes/no)
			try:
				df_copy = self._binarize_binary_columns(df_copy)
				
			except Exception as e:
				self._logger.error(f"Error during binary encoding: {e}")
				traceback.print_exc()	
		else:
			# One-hot encoding columns with few unique values
			try:
				# Identify columns for one-hot encoding
				single_cat_cols = [col for col in df_copy.select_dtypes(include=['object']).columns 
								if not df_copy[col].str.contains(',', na=False).any()]
				
				# Only encode columns with few unique values (binary or small cardinality)
				cols_to_encode = [col for col in single_cat_cols 
								if df_copy[col].nunique() <= 10]  # Limit to reasonable number of categories
				
				if cols_to_encode:
					# One-hot encode the selected columns
					df_copy = self._dummy_encode(df_copy, cols_to_encode)	
					
					self._logger.debug(f"One-hot encoding completed for columns: {', '.join(cols_to_encode)}")

			except Exception as e:
				self._logger.error(f"Error while dummy encoding: {e}")
				traceback.print_exc()

		# Step 3: Impute missing values in categorical columns
		try:
			categorical_cols = df_copy.select_dtypes(include=['object']).columns
			processed_count = 0
			
			# Find groupby key for value propagation across related records
			groupby_key = self._find_groupby_key(df_copy)
			
			for col in categorical_cols:

					# Get mode
					selected_mode = self._find_mode(df_copy, col)

					# Count missing values before imputation
					missing_before = (df_copy[col].isna() | (df_copy[col] == '')).sum()
					
					# If groupby key exists, try to fill values within groups first
					if groupby_key is not None:
						# Group by the identified key
						for group_id, group_data in df_copy.groupby(groupby_key):
							# Find rows with missing values in this group
							missing_mask = group_data[col].isna() | (group_data[col] == '')
							
							if missing_mask.any():
								# Get non-null values in this group for this column
								group_values = group_data[col].dropna().unique()
								
								if len(group_values) > 0:
									# Use the first non-null value in the group
									fill_value = group_values[0] if group_values[0] != '' else group_values[1]
									
									# Apply the fill to the main dataframe for this group
									group_indices = group_data[missing_mask].index
									df_copy.loc[group_indices, col] = fill_value
			
					# For any remaining NA values, use the mode
					missing_after_group_fill = (df_copy[col].isna() | (df_copy[col] == '')).sum()
					
					if missing_after_group_fill > 0 and selected_mode is not None:
						df_copy[col].fillna(selected_mode, inplace=True)
					
					# Count total imputed values
					total_imputed = missing_before - df_copy[col].isna().sum()
					if total_imputed > 0:
						processed_count += 1

			self._log_changes(processed_count, 'categorical columns with missing values imputed')

		except Exception as e:
			self._logger.error(f"Error during imputation of missing values: {e}")
			traceback.print_exc()

		return df_copy
	
	@lru_cache(maxsize=32)
	def _find_groupby_key_cached(self, column_names_tuple: tuple) -> Optional[str]:
		"""Cached version of groupby key finder based on column names tuple."""
		# Define default ID columns to check against
		default_id_columns = ['Customer_ID', 'CustomerID', 'ID', 'User_ID', 'UserID', 'UserName']

		# Find columns that might be ID columns (case insensitive)
		id_columns = [col for col in column_names_tuple if col.lower() in [d.lower() for d in default_id_columns]]
		
		if not id_columns:
			return None
		
		# Return first matching column
		return id_columns[0]
	
	def _find_groupby_key(self, df: pd.DataFrame) -> Optional[str]:
		"""Find suitable columns for groupby operations."""
		# Convert to tuple for caching
		column_names = tuple(sorted(df.columns.tolist()))
		return self._find_groupby_key_cached(column_names)

	def _find_mode(self, df_copy: pd.DataFrame, col: pd.Series) -> Optional[Any]:
		"""Calculate the mode of a pandas Series."""
		if 'id' not in col.lower():
			# First calculate the overall mode for fallback
			mode_value = df_copy[col].mode()
			if not mode_value.empty:
				if mode_value[0] is not None and pd.notna(mode_value[0]) and mode_value[0] != '':
					return mode_value[0]
				elif len(mode_value) > 1:
					return mode_value[1]
		return None

	def _parse_multi_value_column(self, df_copy: pd.DataFrame, col: str) -> pd.Series:
		"""Parse column with multiple values into lists of strings and explode into as many rows."""
		try:
			# Create a result series with the original values
			result = df_copy[col].copy()

			# Get the groupby key (ID column) for grouping
			groupby_key = self._find_groupby_key(df_copy)
			if groupby_key is not None:
				# Use the found key for groupby
				grouped_data = df_copy.groupby(groupby_key)
			else:
				return df_copy[col]  # No grouping key found, return original column


			self._logger.debug(f"Processing multi-value column {col} with groupby key {groupby_key}")

			# Identify common words across all values in this column
			common_words = self._identify_common_words(df_copy[col])

			# Group by id
			for customer_id, customer_group in grouped_data:
				if len(customer_group) <= 1:
					continue  # Skip if only one row for this customer
					
				# Get loan types from the first non-null value
				loan_types_str = customer_group[col].dropna().iloc[0] if not customer_group[col].dropna().empty else ''
				
				# Initialize loan types list
				loan_types = []
				
				# Parse loan types from string if available
				if loan_types_str:
					# Normalize format and parse loan types
					loan_types_str = loan_types_str.replace(" and ", ", ")
					loan_types = [lt.strip() for lt in loan_types_str.split(',') if lt.strip()]

					# Remove common words from each loan type
					loan_types = [self._remove_common_words(lt, common_words) for lt in loan_types]
					
					# Filter out "Not Specified" - we'll use this as default
					loan_types = [lt for lt in loan_types if lt.lower() != "not specified"]
				
				# Check for loan-specific balances
				balance_cols = [c for c in df_copy.columns if 'LoanBalance' in c or 'Balance' in c]
				required_loans = {}
				loan_types_from_balance = set()
				
				# First, find loans with positive balances
				for idx in customer_group.index:
					for balance_col in balance_cols:
						# Extract loan type from balance column name
						if 'AutoLoanBalance' in balance_col:
							loan_type_name = "Auto"
						elif 'StudentLoanBalance' in balance_col:
							loan_type_name = "Student"
						elif 'PersonalLoanBalance' in balance_col:
							loan_type_name = "Personal"
						elif 'MortgageBalance' in balance_col:
							loan_type_name = "Mortgage"
						else:
							continue
						
						# Check if this row has positive balance for this loan type
						if df_copy.loc[idx, balance_col] > 0:
							required_loans[idx] = loan_type_name
							loan_types_from_balance.add(loan_type_name)
				
				# If we didn't get any loan types from the column but have some from balances, use those
				if not loan_types and loan_types_from_balance:
					loan_types = list(loan_types_from_balance)
				
				# If we have fewer loan types than rows, pad with "Not Specified"
				while len(loan_types) < len(customer_group):
					loan_types.append("Not Specified")
					
				# Assign loan types to indices
				remaining_indices = list(customer_group.index)
				
				# First, assign required loans from balance
				for idx, loan_type in required_loans.items():
					if idx in remaining_indices and loan_type in loan_types:
						result.loc[idx] = loan_type
						remaining_indices.remove(idx)
						loan_types.remove(loan_type)
				
				# Then distribute remaining loan types
				for i, idx in enumerate(remaining_indices):
					if i < len(loan_types):
						result.loc[idx] = loan_types[i]
					else:
						result.loc[idx] = "Not Specified"
						
			return result
			
		except Exception as e:
			self._logger.error(f"Error in _parse_multi_value_column: {e}")
			traceback.print_exc()
			return df_copy[col]  # Return original on error

	@staticmethod
	@lru_cache(maxsize=128)
	def _identify_common_words_cached(text_tuple: tuple) -> list:
		"""Identify common words in a collection of text values."""
		from collections import Counter
		
		# Convert tuple back to list
		texts = list(text_tuple)
		
		# Extract words from all texts
		all_words = []
		for text in texts:
			if not isinstance(text, str):
				continue
			# Split by common separators and convert to lowercase
			words = str(text).lower().replace('-', ' ').replace('_', ' ').replace('/', ' ').split()
			all_words.extend(words)
			
		# Count word occurrences
		word_counts = Counter(all_words)
		
		# Filter out common words based on frequency and length
		if word_counts is not None:
			# Get word frequencies as a list
			frequencies = list(word_counts.values())
			
			# Sort frequencies
			frequencies.sort()
			
			# Use median instead of mean for better outlier resistance
			if len(frequencies) % 2 == 0:
				median = (frequencies[len(frequencies)//2] + frequencies[len(frequencies)//2 - 1]) / 2
			else:
				median = frequencies[len(frequencies)//2]
			
			# Calculate 95th percentile
			upper_quartile_idx = int(len(frequencies) * 0.95)
			upper_quartile = frequencies[upper_quartile_idx]
			
			# Use a blend of median and upper quartile as threshold
			threshold = int(median + (upper_quartile - median) * 0.5)
			
			# Ensure minimum threshold of 2 (word must appear at least twice)
			threshold = max(2, threshold)

			# Get the top words based on the threshold, excluding common stop words (e.g., 'the', 'and')
			top_words = [word for word, count in word_counts.items() 
						if count >= threshold and len(word) > 3]

			return top_words
		else:
			# If no words found, return empty list
			return []
				
	def _identify_common_words(self, texts: List[str]) -> List[str]:
		"""Identify common words in a collection of text values."""
		# Convert to hashable type for caching
		text_tuple = tuple(str(t) if pd.notna(t) else '' for t in texts[:1000])  # Limit to 1000 for performance
		return self._identify_common_words_cached(text_tuple)

	def _remove_common_words(self, text: str, common_words: List[str]) -> str:
		"""Remove common words from text."""
		if not common_words:
			return text
			
		text_parts = text.split()
		result = [word for word in text_parts if word.lower() not in common_words]
		
		# If removing common words would make the text empty, return the original text
		if not result:
			return text
		
		return ' '.join(result)

	def _binarize_binary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
		"""
		Convert categorical columns with exactly two unique non-null values to binary (0/1).
		Maps 'yes', 'true', etc. to 1 and 'no', 'false', etc. to 0.
		All null values and outliers are mapped to 0.
		"""
		df_copy = df.copy()
		processed_count = 0
		
		# Define common positive value patterns
		default_positive_values = [
			'yes', 'y', 'true', 't', '1', 'positive', 'pos', 'p', 'success', 
			'pass', 'approved', 'high', 'active', 'available', 'present',
			'completed', 'achieved', 'confirmed', 'valid', 'succeeded', 'done', 
			'in_use', 'in_service', 'on_time', 'on_schedule', 'available', 'in_stock', 
			'in_progress', 'in_transit'
		]

		# Get engine parameters for custom positive values
		positive_values = self._config.get_config('binary_patterns', default_positive_values)
		positive_values.extend(default_positive_values)  # Ensure default values are included

		# Get only object/string columns
		cat_cols = df_copy.select_dtypes(include=['object']).columns
		
		processed_count = 0

		columns_to_process = []
		for col in cat_cols:
			# Get unique non-null, non-empty values
			unique_values = [v for v in df_copy[col].dropna().unique() if str(v).strip() != '']
			
			# Check criteria
			if len(unique_values) > 1 and len(unique_values) <= 3 and 'name' not in col.lower():
				columns_to_process.append((col, unique_values))

		for col, unique_values in columns_to_process:
			try:
				# Convert values to lowercase for comparison
				values_lower = [str(v).lower() for v in unique_values]
				
				# Determine all positive values (all that match our patterns)
				positive_vals = set()
				for val, lower_val in zip(unique_values, values_lower):
					if lower_val in positive_values:
						positive_vals.add(val)
				
				# If no positive value is found, the column is not binary
				if not positive_vals:
					self._logger.info(f"No positive value found for column {col}, treating as non-binary")
					continue
				
				# Create mapping dictionary - map all positive values to 1
				mapping = {val: 1 if val in positive_vals else 0 for val in unique_values}
				
				# Apply mapping with default=0 for nulls and any unexpected values
				df_copy[col] = df_copy[col].map(lambda x: mapping.get(x, 0))
				self._logger.debug(f"Processing binary column {col} with mapping: {mapping}")
				processed_count += 1
				
			except Exception as e:
				self._logger.warning(f"Error binarizing column {col}: {e}")
		
		if processed_count > 0:
			self._log_changes(processed_count, "binary categorical columns encoded")
		
		return df_copy

	def _dummy_encode(self, df: pd.DataFrame, cols_to_encode: List[str]) -> pd.DataFrame:
		"""Dummy encode categorical columns."""
		try:
			# Use pd.get_dummies for one-hot encoding
			dummies = pd.get_dummies(df[cols_to_encode], dummy_na=False, drop_first=False, prefix_sep='_')
			
			# Drop original columns and add dummies
			df = df.drop(columns=cols_to_encode)
			df = pd.concat([df, dummies], axis=1)

			# Log the number of columns added
			encoded_count = len(cols_to_encode)
			self._log_changes(encoded_count, "categorical columns one-hot encoded")

			return df
			
		except Exception as e:
			self._logger.error(f"Error during dummy encoding: {e}")
			traceback.print_exc()
			return df


class DataConcatenationStrategy(PreprocessingStrategy):
	"""Concatenate multiple datasets into one."""
	
	def __init__(self, config_provider: IConfigProvider):
		super().__init__(config_provider)
		self._dataset_name = None
		self._datasets = []
		
	def set_dataset_info(self, dataset_name: str, csv_files: List[str]) -> None:
		"""Set the dataset information."""
		self._dataset_name = dataset_name
		self._datasets = csv_files
	
	def process(self, df: pd.DataFrame = None) -> pd.DataFrame:
		"""Concatenate datasets. This strategy doesn't use the input dataframe."""
		if not self._datasets:
			self._logger.error("No datasets specified for concatenation")
			return pd.DataFrame()
			
		try:
			nsamples = self._config.get_config('nsamples', 1000)
			input_dir = Path(self._config.get_config('input_dir'))
			
			if len(self._datasets) > 1:
				self._logger.info(f"Concatenating {len(self._datasets)} datasets for {self._dataset_name}")
				dataframes = []
				
				for dataset_file in self._datasets:
					try:
						file_path = input_dir / dataset_file
						df = pd.read_csv(file_path)
						if nsamples and nsamples < len(df):
							df = df[:nsamples]
						dataframes.append(df)
						self._logger.info(f"Loaded {len(df)} rows from {dataset_file}")
					except Exception as e:
						self._logger.error(f'Error loading {dataset_file}: {e}')
				
				# Concatenate horizontally (by columns)
				if dataframes:
					result = pd.concat(dataframes, axis=1)
					self._logger.info(f"Concatenated {len(dataframes)} datasets with {len(result)} rows and {len(result.columns)} columns")
					return result
				else:
					self._logger.error("No datasets successfully loaded for concatenation")
					return pd.DataFrame()
			else:
				# Just load the single file
				try:
					file_path = input_dir / self._datasets[0]
					df = pd.read_csv(file_path)
					if nsamples and nsamples < len(df):
						df = df[:nsamples]
					self._logger.info(f"Loaded single dataset {self._datasets[0]} with {len(df)} rows")
					return df
				except Exception as e:
					self._logger.error(f'Error loading {self._datasets[0]}: {e}')
					return pd.DataFrame()
					
		except Exception as e:
			self._logger.error(f'Error in dataset concatenation: {e}')
			return pd.DataFrame()


class CleanupStrategy(PreprocessingStrategy):
	"""Perform cleanup operations."""

	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Apply cleanup operations to the dataframe."""
		if df.empty:
			return df
			
		df_copy = df.copy()
		changes = 0
		
		try:
			# Drop rows with too many missing values
			missing_threshold = self._config.get_config('max_missing_pct', 0.5)
			rows_before = len(df_copy)
			df_copy = df_copy.dropna(thresh=int(len(df_copy.columns) * (1-missing_threshold)))
			rows_dropped = rows_before - len(df_copy)
			if rows_dropped > 0:
				changes += rows_dropped
				self._log_changes(rows_dropped, "rows with excessive missing values dropped")
			
			# Drop duplicates
			rows_before = len(df_copy)
			df_copy = df_copy.drop_duplicates()
			rows_deduped = rows_before - len(df_copy)
			if rows_deduped > 0:
				changes += rows_deduped
				self._log_changes(rows_deduped, "duplicate rows dropped")
			
			# Drop duplicate columns
			cols_before = len(df_copy.columns)
			df_copy = df_copy.loc[:, ~df_copy.columns.duplicated()]
			cols_deduped = cols_before - len(df_copy.columns)
			if cols_deduped > 0:
				changes += cols_deduped
				self._log_changes(cols_deduped, "duplicate columns dropped")
			
			# Drop columns with too many missing values
			cols_before = len(df_copy.columns)
			df_copy = df_copy.loc[:, df_copy.isnull().mean() < missing_threshold]
			cols_dropped = cols_before - len(df_copy.columns)
			if cols_dropped > 0:
				changes += cols_dropped
				self._log_changes(cols_dropped, "columns with excessive missing values dropped")
			
			# Reset index
			df_copy = df_copy.reset_index(drop=True)

			self._logger.info(f"Cleanup completed with {changes} changes")
			return df_copy
		except Exception as e:
			self._logger.error(f"Error during cleanup: {e}")
			traceback.print_exc()
			return df  # Return original dataframe on error
		
		

#======= 2. Strategy Factory =======
class PreprocessingStrategyFactory:
	"""Factory for creating preprocessing strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategies = {}
		self._initialize_strategies()
	
	def _initialize_strategies(self) -> None:
		"""Initialize the registry with default strategies."""
		self.register('text_cleaning', TextCleaningStrategy(self._config))
		self.register('numeric_processing', NumericPreprocessingStrategy(self._config))
		self.register('datetime_processing', DateTimePreprocessingStrategy(self._config))
		self.register('categorical_processing', CategoricalPreprocessingStrategy(self._config))
		self.register('data_concatenation', DataConcatenationStrategy(self._config))
		self.register('cleanup', CleanupStrategy(self._config))
	
	def register(self, name: str, strategy: PreprocessingStrategy) -> None:
		"""Register a preprocessing strategy."""
		if name in self._strategies:
			raise ValueError(f"Strategy {name} already registered")
		self._strategies[name] = strategy
	
	def get_strategy(self, name: str) -> PreprocessingStrategy:
		"""Get a preprocessing strategy by name."""
		if name not in self._strategies:
			raise ValueError(f"Unknown preprocessing strategy: {name}")
		return self._strategies[name]



################################################
#======= 3. Preprocessing Pipeline Steps =======
class PreprocessingPipelineStep(IPipelineStep):
	"""Base class for preprocessing pipeline steps."""
	
	def __init__(self, strategy: PreprocessingStrategy):
		self._strategy = strategy
	
	@property
	def name(self) -> str:
		"""Get the name of this pipeline step."""
		return self._strategy.name
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process the data using the strategy."""
		return self._strategy.process(df)


class DataConcatenationStep(IPipelineStep):
	"""Step for concatenating multiple datasets."""
	
	def __init__(self, strategy: DataConcatenationStrategy, dataset_name: str, csv_files: List[str]):
		self._strategy = strategy
		self._strategy.set_dataset_info(dataset_name, csv_files)
	
	@property
	def name(self) -> str:
		"""Get the name of this pipeline step."""
		return "DataConcatenation"
	
	def execute(self, df: pd.DataFrame = None) -> pd.DataFrame:
		"""Process the data concatenation."""
		return self._strategy.process(df)


#======= 4. Preprocessing Pipeline =======
class PreprocessingPipeline:
	"""Pipeline for applying multiple preprocessing steps."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._steps: List[IPipelineStep] = []
	
	def add_step(self, step: IPipelineStep) -> PreprocessingPipeline:
		"""Add a preprocessing step to the pipeline."""
		self._steps.append(step)
		return self
	
	def process(self, df: pd.DataFrame = None) -> pd.DataFrame:
		"""Process data through the preprocessing pipeline."""
		result = df
		
		# Add steps counting logic
		steps_count = len(self._steps)
		for step_idx, step in enumerate(self._steps):
			step_start_time = datetime.now()
			try:
				self._logger.info(f"Applying preprocessing step {step_idx+1}/{steps_count}: {step.name}")
				result = step.execute(result)
				
				# Track execution time
				step_duration_ms = (datetime.now() - step_start_time).total_seconds() * 1000
				self._logger.debug(f"Step {step.name} completed in {step_duration_ms:.2f} ms")
				
				if result is None or result.empty:
					self._logger.error(f"Step {step.name} returned empty result")
					return pd.DataFrame()
					
			except Exception as e:
				self._logger.error(f"Error in preprocessing step {step.name}: {e}")
				traceback.print_exc()
				
		return result



###########################################
#======= 5. Preprocessor module API =======
class Preprocessor:
	"""Main class for preprocessing datasets."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategy_factory = PreprocessingStrategyFactory(config_provider)
		self._dataset_name = None
		self._data = None
		self._csv_files = None
		self._results = None
	
	def load_dataset(self, dataset_name: str, df: pd.DataFrame = None, csv_files: List[str] = None) -> Preprocessor:
		"""Load the dataset for preprocessing."""
		self._dataset_name = dataset_name
		self._config.set_config('current_dataset', dataset_name)
		self._data = df
		self._csv_files = csv_files or []
		
		if df is not None:
			self._logger.info(f"Loaded dataset: {dataset_name} with {len(df)} rows and {len(df.columns)} columns")
		elif csv_files:
			self._logger.info(f"Loaded dataset info: {dataset_name} with {len(csv_files)} files")
		
		return self
	
	def process(self) -> pd.DataFrame:
		"""Preprocess the dataset by applying all preprocessing steps."""
		if not self._dataset_name:
			self._logger.error("Dataset not loaded")
			return pd.DataFrame()
			
		# Create the preprocessing pipeline
		pipeline = self.create_pipeline()
		
		start_time = datetime.now()
		self._logger.info(f"Starting preprocessing for {self._dataset_name}")
		
		# Process the dataset
		if self._data is not None:
			# If dataframe is provided, use it
			self._data = pipeline.process(self._data)
		elif self._csv_files:
			# If only file paths provided, process from files
			self._data = pipeline.process()
		else:
			self._logger.error("No data or files to process")
			return pd.DataFrame()
			
		execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
		
		# Store results
		self._results = {
			"dataset_name": self._dataset_name,
			"row_count": len(self._data) if self._data is not None else 0,
			"column_count": len(self._data.columns) if self._data is not None else 0,
			"execution_time_ms": execution_time_ms
		}
		
		self._logger.info(f"Preprocessing completed in {execution_time_ms:.2f} ms")
		return self._data
	
	def get_results(self) -> Dict[str, Any]:
		"""Get the preprocessing results and metadata."""
		return self._results
	
	def save_results(self, output_path: Optional[Path] = None) -> bool:
		"""Save processed data to disk."""
		if self._data is None or self._data.empty:
			self._logger.error("No processed data to save")
			return False
			
		try:
			output_dir = Path(self._config.get_config('output_dir'))
			
			# Use provided path or default
			if output_path is None:
				output_path = output_dir / f"{self._dataset_name}_processed.csv"
			
			# Ensure output directory exists
			output_path.parent.mkdir(parents=True, exist_ok=True)
			
			# Save the data
			self._data.to_csv(output_path, index=False)
			self._logger.info(f"Saved processed data to {output_path}")
			
			return True
			
		except Exception as e:
			self._logger.error(f"Error saving processed data: {e}")
			traceback.print_exc()
			return False
	
	def create_pipeline(self) -> PreprocessingPipeline:
		"""Create a preprocessing pipeline for the dataset."""
		pipeline = PreprocessingPipeline(self._config)
		
		# Add data concatenation step if files are provided
		if self._csv_files:
			concat_strategy = self._strategy_factory.get_strategy('data_concatenation')
			if isinstance(concat_strategy, DataConcatenationStrategy):
				concat_strategy.set_dataset_info(self._dataset_name, self._csv_files)
				
			pipeline.add_step(PreprocessingPipelineStep(concat_strategy))
		
		# Standard preprocessing steps
		pipeline.add_step(PreprocessingPipelineStep(
			self._strategy_factory.get_strategy('text_cleaning')
		))
		
		pipeline.add_step(PreprocessingPipelineStep(
			self._strategy_factory.get_strategy('datetime_processing')
		))
		
		pipeline.add_step(PreprocessingPipelineStep(
			self._strategy_factory.get_strategy('numeric_processing')
		))
		
		pipeline.add_step(PreprocessingPipelineStep(
			self._strategy_factory.get_strategy('categorical_processing')
		))
		
		pipeline.add_step(PreprocessingPipelineStep(
			self._strategy_factory.get_strategy('cleanup')
		))
		
		return pipeline
	