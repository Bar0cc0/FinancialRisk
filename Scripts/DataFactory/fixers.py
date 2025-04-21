#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Financial Data Quality Fixer Framework
======================================

This module provides an extensible framework for fixing data quality issues in financial datasets.
It implements a strategy pattern and pipeline architecture for applying domain-specific and
general-purpose fixes to address data quality issues identified by validators.

The module supports fixing a wide range of issues including missing values, outliers,
domain constraint violations, logical inconsistencies, and cross-field dependencies.

Key Components
--------------
- Fixer Strategies: Implementations for specific data fixing tasks
	- Missing value imputation
	- Outlier detection and fixing
	- Domain-specific validators (Loan, Market, Fraud, Macro)
	- Data consistency checkers
- Strategy Factory: Creates and manages fixer strategy instances
- Pipeline Steps: Wraps strategies for execution in a pipeline context
- Fixer Pipeline: Orchestrates sequential execution of fixer steps
- DataFixer API: Main entry point for client code to access fixing functionality

Domain-Specific Capabilities
----------------------------
- Loan Data: Age range validation, credit score normalization, interest rate fixes
- Market Data: OHLC consistency fixes, price relationship validation
- Fraud Data: Transaction indicator consistency, distance validation
- Macro Data: Economic indicator range fixing, ratio validation

Usage Example
-------------
from configuration import Configuration
from fixers import DataFixer
import pandas as pd

# Initialize configuration
config = Configuration()
config.load_from_file("config.yaml")

# Load dataset with quality issues
df = pd.read_csv("financial_data_with_issues.csv")

# Fix issues using the module API
fixer = DataFixer(config)
fixed_df = fixer.load_dataset("Loan", df).process()

# Save fixed data
fixer.save_results("fixed_financial_data.csv")
"""

#=================================================

from __future__ import annotations
import traceback
import pandas as pd
import numpy as np
from functools import lru_cache
from pathlib import Path
from abc import abstractmethod
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timedelta

from interfaces import IConfigProvider, IPipelineStep, IDataFixerStrategy


#======== 1. Fixer Strategies ==========
class FixerStrategy(IDataFixerStrategy):
	"""Base class for data fixer strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._fix_count = 0

	@property
	def name(self) -> str:
		"""Return the name of the fixer strategy."""
		return self.__class__.__name__
	
	def fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix issues in the dataframe according to the strategy."""
		self._fix_count = 0
		
		try:
			if df.empty:
				self._logger.warning(f"{self.name}: Empty dataframe, nothing to fix")
				return df
				
			result_df = self._fix(df.copy())
			
			if self._fix_count > 0:
				self._logger.info(f"{self.name}: Applied {self._fix_count} fixes")
			
			return result_df
		except Exception as e:
			self._logger.error(f"Error in fix strategy {self.name}: {e}")
			traceback.print_exc()
			return df  # Return original dataframe on error
	
	@abstractmethod
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Implementation-specific fixing logic."""
		pass
	
	def _log_fixes(self, fix_count: int, message: str) -> None:
		"""Log fixes if there were any."""
		if fix_count > 0:
			self._fix_count += fix_count
			self._logger.debug(f"{self.name}: {fix_count} {message}")


class MissingValueFixerStrategy(FixerStrategy):
	"""Fix missing values in the dataset."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix missing values using appropriate strategies for each column type."""
		# Find columns with missing values
		missing_cols = df.columns[df.isnull().any()].tolist()
		
		if not missing_cols:
			return df
			
		# Get dataset type for domain-specific fixes
		dataset_name = self._config.get_config("current_dataset", "")
		
		for col in missing_cols:
			col_missing = df[col].isnull().sum()
			
			# Skip columns with too many missing values - these should be dropped instead
			missing_pct = col_missing / len(df)
			if missing_pct > self._config.get_config("max_missing_pct", 0.5):
				continue
				
			# Choose appropriate imputation based on column type
			if pd.api.types.is_numeric_dtype(df[col]):
				self._fix_numeric_missing(df, col)
			elif pd.api.types.is_datetime64_any_dtype(df[col]):
				self._fix_datetime_missing(df, col)
			elif pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
				self._fix_categorical_missing(df, col, dataset_name)
		
		return df
	
	def _fix_numeric_missing(self, df: pd.DataFrame, col: str) -> None:
		"""Fix missing values in numeric columns."""
		missing_count = df[col].isnull().sum()
		if missing_count == 0:
			return
			
		# Try KNN imputation for numeric columns first if there's enough data
		if len(df) > 10 and missing_count / len(df) < 0.3:
			try:
				# Get numeric columns for KNN imputation
				numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
				
				if len(numeric_cols) >= 3:  # Need at least a few predictors
					from sklearn.impute import KNNImputer
					
					# Create a subset for imputation with just numeric columns
					subset = df[numeric_cols].copy()
					
					# Create and fit imputer
					imputer = KNNImputer(n_neighbors=min(5, len(df) - 1))
					imputed = imputer.fit_transform(subset)
					
					# Update the original column with imputed values
					df.loc[:, col] = imputed[:, numeric_cols.index(col)]
					
					self._log_fixes(missing_count, f"missing values in '{col}' fixed with KNN imputation")
					return
			except Exception as e:
				self._logger.warning(f"KNN imputation failed for '{col}': {str(e)}")
		
		# Fall back to median for numeric columns
		median_value = df[col].median()
		df[col].fillna(median_value, inplace=True)
		self._log_fixes(missing_count, f"missing values in '{col}' fixed with median imputation")
	
	def _fix_datetime_missing(self, df: pd.DataFrame, col: str) -> None:
		"""Fix missing values in datetime columns."""
		missing_count = df[col].isnull().sum()
		if missing_count == 0:
			return
			
		# Use the median date
		median_date = df[col].median()
		df[col].fillna(median_date, inplace=True)
		
		self._log_fixes(missing_count, f"missing values in '{col}' fixed with median date")
	
	def _fix_categorical_missing(self, df: pd.DataFrame, col: str, dataset_name: str) -> None:
		"""Fix missing values in categorical/string columns."""
		missing_count = df[col].isnull().sum()
		if missing_count == 0:
			return
			
		# Use mode (most frequent value) for categorical columns
		mode_value = df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown"
		
		# Use domain-specific values for certain columns
		if 'Type' in col or 'Status' in col:
			df[col].fillna("Unknown", inplace=True)
		elif 'Name' in col:
			df[col].fillna("Not Specified", inplace=True)
		else:
			df[col].fillna(mode_value, inplace=True)
			
		self._log_fixes(missing_count, f"missing values in '{col}' fixed with categorical imputation")


class OutlierFixerStrategy(FixerStrategy):
	"""Fix statistical outliers in numeric columns."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix outliers in numeric columns using first group-based approach, then statistical methods."""
		if df.empty:
			return df
			
		# Only process numeric columns
		numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
		outlier_threshold = self._config.get_config('outlier_threshold', 3.0)
		
		# Get dataset type for domain-specific handling
		dataset_name = self._config.get_config('current_dataset', '')
		
		# Try to find a groupby key for more accurate outlier detection within groups
		groupby_key = self._find_groupby_key(df)
		has_group_fixes = False
		
		for col in numeric_cols:
			# Skip columns that shouldn't be processed
			if self._should_skip_column(col, df, dataset_name):
				continue
			
			# Step 1: First attempt to fix outliers within groups if a group key exists
			if groupby_key is not None:
				group_fix_count = self._fix_group_outliers(df, col, groupby_key, outlier_threshold)
				if group_fix_count > 0:
					has_group_fixes = True
			
			# Step 2: Apply global outlier detection for remaining outliers
			self._fix_global_outliers(df, col, outlier_threshold)
				
		if has_group_fixes:
			self._logger.info(f"Applied group-based outlier detection using '{groupby_key}' as the group key")
			
		return df
	
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

	def _should_skip_column(self, col: str, df: pd.DataFrame, dataset_name: str) -> bool:
		"""Determine if column should be skipped for outlier detection."""
		# Skip ID columns, boolean columns, and columns with high missing values
		if (('id' in col.lower() and 'ratio' not in col.lower()) or 
			df[col].nunique() <= 2 or 
			df[col].isnull().mean() > 0.5):
			return True
			
		# Skip columns with domain-specific handling
		domain_specific_columns = {
			'Loan': ['Age', 'AnnualIncome', 'CreditScore', 'InterestRate', 'LoanAmount', 'DebtToIncomeRatio'],
			'Fraud': ['TransactionAmount', 'DistanceFromHome'],
			'Market': ['OpenValue', 'CloseValue', 'HighestValue', 'LowestValue', 'VIX', 'TEDSpread'],
			'Macro': ['UnemploymentRate', 'GDP', 'InflationRate']
		}
		
		if dataset_name in domain_specific_columns:
			return col in domain_specific_columns[dataset_name]
			
		return False
		
	@staticmethod
	@lru_cache(maxsize=128)
	def _calculate_bounds_cached(q1: float, q3: float, threshold: float) -> tuple:
		"""Calculate IQR-based bounds for outlier detection."""
		iqr = q3 - q1
		
		# If no variability, return None
		if iqr == 0:
			return None, None
			
		lower_bound = q1 - threshold * iqr
		upper_bound = q3 + threshold * iqr
		
		return lower_bound, upper_bound
		
	def _calculate_bounds(self, series: pd.Series, threshold: float) -> tuple:
		"""Calculate IQR-based bounds for outlier detection."""
		q1 = series.quantile(0.25)
		q3 = series.quantile(0.75)
		
		return self._calculate_bounds_cached(q1, q3, threshold)

	def _fix_outliers_with_bounds(self, df: pd.DataFrame, col: str, 
								lower_bound: float, upper_bound: float,
								condition: pd.Series = None) -> int:
		"""Fix outliers in a column based on bounds and an optional condition."""
		fix_count = 0
		
		# Create masks with condition if provided
		if condition is not None:
			lower_mask = condition & (df[col] < lower_bound)
			upper_mask = condition & (df[col] > upper_bound)
		else:
			lower_mask = df[col] < lower_bound
			upper_mask = df[col] > upper_bound
		
		# Fix lower outliers
		if lower_mask.any():
			df.loc[lower_mask, col] = lower_bound
			fix_count += lower_mask.sum()
		
		# Fix upper outliers
		if upper_mask.any():
			df.loc[upper_mask, col] = upper_bound
			fix_count += upper_mask.sum()
			
		return fix_count

	def _fix_group_outliers(self, df: pd.DataFrame, col: str, 
						groupby_key: str, threshold: float) -> int:
		"""Fix outliers within groups using the majority value approach."""		
		total_fix_count = 0
		
		for group_id, group_df in df.groupby(groupby_key):
			# Get group size
			n = len(group_df)
			
			# Skip very small groups (need at least 3 values to meaningfully apply n-2 rule)
			if n < 3:
				continue
			
			# Get value counts for this column in this group
			value_counts = group_df[col].value_counts()
			
			# If the most common value appears at least n-2 times, use it as the expected value
			if len(value_counts) > 0 and value_counts.iloc[0] >= n - 2:
				expected_value = value_counts.index[0]
				
				# Create condition for this group and different values
				group_condition = (df[groupby_key] == group_id) & (df[col] != expected_value)
				
				# Count rows that need fixing
				fix_count = group_condition.sum()
				
				if fix_count > 0:
					# Replace outliers with the expected value
					df.loc[group_condition, col] = expected_value
					total_fix_count += fix_count
					
					# Log specific fixes for debugging
					self._logger.debug(f"Group {group_id}: Replaced {fix_count} values in column '{col}' with consensus value {expected_value}")
		
		if total_fix_count > 0:
			self._log_fixes(total_fix_count, f"outliers fixed in '{col}' using group majority value approach")
		
		return total_fix_count

	def _fix_global_outliers(self, df: pd.DataFrame, col: str, threshold: float) -> int:
		"""Fix outliers globally."""
		# Calculate global bounds
		bounds = self._calculate_bounds(df[col], threshold)
		if bounds[0] is None:  # No variability
			return 0
			
		lower_bound, upper_bound = bounds
		
		# Fix outliers globally (no condition)
		fix_count = self._fix_outliers_with_bounds(df, col, lower_bound, upper_bound)
		
		if fix_count > 0:
			self._log_fixes(fix_count, f"outliers fixed in '{col}' using statistical approach")
			
		return fix_count


class DomainLoanFixerStrategy(FixerStrategy):
	"""Fix domain-specific issues in loan data."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix domain-specific issues in loan data."""
		if df.empty:
			return df
			
		# Fix age values
		if 'Age' in df.columns:
			self._fix_age_values(df)
			
		# Fix loan amount values
		if 'LoanAmount' in df.columns:
			self._fix_loan_amount(df)
			
		# Fix interest rate values
		if 'InterestRate' in df.columns:
			self._fix_interest_rate(df)
			
		# Fix credit score values
		if 'CreditScore' in df.columns:
			self._fix_credit_score(df)
			
		# Fix annual income values
		if 'AnnualIncome' in df.columns:
			self._fix_annual_income(df)
			
		# Fix num loans values
		if 'NumLoans' in df.columns:
			self._fix_num_loans(df)

		# Fix all count columns (any column starting with Num_)
		self._fix_count_columns(df)
			
		return df
	
	def _fix_count_columns(self, df: pd.DataFrame) -> None:
		"""Fix count columns to ensure they are non-negative integers."""
		# Find all columns starting with 'Num_' or containing 'Count' or 'Number'
		count_cols = [col for col in df.columns if col.startswith('Num_') or 'Count' in col or 'Number' in col]
		
		for col in count_cols:
			if col not in df.columns:
				continue
				
			# Fix negative values
			negative_mask = df[col] < 0
			if negative_mask.sum() > 0:
				df.loc[negative_mask, col] = 0
				self._log_fixes(negative_mask.sum(), f"negative {col} values fixed")
			
			# Fix non-integer values
			non_integer_mask = df[col].apply(lambda x: isinstance(x, float) and x % 1 != 0)
			if non_integer_mask.sum() > 0:
				df.loc[non_integer_mask, col] = df.loc[non_integer_mask, col].round()
				self._log_fixes(non_integer_mask.sum(), f"non-integer {col} values fixed")	

	def _fix_age_values(self, df: pd.DataFrame) -> None:
		"""Fix invalid age values."""
		# Fix negative ages
		negative_mask = df['Age'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'Age'] = df['Age'].median()
			self._log_fixes(negative_mask.sum(), "negative ages fixed")
			
		# Fix unrealistically high ages
		high_mask = df['Age'] > 100
		if high_mask.sum() > 0:
			df.loc[high_mask, 'Age'] = 100
			self._log_fixes(high_mask.sum(), "unrealistically high ages fixed")
			
		# Fix unrealistically low ages for loans (typically 18+)
		low_mask = (df['Age'] > 0) & (df['Age'] < 18)
		if low_mask.sum() > 0:
			df.loc[low_mask, 'Age'] = 18
			self._log_fixes(low_mask.sum(), "unrealistically low ages fixed")
	
	def _fix_loan_amount(self, df: pd.DataFrame) -> None:
		"""Fix invalid loan amounts."""
		# Fix negative loan amounts
		negative_mask = df['LoanAmount'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'LoanAmount'] = df['LoanAmount'].abs()
			self._log_fixes(negative_mask.sum(), "negative loan amounts fixed")
			
		# Fix unrealistically high loan amounts (assuming upper limit of $10M)
		high_mask = df['LoanAmount'] > 10000000
		if high_mask.sum() > 0:
			df.loc[high_mask, 'LoanAmount'] = 10000000
			self._log_fixes(high_mask.sum(), "unrealistically high loan amounts fixed")
	
	def _fix_interest_rate(self, df: pd.DataFrame) -> None:
		"""Fix invalid interest rates."""
		# Fix negative interest rates
		negative_mask = df['InterestRate'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'InterestRate'] = df['InterestRate'].abs()
			self._log_fixes(negative_mask.sum(), "negative interest rates fixed")
			
		# Fix unrealistically high interest rates (cap at 100%)
		high_mask = df['InterestRate'] > 100
		if high_mask.sum() > 0:
			df.loc[high_mask, 'InterestRate'] = 100
			self._log_fixes(high_mask.sum(), "unrealistically high interest rates fixed")
	
	def _fix_credit_score(self, df: pd.DataFrame) -> None:
		"""Fix invalid credit scores."""
		# Different credit score scales
		# FICO: 300-850
		# VantageScore: 501-990
		# Use most common FICO as default (300-850)
		
		# Fix negative credit scores
		negative_mask = df['CreditScore'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'CreditScore'] = 300
			self._log_fixes(negative_mask.sum(), "negative credit scores fixed")
			
		# Fix unrealistically low credit scores
		low_mask = (df['CreditScore'] > 0) & (df['CreditScore'] < 300)
		if low_mask.sum() > 0:
			df.loc[low_mask, 'CreditScore'] = 300
			self._log_fixes(low_mask.sum(), "unrealistically low credit scores fixed")
			
		# Fix unrealistically high credit scores
		high_mask = df['CreditScore'] > 850
		if high_mask.sum() > 0:
			df.loc[high_mask, 'CreditScore'] = 850
			self._log_fixes(high_mask.sum(), "unrealistically high credit scores fixed")
	
	def _fix_annual_income(self, df: pd.DataFrame) -> None:
		"""Fix invalid annual income values."""
		# Fix negative income
		negative_mask = df['AnnualIncome'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'AnnualIncome'] = df['AnnualIncome'].abs()
			self._log_fixes(negative_mask.sum(), "negative annual income values fixed")
			
		# Fix unrealistically high income (cap at $100M)
		high_mask = df['AnnualIncome'] > 100000000
		if high_mask.sum() > 0:
			df.loc[high_mask, 'AnnualIncome'] = 100000000
			self._log_fixes(high_mask.sum(), "unrealistically high annual income values fixed")
	
	def _fix_num_loans(self, df: pd.DataFrame) -> None:
		"""Fix invalid number of loans."""
		# Fix negative loan counts
		negative_mask = df['NumLoans'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'NumLoans'] = 0
			self._log_fixes(negative_mask.sum(), "negative loan counts fixed")
			
		# Fix unrealistically high loan counts
		high_mask = df['NumLoans'] > 100
		if high_mask.sum() > 0:
			# Cap at maximum reasonable value, the 99th percentile or 20, whichever is lower
			cap_value = min(df['NumLoans'].quantile(0.99), 20)
			df.loc[high_mask, 'NumLoans'] = cap_value
			self._log_fixes(high_mask.sum(), f"unrealistically high loan counts fixed (capped at {cap_value})")


class DomainFraudFixerStrategy(FixerStrategy):
	"""Fix domain-specific issues in fraud data."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix domain-specific issues in fraud data."""
		if df.empty:
			return df
			
		# Fix binary indicator fields
		binary_columns = ['IsFraudulent', 'IsOnlineTransaction', 'IsUsedChip', 'IsUsedPIN']
		for col in binary_columns:
			if col in df.columns:
				self._fix_binary_indicators(df, col)
		
		# Fix distance values
		if 'DistanceFromHome' in df.columns:
			self._fix_distance_from_home(df)
			
		if 'DistanceFromLastTransaction' in df.columns:
			self._fix_distance_from_last(df)
			
		# Fix transaction dates
		if 'TransactionDate' in df.columns:
			self._fix_transaction_dates(df)
			
		return df
	
	def _fix_binary_indicators(self, df: pd.DataFrame, col: str) -> None:
		"""Fix binary indicator values to be 0 or 1."""
		# Find values that are not 0 or 1
		invalid_mask = ~df[col].isin([0, 1]) & ~df[col].isna()
		
		if invalid_mask.sum() > 0:
			# Convert invalid values to 0 or 1 based on threshold
			df.loc[invalid_mask, col] = df.loc[invalid_mask, col].apply(
				lambda x: 1 if x and float(x) >= 0.5 else 0
			)
			self._log_fixes(invalid_mask.sum(), f"invalid {col} values fixed")
			
		# Fill NaN values with 0 (most common for binary indicators)
		missing_mask = df[col].isna()
		if missing_mask.sum() > 0:
			df.loc[missing_mask, col] = 0
			self._log_fixes(missing_mask.sum(), f"missing {col} values filled with 0")
	
	def _fix_distance_from_home(self, df: pd.DataFrame) -> None:
		"""Fix distance from home values."""
		# Fix negative distances
		negative_mask = df['DistanceFromHome'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'DistanceFromHome'] = df['DistanceFromHome'].abs()
			self._log_fixes(negative_mask.sum(), "negative distance from home values fixed")
			
		# Fix unrealistically large distances (cap at reasonable maximum, e.g., 20000 km)
		high_mask = df['DistanceFromHome'] > 20000
		if high_mask.sum() > 0:
			df.loc[high_mask, 'DistanceFromHome'] = 20000
			self._log_fixes(high_mask.sum(), "unrealistically large distance from home values fixed")
	
	def _fix_distance_from_last(self, df: pd.DataFrame) -> None:
		"""Fix distance from last transaction values."""
		# Fix negative distances
		negative_mask = df['DistanceFromLastTransaction'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'DistanceFromLastTransaction'] = df['DistanceFromLastTransaction'].abs()
			self._log_fixes(negative_mask.sum(), "negative distance from last transaction values fixed")
			
		# Fix unrealistically large distances (cap at reasonable maximum, e.g., 20000 km)
		high_mask = df['DistanceFromLastTransaction'] > 20000
		if high_mask.sum() > 0:
			df.loc[high_mask, 'DistanceFromLastTransaction'] = 20000
			self._log_fixes(high_mask.sum(), "unrealistically large distance from last transaction values fixed")
	
	def _fix_transaction_dates(self, df: pd.DataFrame) -> None:
		"""Fix transaction dates."""
		if not pd.api.types.is_datetime64_any_dtype(df['TransactionDate']):
			# Try to convert to datetime first
			try:
				df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
			except:
				self._logger.warning("Could not convert TransactionDate to datetime")
				return
		
		# Fix future dates
		today = datetime.now()
		future_mask = df['TransactionDate'] > today
		if future_mask.sum() > 0:
			df.loc[future_mask, 'TransactionDate'] = today
			self._log_fixes(future_mask.sum(), "future transaction dates fixed")
			
		# Fix very old dates (more than 10 years ago)
		ten_years_ago = today - timedelta(days=365*10)
		old_mask = df['TransactionDate'] < ten_years_ago
		if old_mask.sum() > 0:
			df.loc[old_mask, 'TransactionDate'] = ten_years_ago
			self._log_fixes(old_mask.sum(), "very old transaction dates fixed")


class DomainMarketFixerStrategy(FixerStrategy):
	"""Fix domain-specific issues in market data."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix domain-specific issues in market data."""
		if df.empty:
			return df
			
		# Fix open/close/high/low inconsistencies
		price_cols = ['OpenValue', 'CloseValue', 'HighestValue', 'LowestValue']
		if all(col in df.columns for col in price_cols):
			self._fix_price_relationships(df)
			
		# Fix negative price values
		for col in price_cols:
			if col in df.columns:
				self._fix_negative_prices(df, col)
				
		# Fix interest rate values
		if 'InterestRate' in df.columns:
			self._fix_interest_rates(df)
			
		# Fix market dates
		if 'MarketDate' in df.columns:
			self._fix_market_dates(df)
			
		return df
	
	def _fix_price_relationships(self, df: pd.DataFrame) -> None:
		"""Fix inconsistent price relationships (open/close/high/low)."""
		price_cols = ['OpenValue', 'CloseValue', 'HighestValue', 'LowestValue']
		
		# First, ensure we have all required columns
		if not all(col in df.columns for col in price_cols):
			return
		
		# Track valid rows (non-null values for all price columns)
		valid_mask = df[price_cols].notna().all(axis=1)
		if not valid_mask.any():
			self._logger.info("No complete price data rows to fix relationships")
			return
		
		# STEP 1: FIRST fix the High/Low relationship
		invalid_high_low = valid_mask & (df['HighestValue'] < df['LowestValue'])
		if invalid_high_low.any():
			# Store original counts for logging
			invalid_count = invalid_high_low.sum()
			
			# Swap highest and lowest where relationship is wrong
			temp = df.loc[invalid_high_low, 'HighestValue'].copy()
			df.loc[invalid_high_low, 'HighestValue'] = df.loc[invalid_high_low, 'LowestValue']
			df.loc[invalid_high_low, 'LowestValue'] = temp
			
			# Verify swap worked correctly
			still_invalid = valid_mask & (df['HighestValue'] < df['LowestValue'])
			if still_invalid.any():
				self._logger.warning(f"First swap attempt failed for {still_invalid.sum()} rows")
				# Force fix by using max/min
				df.loc[still_invalid, 'HighestValue'] = df.loc[still_invalid, price_cols].max(axis=1)
				df.loc[still_invalid, 'LowestValue'] = df.loc[still_invalid, price_cols].min(axis=1)
				
			self._log_fixes(invalid_count, "highest/lowest value inconsistencies fixed")
		
		# STEP 2: Handle outliers in price columns
		for col in price_cols:
			# Calculate reasonable bounds
			q1 = df[col].quantile(0.25)
			q3 = df[col].quantile(0.75)
			iqr = q3 - q1
			# More conservative bounds for HighestValue to address outliers issue
			lower_bound = max(0, q1 - (1.5 if col != 'HighestValue' else 1.0) * iqr)
			upper_bound = q3 + (1.5 if col != 'HighestValue' else 1.0) * iqr
			
			# Cap extreme outliers
			outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
			if outliers.any():
				df.loc[df[col] < lower_bound, col] = lower_bound
				df.loc[df[col] > upper_bound, col] = upper_bound
				self._log_fixes(outliers.sum(), f"extreme {col} outliers capped")
		
		# STEP 3: Fix open/close values to be within high/low range
		for col in ['OpenValue', 'CloseValue']:
			too_high = valid_mask & (df[col] > df['HighestValue'])
			if too_high.any():
				df.loc[too_high, col] = df.loc[too_high, 'HighestValue']
				self._log_fixes(too_high.sum(), f"{col} values above highest value fixed")
				
			too_low = valid_mask & (df[col] < df['LowestValue'])
			if too_low.any():
				df.loc[too_low, col] = df.loc[too_low, 'LowestValue']
				self._log_fixes(too_low.sum(), f"{col} values below lowest value fixed")
		
		# FINAL CHECK: Ensure no remaining inconsistencies
		final_invalid = valid_mask & (df['HighestValue'] < df['LowestValue'])
		if final_invalid.any():
			self._logger.error(f"CRITICAL: Still have {final_invalid.sum()} high/low inconsistencies after all fixes")

	def _fix_negative_prices(self, df: pd.DataFrame, col: str) -> None:
		"""Fix negative price values."""
		negative_mask = df[col] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, col] = df[col].abs()
			self._log_fixes(negative_mask.sum(), f"negative {col} values fixed")
	
	def _fix_interest_rates(self, df: pd.DataFrame) -> None:
		"""Fix interest rate values."""
		# Fix negative interest rates
		negative_mask = df['InterestRate'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'InterestRate'] = 0
			self._log_fixes(negative_mask.sum(), "negative interest rates fixed")
			
		# Fix unrealistically high interest rates (cap at 30%)
		high_mask = df['InterestRate'] > 30
		if high_mask.sum() > 0:
			df.loc[high_mask, 'InterestRate'] = 30
			self._log_fixes(high_mask.sum(), "unrealistically high interest rates fixed")
	
	def _fix_market_dates(self, df: pd.DataFrame) -> None:
		"""Fix market dates."""
		if not pd.api.types.is_datetime64_any_dtype(df['MarketDate']):
			# Try to convert to datetime first
			try:
				df['MarketDate'] = pd.to_datetime(df['MarketDate'])
			except:
				self._logger.warning("Could not convert MarketDate to datetime")
				return
				
		# Fix future dates
		today = datetime.now()
		future_mask = df['MarketDate'] > today
		if future_mask.sum() > 0:
			df.loc[future_mask, 'MarketDate'] = today
			self._log_fixes(future_mask.sum(), "future market dates fixed")


class DomainMacroFixerStrategy(FixerStrategy):
	"""Fix domain-specific issues in macroeconomic data."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix domain-specific issues in macroeconomic data."""
		if df.empty:
			return df
			
		# Fix ratio values (unemployment, inflation, etc.)
		for col in ['UnemploymentRate', 'InflationRate', 'DebtRatio', 'DeficitRatio']:
			if col in df.columns:
				self._fix_ratio_values(df, col)
				
		# Fix GDP values
		if 'GDP' in df.columns:
			self._fix_gdp_values(df)
			
		# Fix index values
		for col in ['ConsumerPriceIndex', 'HousePriceIndex']:
			if col in df.columns:
				self._fix_index_values(df, col)
				
		# Fix report dates
		if 'ReportDate' in df.columns:
			self._fix_report_dates(df)
			
		return df
	
	def _fix_ratio_values(self, df: pd.DataFrame, col: str) -> None:
		"""Fix ratio values (percentages)."""
		# Special case for inflation which can be legitimately negative
		if 'inflation' in col.lower():
			return
			
		# Fix negative ratios for other fields
		negative_mask = df[col] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, col] = df[col].abs()
			self._log_fixes(negative_mask.sum(), f"negative {col} values fixed")
			
		# Fix unrealistically high ratios
		# Different caps for different ratios
		if col == 'UnemploymentRate':
			cap = 50  # Maximum realistic unemployment rate
		elif col == 'InflationRate':
			cap = 100  # Maximum realistic inflation rate
		elif col in ['DebtRatio', 'DeficitRatio']:
			cap = 200  # Maximum debt or deficit ratio
		else:
			cap = 100  # Default cap for other ratios
			
		high_mask = df[col] > cap
		if high_mask.sum() > 0:
			df.loc[high_mask, col] = cap
			self._log_fixes(high_mask.sum(), f"unrealistically high {col} values fixed")
	
	def _fix_gdp_values(self, df: pd.DataFrame) -> None:
		"""Fix GDP values."""
		# Fix negative GDP
		negative_mask = df['GDP'] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, 'GDP'] = df['GDP'].abs()
			self._log_fixes(negative_mask.sum(), "negative GDP values fixed")
	
	def _fix_index_values(self, df: pd.DataFrame, col: str) -> None:
		"""Fix index values like CPI, HPI."""
		# Fix negative index values
		negative_mask = df[col] < 0
		if negative_mask.sum() > 0:
			df.loc[negative_mask, col] = df[col].abs()
			self._log_fixes(negative_mask.sum(), f"negative {col} values fixed")
	
	def _fix_report_dates(self, df: pd.DataFrame) -> None:
		"""Fix report dates."""
		if not pd.api.types.is_datetime64_any_dtype(df['ReportDate']):
			# Try to convert to datetime first
			try:
				df['ReportDate'] = pd.to_datetime(df['ReportDate'])
			except:
				self._logger.warning("Could not convert ReportDate to datetime")
				return
				
		# Fix future dates
		today = datetime.now()
		future_mask = df['ReportDate'] > today
		if future_mask.sum() > 0:
			df.loc[future_mask, 'ReportDate'] = today
			self._log_fixes(future_mask.sum(), "future report dates fixed")


class DataConsistencyFixerStrategy(FixerStrategy):
	"""Fix data consistency issues across related fields."""
	
	def _fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix data consistency issues."""
		if df.empty:
			return df
			
		# Get dataset type for domain-specific consistency fixes
		dataset_name = self._config.get_config("current_dataset", "")
		
		if dataset_name == "Loan":
			self._fix_loan_consistency(df)
		elif dataset_name == "Market":
			self._fix_market_consistency(df)
		elif dataset_name == "Fraud":
			self._fix_fraud_consistency(df)
			
		return df
	
	def _fix_loan_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency issues in loan data."""
		# Fix monthly payment vs loan amount consistency
		if all(col in df.columns for col in ['MonthlyPayment', 'LoanAmount', 'LoanDurationMonths']):
			self._fix_payment_amount_consistency(df)
			
		# Fix credit utilization vs debt consistency 
		if all(col in df.columns for col in ['CreditUtilizationRatio', 'Balance', 'CreditLimit']):
			self._fix_credit_utilization_consistency(df)
	
	def _fix_payment_amount_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency between monthly payment and loan amount/duration."""
		# Skip rows with missing values
		mask = df['MonthlyPayment'].notna() & df['LoanAmount'].notna() & df['LoanDurationMonths'].notna()
		
		if mask.sum() == 0:
			return
			
		# Calculate approximate expected monthly payment (simple division)
		df['ExpectedPayment'] = df['LoanAmount'] / df['LoanDurationMonths']
		
		# Find rows with major discrepancies (payment is significantly different from expected)
		discrepancy_mask = (
			mask & 
			(df['MonthlyPayment'] > df['ExpectedPayment'] * 2) | 
			(df['MonthlyPayment'] * 2 < df['ExpectedPayment'])
		)
		
		if discrepancy_mask.sum() > 0:
			# Fix monthly payment to be closer to expected
			df.loc[discrepancy_mask, 'MonthlyPayment'] = df.loc[discrepancy_mask, 'ExpectedPayment']
			self._log_fixes(discrepancy_mask.sum(), "monthly payment inconsistencies fixed")
			
		# Clean up temporary column
		df.drop('ExpectedPayment', axis=1, inplace=True)
	
	def _fix_credit_utilization_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency between credit utilization ratio, balance and credit limit."""
		# Fix cases where credit limit is present
		if 'CreditLimit' in df.columns:
			# Skip rows with missing values
			mask = df['CreditUtilizationRatio'].notna() & df['Balance'].notna() & df['CreditLimit'].notna()
			
			if mask.sum() > 0:
				# Calculate expected ratio
				expected_ratio = (df.loc[mask, 'Balance'] / df.loc[mask, 'CreditLimit']) * 100
				
				# Find inconsistent ratios (with significant difference)
				diff_mask = mask & (abs(df['CreditUtilizationRatio'] - expected_ratio) > 10)
				
				if diff_mask.sum() > 0:
					df.loc[diff_mask, 'CreditUtilizationRatio'] = expected_ratio[diff_mask]
					self._log_fixes(diff_mask.sum(), "credit utilization ratio inconsistencies fixed")
		
		# If credit limit not present, ensure ratio is between 0-100
		inconsistent_ratio = (df['CreditUtilizationRatio'] < 0) | (df['CreditUtilizationRatio'] > 100)
		if inconsistent_ratio.sum() > 0:
			# For negative values, use absolute value
			negative_mask = df['CreditUtilizationRatio'] < 0
			if negative_mask.sum() > 0:
				df.loc[negative_mask, 'CreditUtilizationRatio'] = df.loc[negative_mask, 'CreditUtilizationRatio'].abs()
				
			# For values over 100, cap at 100
			high_mask = df['CreditUtilizationRatio'] > 100
			if high_mask.sum() > 0:
				df.loc[high_mask, 'CreditUtilizationRatio'] = 100
				
			self._log_fixes(inconsistent_ratio.sum(), "out-of-range credit utilization ratios fixed")
	
	def _fix_market_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency issues in market data."""
		# Fix consistency between related market indicators
		if all(col in df.columns for col in ['VIX', 'TEDSpread']):
			self._fix_market_indicator_consistency(df)
	
	def _fix_market_indicator_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency between related market indicators."""
		# VIX and TED spread often move together in crisis periods
		# Fix extreme outliers in one indicator when the other is normal
		
		# First check VIX outliers when TED spread is normal
		mask = (df['VIX'] > 50) & (df['TEDSpread'] < 0.5)
		if mask.sum() > 0:
			# Cap VIX at more reasonable level based on TED spread
			df.loc[mask, 'VIX'] = 30 + (df.loc[mask, 'TEDSpread'] * 40)
			self._log_fixes(mask.sum(), "inconsistent VIX values fixed")
			
		# Check TED spread outliers when VIX is normal
		mask = (df['TEDSpread'] > 2) & (df['VIX'] < 20)
		if mask.sum() > 0:
			# Cap TED spread at more reasonable level based on VIX
			df.loc[mask, 'TEDSpread'] = 0.5 + (df.loc[mask, 'VIX'] * 0.025)
			self._log_fixes(mask.sum(), "inconsistent TED spread values fixed")
	
	def _fix_fraud_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency issues in fraud data."""
		# Fix consistency between fraud indicators
		if all(col in df.columns for col in ['IsFraudulent', 'IsOnlineTransaction', 'IsUsedChip', 'IsUsedPIN']):
			self._fix_fraud_indicator_consistency(df)
	
	def _fix_fraud_indicator_consistency(self, df: pd.DataFrame) -> None:
		"""Fix consistency between fraud indicators."""
		# Chip and PIN usage is inconsistent with online transactions
		online_with_chip = (df['IsOnlineTransaction'] == 1) & (df['IsUsedChip'] == 1)
		if online_with_chip.sum() > 0:
			df.loc[online_with_chip, 'IsUsedChip'] = 0
			self._log_fixes(online_with_chip.sum(), "inconsistent chip usage for online transactions fixed")
			
		online_with_pin = (df['IsOnlineTransaction'] == 1) & (df['IsUsedPIN'] == 1)
		if online_with_pin.sum() > 0:
			df.loc[online_with_pin, 'IsUsedPIN'] = 0
			self._log_fixes(online_with_pin.sum(), "inconsistent PIN usage for online transactions fixed")


#======== 2. Fixer Strategy Factory ==========
class FixerStrategyFactory:
	"""Factory for creating data fixer strategies."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._strategies = {}
		self._initialize_strategies()
	
	def _initialize_strategies(self) -> None:
		"""Initialize the registry with default strategies."""
		self.register('missing_value_fixer', MissingValueFixerStrategy(self._config))
		self.register('outlier_fixer', OutlierFixerStrategy(self._config))
		self.register('loan_fixer', DomainLoanFixerStrategy(self._config))
		self.register('fraud_fixer', DomainFraudFixerStrategy(self._config))
		self.register('market_fixer', DomainMarketFixerStrategy(self._config))
		self.register('macro_fixer', DomainMacroFixerStrategy(self._config))
		self.register('consistency_fixer', DataConsistencyFixerStrategy(self._config))
	
	def register(self, name: str, strategy: FixerStrategy) -> None:
		"""Register a fixer strategy."""
		if name in self._strategies:
			raise ValueError(f"Strategy {name} already registered")
		self._strategies[name] = strategy
	
	def get_strategy(self, name: str) -> FixerStrategy:
		"""Get a fixer strategy by name."""
		if name not in self._strategies:
			raise ValueError(f"Unknown fixer strategy: {name}")
		return self._strategies[name]
	
	def create_missing_value_fixer(self) -> FixerStrategy:
		"""Create a missing value fixer strategy."""
		return MissingValueFixerStrategy(self._config)
	
	def create_outlier_fixer(self) -> FixerStrategy:
		"""Create an outlier fixer strategy."""
		return OutlierFixerStrategy(self._config)
	
	def create_domain_fixer(self, dataset_type: str) -> FixerStrategy:
		"""Create a domain-specific fixer based on dataset type."""
		if dataset_type.lower() == "loan":
			return DomainLoanFixerStrategy(self._config)
		elif dataset_type.lower() == "fraud":
			return DomainFraudFixerStrategy(self._config)
		elif dataset_type.lower() == "market":
			return DomainMarketFixerStrategy(self._config)
		elif dataset_type.lower() == "macro":
			return DomainMacroFixerStrategy(self._config)
		else:
			raise ValueError(f"Unknown dataset type: {dataset_type}")



############################################
#======== 3. Fixer Pipeline Steps ==========
class FixerPipelineStep(IPipelineStep):
	"""Base class for fixer pipeline steps."""
	
	def __init__(self, strategy: FixerStrategy):
		self._strategy = strategy
	
	@property
	def name(self) -> str:
		"""Get the name of this pipeline step."""
		return self._strategy.name
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Execute fixer strategy and return fixed dataframe."""
		return self._strategy.fix(df)


#======== 4. Fixer Pipeline ==========
class FixerPipeline:
	"""Pipeline for fixing data quality issues."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._steps: List[IPipelineStep] = []
	
	def add_step(self, step: FixerPipelineStep) -> None:
		"""Add a step to the pipeline."""
		self._steps.append(step)
	
	def process(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process the dataframe through all fixer steps."""
		if df.empty:
			self._logger.warning("Empty dataframe, skipping fixer pipeline")
			return df
			
		result_df = df.copy()
		
		# Add steps counting logic
		steps_count = len(self._steps)
		for step_idx, step in enumerate(self._steps):
			step_start_time = datetime.now()
			try:
				self._logger.info(f"Executing fixer step {step_idx+1}/{steps_count}: {step.name}")
				result_df = step.execute(result_df)
				
				# Track execution time
				step_duration_ms = (datetime.now() - step_start_time).total_seconds() * 1000
				self._logger.debug(f"Step {step.name} completed in {step_duration_ms:.2f} ms")
				
			except Exception as e:
				self._logger.error(f"Error in fixer step {step.name}: {e}")
				traceback.print_exc()
		
		# Report total changes
		fixed_columns = sum(1 for col in df.columns if not df[col].equals(result_df[col]))
		if fixed_columns > 0:
			self._logger.info(f"Fixed issues in {fixed_columns} columns")
			
		return result_df



##############################################
#======== 5. Fixer module API ==========
class DataFixer:
    """Main class for fixing datasets."""
    
    def __init__(self, config_provider: IConfigProvider):
        self._config = config_provider
        self._logger = config_provider.get_logger()
        self._factory = FixerStrategyFactory(config_provider)
        self._dataset_name = None
        self._data = None
        self._results = None
    
    def load_dataset(self, dataset_name: str, df: pd.DataFrame) -> DataFixer:
        """Load the dataset for fixing.
        
        Args:
            dataset_name: Name of the dataset
            df: Dataframe to fix
        """
        self._dataset_name = dataset_name
        self._data = df
        self._config.set_config('current_dataset', dataset_name)
        self._logger.info(f"Loaded dataset for fixing: {dataset_name} with {len(df)} rows and {len(df.columns)} columns")
        return self
    
    def process(self) -> pd.DataFrame:
        """Process dataset by applying all appropriate fixes."""
        if not self._dataset_name or self._data is None:
            self._logger.error("Dataset not loaded")
            return pd.DataFrame()
            
        start_time = datetime.now()
        self._logger.info(f"Starting fixes for {self._dataset_name}")
        
        # Create the fixer pipeline
        pipeline = self.create_pipeline(self._dataset_name)
        
        # Process the dataset
        fixed_data = pipeline.process(self._data)
        
        # Calculate changes
        changed_cols = sum(1 for col in self._data.columns 
                         if col in fixed_data.columns and not self._data[col].equals(fixed_data[col]))
        
        execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        # Store results
        self._results = {
            "dataset_name": self._dataset_name,
            "row_count": len(fixed_data),
            "column_count": len(fixed_data.columns),
            "changed_columns": changed_cols,
            "execution_time_ms": execution_time_ms
        }
        
        self._logger.info(f"Fixing completed in {execution_time_ms:.2f} ms with {changed_cols} columns modified")
        return fixed_data
    
    def get_results(self) -> Dict[str, Any]:
        """Get the fixer results and metadata."""
        return self._results
    
    def save_results(self, output_path: Optional[Path] = None) -> bool:
        """Save fixed data to disk."""
        if self._data is None or self._data.empty:
            self._logger.error("No fixed data to save")
            return False
            
        try:
            output_dir = Path(self._config.get_config('output_dir'))
            
            # Use provided path or default
            if output_path is None:
                output_path = output_dir / f"{self._dataset_name}_fixed.csv"
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save the data
            self._data.to_csv(output_path, index=False)
            self._logger.info(f"Saved fixed data to {output_path}")
            
            return True
            
        except Exception as e:
            self._logger.error(f"Error saving fixed data: {e}")
            traceback.print_exc()
            return False
    
    def create_pipeline(self, dataset_type: str = "") -> FixerPipeline:
        """Create a fixer pipeline appropriate for the given dataset type."""
        pipeline = FixerPipeline(self._config)
        
        # Always add general-purpose fixers
        pipeline.add_step(FixerPipelineStep(
            self._factory.get_strategy('missing_value_fixer')
        ))
        
        pipeline.add_step(FixerPipelineStep(
            self._factory.get_strategy('outlier_fixer')
        ))
        
        # Add domain-specific fixers based on dataset type
        if dataset_type:
            if dataset_type.lower() == "loan":
                pipeline.add_step(FixerPipelineStep(
                    self._factory.get_strategy('loan_fixer')
                ))
            elif dataset_type.lower() == "fraud":
                pipeline.add_step(FixerPipelineStep(
                    self._factory.get_strategy('fraud_fixer')
                ))
            elif dataset_type.lower() == "market":
                pipeline.add_step(FixerPipelineStep(
                    self._factory.get_strategy('market_fixer')
                ))
            elif dataset_type.lower() == "macro":
                pipeline.add_step(FixerPipelineStep(
                    self._factory.get_strategy('macro_fixer')
                ))
        
        # Add consistency fixer last
        pipeline.add_step(FixerPipelineStep(
            self._factory.get_strategy('consistency_fixer')
        ))
        
        return pipeline
	