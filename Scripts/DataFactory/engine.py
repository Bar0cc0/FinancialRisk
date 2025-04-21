#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data Factory Engine
===================

This module serves as the main entry point for the financial data processing pipeline,
implementing an extensible orchestration framework for ETL operations on financial datasets.

The engine coordinates the sequential or parallel processing of multiple datasets through
a series of configurable pipeline steps:

1. Data Loading - Extract data from various file formats (CSV, Excel, JSON, Parquet)
2. Data Cleaning - Apply text, datetime, numeric, and categorical preprocessing strategies 
3. Data Transformation - Transform data to match target schemas and generate synthetic fields
4. Data Validation - Validate schema conformity and data quality standards
5. Data Fixing - Apply automatic corrections to identified quality issues
6. Report Generation - Create quality reports for processed datasets

Components
----------
- DataLoader/DataSaver: Handle I/O operations for various file formats
- CacheManager: Manages caching of intermediate results for performance optimization
- Pipeline Classes: Implement the pipeline processing framework
- PipelineExecutor: Runs a complete data pipeline with multiple steps
- PipelineFactory: Creates dataset-specific pipelines with appropriate strategies
- OrchestrationService: Coordinates processing of multiple datasets

Usage
-----
python engine.py --config config.yaml [--root project_root] [--loglevel {DEBUG,INFO}]

Configuration
-------------
The engine is configured through a YAML file that defines:
- Dataset definitions and source files
- Processing strategies and parameters
- Output directories and report settings
- Performance settings (parallelization, caching)

Dependencies
------------
- pandas: Data manipulation
- concurrent.futures: Parallel processing
- Various domain-specific modules: validators, transformers, fixers, etc.
"""

__author__ = "Michael Garancher"
__date__ = "2025-04-08"
__version__ = "1.0"

#=================================================

import os
import sys
import warnings
import argparse
import traceback
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from interfaces import (
	IConfigProvider, IPipelineStep, IDataLoader, IDataSaver, ICacheManager
)

from configuration import Configuration
from processors import PreprocessingPipeline, PreprocessingStrategyFactory, PreprocessingPipelineStep
from transformers import TransformationPipeline, TransformerStrategyFactory, TransformationPipelineStep
from validators import ValidationPipeline, DataValidatorFactory, ValidationPipelineStep, DataQualityReporter
from fixers import FixerPipeline, FixerStrategyFactory, FixerPipelineStep



# Suppress warnings
warnings.filterwarnings('ignore')

# Project root directory
ROOT = Path(__file__).resolve().parents[2]

# Add project root to path for module imports
sys.path.append(str(ROOT))



#========= 1. I/O Helpers ==========
class DataLoader(IDataLoader):
	"""Loads data from files."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._nsamples= config_provider.get_config('nsamples', None)
	
	def load(self, data_path: Path) -> pd.DataFrame:
		"""Load data from a file path."""
		try:
			self._logger.debug(f"Loading data from {data_path}")
			
			if data_path.suffix.lower() == '.csv':
				df = pd.read_csv(data_path, nrows=self._nsamples)
			elif data_path.suffix.lower() == '.xlsx':
				df = pd.read_excel(data_path, nrows=self._nsamples)
			elif data_path.suffix.lower() == '.json':
				df = pd.read_json(data_path, nrows=self._nsamples, orient='records')
			elif data_path.suffix.lower() == '.parquet':
				df = pd.read_parquet(data_path)
			else:
				raise ValueError(f"Unsupported file format: {data_path.suffix}")
				
			self._logger.info(f"Successfully loaded {len(df)} rows from {data_path.name}")
			return df
			
		except Exception as e:
			self._logger.error(f"Error loading data from {data_path}: {e}")
			traceback.print_exc()
			return pd.DataFrame()  # Return empty dataframe on error


class DataSaver(IDataSaver):
	"""Saves data to files."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
	
	def save(self, df: pd.DataFrame, output_path: Path) -> bool:
		"""Save data to a file path."""
		try:
			self._logger.info(f"Saving data to {output_path}")
				
			# Ensure directory exists
			output_path.parent.mkdir(parents=True, exist_ok=True)
			
			# Save based on file extension
			if output_path.suffix.lower() == '.csv':
				df.to_csv(output_path, index=False)
			elif output_path.suffix.lower() == '.xlsx':
				df.to_excel(output_path, index=False)
			elif output_path.suffix.lower() == '.json':
				df.to_json(output_path, orient='records')
			else:
				raise ValueError(f"Unsupported file format: {output_path.suffix}")
				
			self._logger.info(f"Successfully saved {len(df)} rows to {output_path.name}")
			return True
			
		except Exception as e:
			self._logger.error(f"Error saving data to {output_path}: {e}")
			traceback.print_exc()
			return False


class CacheManager(ICacheManager):
	"""Manage caching of intermediate results between pipeline stages."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._memory_cache = {}
		self._enabled = config_provider.get_config('enable_caching', True)
		
		# Set up cache directory
		cache_dir = config_provider.get_config('cache_dir', 'cache')
		self._cache_dir = Path(cache_dir)
		if self._enabled:
			self._cache_dir.mkdir(exist_ok=True, parents=True)
	
	def get_from_cache(self, dataset_name: str, step_name: str) -> Optional[pd.DataFrame]:
		"""Retrieve data from cache if available."""
		if not self._enabled:
			return None
			
		# Generate cache key
		cache_key = f"{dataset_name}_{step_name}"
		
		# Check memory cache first
		if cache_key in self._memory_cache:
			self._logger.info(f"Retrieved {step_name} result from memory cache")
			return self._memory_cache[cache_key].copy()
		
		# Try disk cache
		cache_path = self._cache_dir / f"{cache_key}.pkl"
		if cache_path.exists():
			try:
				df = pd.read_pickle(cache_path)
				# Store in memory cache for future use
				self._memory_cache[cache_key] = df.copy()
				self._logger.info(f"Retrieved {step_name} result from disk cache")
				return df
			except Exception as e:
				self._logger.warning(f"Failed to load cache from {cache_path}: {e}")
		
		return None
	
	def save_to_cache(self, df: pd.DataFrame, dataset_name: str, step_name: str) -> None:
		"""Save data to cache for future use."""
		if not self._enabled or df is None or df.empty:
			return
			
		cache_key = f"{dataset_name}_{step_name}"
		
		# Save to memory cache
		self._memory_cache[cache_key] = df.copy()
		
		# Save to disk cache
		try:
			cache_path = self._cache_dir / f"{cache_key}.pkl"
			df.to_pickle(cache_path)
			self._logger.debug(f"Cached {step_name} result for {dataset_name}")
		except Exception as e:
			self._logger.warning(f"Failed to save cache to {cache_path}: {e}")
	
	def clear_cache(self, dataset_name: Optional[str] = None) -> None:
		"""Clear cache for a specific dataset or all datasets."""
		if not self._enabled:
			return
			
		# Clear memory cache
		if dataset_name:
			keys_to_remove = [k for k in self._memory_cache if k.startswith(f"{dataset_name}_")]
			for key in keys_to_remove:
				del self._memory_cache[key]
			
			# Clear disk cache for this dataset
			for cache_file in self._cache_dir.glob(f"{dataset_name}_*.pkl"):
				cache_file.unlink()
			
			self._logger.info(f"Cleared cache for dataset {dataset_name}")
		else:
			# Clear all cache
			self._memory_cache.clear()
			
			# Clear all disk cache
			for cache_file in self._cache_dir.glob("*.pkl"):
				cache_file.unlink()
			
			self._logger.info("Cleared all cache")

	def delete_cache_directory(self) -> None:
		"""Delete the entire cache directory."""
		if not self._enabled:
			return
			
		try:
			# First clear memory cache
			self._memory_cache.clear()
			self._logger.info('Cleared memory cache')
			
			# Delete cache directory if it exists
			if self._cache_dir.exists():
				# Delete all files first
				for cache_file in self._cache_dir.glob("*.*"):
					try:
						cache_file.unlink()
					except Exception as e:
						self._logger.warning(f"Could not delete cache file {cache_file}: {e}")
				
				# Try to remove the directory itself
				try:
					self._cache_dir.rmdir()
					self._logger.info('Deleted cache directory')
				except Exception as e:
					self._logger.warning(f"Could not delete cache directory: {e}")
		except Exception as e:
			self._logger.error(f"Error during cache directory deletion: {e}")
			traceback.print_exc()		



#========= 2. Pipeline Steps ==========
class DataCleaningStep(IPipelineStep):
	"""Pipeline step for data cleaning using the cleaning pipeline."""
	
	def __init__(self, config_provider: IConfigProvider, cleaning_pipeline: PreprocessingPipeline):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._cleaning_pipeline = cleaning_pipeline
	
	@property
	def name(self) -> str:
		return 'DataCleaning'
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Clean data using the preprocessing pipeline."""
		if df.empty:
			return df
			
		self._logger.info('Running data preprocessing pipeline')
		return self._cleaning_pipeline.process(df)
	
 
class TransformationStep(IPipelineStep):
	"""Pipeline step for data transformation."""
	
	def __init__(self, config_provider: IConfigProvider, transformer: TransformationPipeline):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._transformer = transformer
	
	@property
	def name(self) -> str:
		return 'Transformation'
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Transform the data."""
		if df.empty:
			return df
		
		self._logger.info('Running data transformation pipeline')
		return self._transformer.process(df)
	

class DataValidationStep(IPipelineStep):
	"""Pipeline step for data validation."""
	
	def __init__(self, config_provider: IConfigProvider, validator: ValidationPipeline, post_fixing: bool = False):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._validator = validator
		self._last_validation_results = None  # Store results for later access
		self._post_fixing = post_fixing
	
	@property
	def name(self) -> str:
		# Use different names for pre-fix and post-fix validation
		return 'DataValidation_Post' if self._post_fixing else 'DataValidation_Pre'
	
	@property
	def last_validation_results(self) -> Dict[str, Any]:
		"""Get the results from the last validation."""
		return self._last_validation_results
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Validate the data and store results."""
		if df.empty:
			self._logger.error('Data validation failed: DataFrame is empty')
			return df
		
		validation_stage = "post-fixing" if self._post_fixing else "pre-fixing"
		self._logger.info(f"Running data validation pipeline ({validation_stage})")
		
		# Run validation and store results
		self._last_validation_results = self._validator.process(df)
		
		return df
	

class DataFixerStep(IPipelineStep):
	"""Pipeline step for fixing data quality issues."""
	
	def __init__(self, config_provider: IConfigProvider, fixer: FixerPipeline, dataset_type: str):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._dataset_type = dataset_type
		self._fixer = fixer
		self._validation_results = None
	
	@property
	def name(self) -> str:
		return 'DataFixer'
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Fix data quality issues in the dataset."""
		if df.empty:
			return df
			
		self._logger.info(f"Running data fixer pipeline for {self._dataset_type}")
		
		return self._fixer.process(df)


class FinalCustomStep(IPipelineStep):
	"""Pipeline step for final data cleanup operations."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()

		# Create CleanupStrategy instance to handle the actual cleanup operations
		from processors import CleanupStrategy
		self._cleanup_strategy = CleanupStrategy(config_provider)
	
	@property
	def name(self) -> str:
		return 'FinalCleanup'
	
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		if df.empty:
			return df
		
		df_copy = df.copy()
			
		self._logger.info('Running final custom operations')

		# Final operations on the dataframe
		try:
			# Delegate cleanup operations to the CleanupStrategy
			df_copy = self._cleanup_strategy.process(df_copy)
			
			# Apply SQL compatibility fixes before adding lineage
			dataset_name = self._config.get_config('current_dataset', '')
			df_copy = self._ensure_sql_compatibility(df_copy, dataset_name)
			
			# Add lineage columns
			if self._config.get_config('add_data_lineage', True):
				df_copy = self._add_datalineage(df_copy)
			
			return df_copy
			
		except Exception as e:
			self._logger.error(f"Error during cleanup: {e}")
			traceback.print_exc() 
			return df  # Return original dataframe on error

	def _ensure_sql_compatibility(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
		"""Ensure the dataframe is compatible with SQL database constraints."""
		import re
		try:
			# Get schema for the dataset
			schema = self._config.get_schema_for_dataset(dataset_name)
			if not schema:
				return df
				
			# 1. Handle decimal precision for numeric columns
			for col in df.columns:
				if col in schema and pd.api.types.is_numeric_dtype(df[col]):
					sql_type = schema[col]
					
					# Process DECIMAL columns
					if "DECIMAL" in sql_type:
						try:
							# Extract precision and scale
							match = re.search(r'DECIMAL\((\d+),(\d+)\)', sql_type)
							if match:
								scale = int(match.group(2))
								self._logger.debug(f"Enforcing {scale} decimal places for {col}")
								df[col] = df[col].round(scale)
						except:
							# Default to 2 decimal places if parsing fails
							self._logger.debug(f"Using default 2 decimal places for {col}")
							df[col] = df[col].round(2)
					
					# Special handling for GDP (should be integer)
					if col == "GDP":
						df[col] = df[col].round().astype('Int64')
			
			# 2. Handle uniqueness constraints for Macro data
			if dataset_name == 'Macro':
				if 'ReportDate' in df.columns and 'CountryName' in df.columns:
					# Check for duplicates
					duplicates = df.duplicated(subset=['ReportDate', 'CountryName'], keep=False)
					duplicate_count = duplicates.sum()
					
					if duplicate_count > 0:
						self._logger.warning(f"Found {duplicate_count} duplicate date/country combinations in final Macro data. Keeping first occurrence only.")
						df = df.drop_duplicates(subset=['ReportDate', 'CountryName'], keep='first')
			
			# 3. Handle uniqueness of account IDs
			if 'AccountID' in df.columns:
				# Check for duplicates
				duplicates = df.duplicated(subset=['AccountID'], keep=False)
				duplicate_count = duplicates.sum()
				
				if duplicate_count > 0:
					self._logger.warning(f"Found {duplicate_count} duplicate AccountIDs. Fixing by adding suffixes.")
					
					# Get list of duplicate IDs
					duplicate_ids = df.loc[duplicates, 'AccountID'].unique()
					
					for dup_id in duplicate_ids:
						# Get indices of rows with this duplicate ID
						dup_indices = df.index[df['AccountID'] == dup_id].tolist()
						
						# Keep first occurrence unchanged, modify others
						for i, idx in enumerate(dup_indices[1:], 1):
							# Generate a new unique suffix based on position
							new_id = f"{df.at[idx, 'AccountID']}_{i:02d}"
							df.at[idx, 'AccountID'] = new_id
							self._logger.debug(f"Changed duplicate AccountID from {dup_id} to {new_id}")
				
				# Verify uniqueness again
				if df.duplicated(subset=['AccountID']).any():
					self._logger.error("Failed to ensure AccountID uniqueness")
				else:
					self._logger.info(f"Verified {len(df['AccountID'].unique())} unique AccountID values")

			# 4. Enforce specific domain constraints for database CHECK constraints
			if dataset_name == 'Loan' and 'InterestRate' in df.columns:
				# Apply CK_InterestRate CHECK (InterestRate >= 0 AND InterestRate <= 30)
				original_values = df['InterestRate'].copy()
				df['InterestRate'] = df['InterestRate'].clip(0, 30)
				
				# Log how many values were modified
				modified_count = (original_values != df['InterestRate']).sum()
				if modified_count > 0:
					self._logger.warning(
						f"Modified {modified_count} out-of-range InterestRate values to meet CHECK constraint (0-30%)"
					)
				
			# 5. Handle Customer dataset primary key constraint
			if dataset_name == 'Customer' and 'CustomerID' in df.columns:
				date_col = next((col for col in df.columns if 'Date' in col), None)
				
				if date_col:
					# Check for duplicates on composite PK
					duplicates = df.duplicated(subset=['CustomerID', date_col], keep=False)
					duplicate_count = duplicates.sum()
					
					if duplicate_count > 0:
						self._logger.warning(f"Found {duplicate_count} duplicate CustomerID/Date combinations. Aggregating to ensure PRIMARY KEY compliance.")
						
						# Use groupby to aggregate duplicate records
						# For specific financial metrics, use appropriate aggregation functions
						numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
						
						# Define specific aggregation methods for financial metrics
						agg_dict = {}
						for col in numeric_cols:
							if col == 'CustomerID':
								continue  # Skip the CustomerID column as it's a groupby key
							elif col in ['TotalAssets', 'TotalLiabilities', 'AnnualExpenses', 'AnnualBonus']:
								agg_dict[col] = 'sum'  # Sum financial amounts
							elif col in ['DebtToIncomeRatio', 'CreditUtilizationRatio', 'MonthlySavings']:
								agg_dict[col] = 'mean'  # Average ratios
							elif col in ['CreditScore', 'AnnualIncome', 'PaymentHistoryYears']:
								agg_dict[col] = 'max'   # Use max for ratings, income
							else:
								agg_dict[col] = 'first'  # Default to first value for other numeric fields
						
						# Handle string columns - generally keep the first non-null value
						for col in df.select_dtypes(include=['object']).columns:
							if col != date_col and col != 'CustomerID':
								agg_dict[col] = lambda x: x.dropna().iloc[0] if not x.dropna().empty else None
						
						# Perform the aggregation
						df = df.groupby(['CustomerID', date_col], as_index=False).agg(agg_dict)
						self._logger.info(f"Successfully aggregated customer data to {len(df)} unique records")
					
						# Verify uniqueness
						if df.duplicated(subset=['CustomerID', date_col]).any():
							self._logger.error("Failed to ensure CustomerID/Date uniqueness after aggregation!")
						else:
							self._logger.info(f"Verified unique CustomerID/Date combinations: {df[['CustomerID', date_col]].shape[0]}")
			
			
			return df
			
		except Exception as e:
			self._logger.error(f"Error ensuring SQL compatibility: {str(e)}")
			traceback.print_exc()
			return df

	def _add_datalineage(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Add data lineage columns to the dataframe."""
		if df.empty:
			return df
		
		try:
			import uuid
			batch_id = str(uuid.uuid4())
			df['LoadBatchID'] = batch_id
			df['LoadDate'] = pd.Timestamp.now()
			df['LastUpdated'] = None
			self._logger.info('Added data lineage columns')
		except Exception as e:
			self._logger.error(f"Error adding data lineage: {e}")
		
		return df


#========= 3. Pipeline Orchestration ==========
class PipelineExecutor:
	"""Executes a data processing pipeline with multiple steps."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._steps: List[IPipelineStep] = []
		self._loader = DataLoader(config_provider)
		self._saver = DataSaver(config_provider)
		self._cache_manager = CacheManager(config_provider)
		self._dataset_name = config_provider.get_config('current_dataset', 'Unknown')
	
	def add_step(self, step: IPipelineStep) -> 'PipelineExecutor':
		"""Add a step to the pipeline."""
		self._steps.append(step)
		return self
		
	def execute(self, input_path: Path, output_path: Path) -> tuple[bool, Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
		"""Execute pipeline and return success status, final dataframe, and validation results."""
		final_df = None
		last_validation_results = None
		success = False
		try:
			# Load data
			df = self._loader.load(input_path)
			
			if df.empty:
				self._logger.error('Failed to load data or empty dataset')
				return False, None, None
			
			# Process through pipeline steps
			for step in self._steps:
				step_start_time = pd.Timestamp.now()
				
				# Try to get cached result
				cached_df = self._cache_manager.get_from_cache(self._dataset_name, step.name)

				if cached_df is not None:
					# Use cached result
					df = cached_df
				else:
					# Execute the step
					df = step.execute(df)
					
					# Cache the result
					self._cache_manager.save_to_cache(df, self._dataset_name, step.name)

				# Capture validation results if this is a validation step
				if isinstance(step, DataValidationStep) and hasattr(step, 'last_validation_results'):
					last_validation_results = step.last_validation_results
				
				step_duration = (pd.Timestamp.now() - step_start_time).total_seconds()
				self._logger.info(f"Step {step.name} completed in {step_duration:.2f} seconds")
				
				if df.empty:
					self._logger.error(f"Pipeline step {step.name} returned empty dataframe")
					return False
				
				# Save checkpoint if enabled
				if self._config.get_config('checkpointing', False):
					checkpoint_path = output_path.with_name(f"{output_path.stem}_{step.name}_checkpoint{output_path.suffix}")
					self._saver.save(df, checkpoint_path)
			
			# Save final result
			final_df = df  # Store the final dataframe
			success = self._saver.save(df, output_path)
			
			# Return success, final dataframe, and validation results
			return success, final_df, last_validation_results
			
		except Exception as e:
			self._logger.error(f"Error executing pipeline: {e}")
			traceback.print_exc()
			return False, final_df, last_validation_results


class PipelineFactory:
	"""Factory for creating pipelines based on dataset type."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
	
	def create_pipeline(self, dataset_type: str) -> PipelineExecutor:
		"""Create a pipeline for the specified dataset type."""
		pipeline = PipelineExecutor(self._config)
		
		# Store the current dataset name in configuration 
		self._config.set_config('current_dataset', dataset_type)
		
		# Create data cleaning pipeline with appropriate strategies
		cleaning_pipeline = self._create_cleaning_pipeline(dataset_type)
		pipeline.add_step(DataCleaningStep(self._config, cleaning_pipeline))
		
		# Add transformation step
		transformation_pipeline = self._create_transformation_pipeline(dataset_type)
		pipeline.add_step(TransformationStep(self._config, transformation_pipeline))
		
		# Add data validation step
		validation_pipeline = self._create_validation_pipeline(dataset_type)
		validation_pipeline.reset_validation_count()
		pipeline.add_step(DataValidationStep(self._config, validation_pipeline, post_fixing=False))
		
		# Add data fixer step
		fixing_pipeline = self._create_fixing_pipeline(dataset_type)
		pipeline.add_step(DataFixerStep(self._config, fixing_pipeline, dataset_type))
			
		# Add a second validation step to verify fixes
		if self._config.get_config('validate_after_fixing', True):
			pipeline.add_step(DataValidationStep(self._config, validation_pipeline, post_fixing=True))
		
		# Add final custom step
		pipeline.add_step(FinalCustomStep(self._config))
		
		return pipeline
	
	def _create_cleaning_pipeline(self, dataset_type: str) -> PreprocessingPipeline:
		"""Create a cleaning pipeline with appropriate strategies for the dataset."""
		factory = PreprocessingStrategyFactory(self._config)
		pipeline = PreprocessingPipeline(self._config)
		
		# Add standard preprocessing strategies
		pipeline.add_step(PreprocessingPipelineStep(
			factory.get_strategy('text_cleaning')
		))
		
		pipeline.add_step(PreprocessingPipelineStep(
			factory.get_strategy('datetime_processing')
		))

		pipeline.add_step(PreprocessingPipelineStep(
			factory.get_strategy('numeric_processing')
		))
			
		pipeline.add_step(PreprocessingPipelineStep(
			factory.get_strategy('categorical_processing')
		))
		
		return pipeline

	def _create_transformation_pipeline(self, dataset_type: str) -> TransformationPipeline:
		"""Create a transformation pipeline with appropriate strategies for the dataset."""
		factory = TransformerStrategyFactory(self._config)
		factory.initialize_strategies(dataset_type)
		pipeline = TransformationPipeline(self._config)
		
		# Add schema transformation step - adjust with correct strategy name
		pipeline.add_step(TransformationPipelineStep(
			factory.get_strategy('Schema_transformation')
		))
		
		# Add data generation step if needed
		pipeline.add_step(TransformationPipelineStep(
			factory.get_strategy('Data_generation')
		))
		
		return pipeline
	
	def _create_validation_pipeline(self, dataset_type: str) -> ValidationPipeline:
		"""Create a validation pipeline with appropriate strategies for the dataset."""
		factory = DataValidatorFactory(self._config)
		pipeline = ValidationPipeline(self._config)
		
		# Add standard validation strategies
		# Schema validation
		pipeline.add_step(ValidationPipelineStep(
			factory.get_strategy('schema_validation')
		))
		
		# Data quality validation
		pipeline.add_step(ValidationPipelineStep(
			factory.get_strategy('data_quality')
		))
		
		# Domain-specific validation
		pipeline.add_step(ValidationPipelineStep(
			factory.get_strategy('domain_validation')
		)) 

		return pipeline

	def _create_fixing_pipeline(self, dataset_type: str) -> FixerPipeline:
		"""Create a fixing pipeline with appropriate strategies for the dataset."""
		factory = FixerStrategyFactory(self._config)
		pipeline = FixerPipeline(self._config)
		
		# Always add general-purpose fixers
		pipeline.add_step(FixerPipelineStep(
			factory.get_strategy('missing_value_fixer')
		))
		
		pipeline.add_step(FixerPipelineStep(
			factory.get_strategy('outlier_fixer')
		))
		
		# Add domain-specific fixers based on dataset type
		if dataset_type:
			if dataset_type.lower() == "loan":
				pipeline.add_step(FixerPipelineStep(
					factory.get_strategy('loan_fixer')
				))
			elif dataset_type.lower() == "fraud":
				pipeline.add_step(FixerPipelineStep(
					factory.get_strategy('fraud_fixer')
				))
			elif dataset_type.lower() == "market":
				pipeline.add_step(FixerPipelineStep(
					factory.get_strategy('market_fixer')
				))
			elif dataset_type.lower() == "macro":
				pipeline.add_step(FixerPipelineStep(
					factory.get_strategy('macro_fixer')
				))
		
		# Add consistency fixer last
		pipeline.add_step(FixerPipelineStep(
			factory.get_strategy('consistency_fixer')
		))
		
		return pipeline


class OrchestrationService:
	"""Orchestrates processing of multiple datasets."""
	
	def __init__(self, config_provider: IConfigProvider):
		self._config = config_provider
		self._logger = config_provider.get_logger()
		self._datasets = {}
		self._pipeline_factory = PipelineFactory(config_provider)
		self._loader = DataLoader(config_provider)
		self._saver = DataSaver(config_provider)
		self._reporter = DataQualityReporter(config_provider)
	
	def register_datasets(self, datasets: Dict[str, Dict]) -> None:
		"""Register datasets for processing from config mapping."""
		# Transform config format to orchestration format
		dataset_mapping = {}
		for dataset_name, config in datasets.items():
			dataset_mapping[dataset_name] = config.get('sources', [])
			
		self._datasets = dataset_mapping
		self._logger.info(f"Registered {len(dataset_mapping)} datasets for processing")
	
	def process_all(self) -> List[Dict[str, Any]]:
		"""Process all registered datasets then generate reports."""
		all_processing_results = []  # Store complete results for reporting
		processing_summary = []      # Summary for return value
		
		parallel = self._config.get_config('parallel_processing', False)
		max_workers = self._config.get_config('max_workers', min(4, os.cpu_count() or 1))
		
		if parallel and len(self._datasets) > 1:
			self._logger.info(f"Starting processing with parallel={parallel}, max_workers={max_workers}")
			if isinstance(max_workers, str): # Allow 'auto', 'os.cpu_count()' calculation
				max_workers = max_workers.strip()
				max_workers = max(1, os.cpu_count() - 1) if max_workers == 'auto' else eval(max_workers)
			elif isinstance(max_workers, int):
				max_workers = max(1, min(max_workers, os.cpu_count() or 1))
			
			with ThreadPoolExecutor(max_workers=max_workers) as executor:
				# Submit all jobs
				future_to_dataset = {
					executor.submit(self._process_dataset, dataset_type, files): dataset_type
					for dataset_type, files in self._datasets.items()
				}
				
				# Process results as they complete
				for future in concurrent.futures.as_completed(future_to_dataset):
					dataset_name = future_to_dataset[future]
					try:
						result = future.result()
						
						# Ensure result has dataset set 
						if 'dataset' not in result:
							result['dataset'] = dataset_name
							
						all_processing_results.append(result)
						
						# Create a summary without the dataframes
						summary = {
							'dataset': dataset_name,
							'status': result['status'],
							'output_path': result.get('output_path')
						}
						processing_summary.append(summary)
						
						self._logger.info(f"Completed processing dataset: {dataset_name}")
					except Exception as e:
						error_result = {
							'status': 'error', 
							'dataset': dataset_name, 
							'error': f"Exception: {str(e)}"
						}
						all_processing_results.append(error_result)
						processing_summary.append(error_result)
						self._logger.error(f"Exception processing {dataset_name}: {e}")
		else:
			# Sequential processing
			self._logger.info('Starting sequential processing')
			for dataset_type, files in self._datasets.items():
				try:
					# Set dataset context for logging
					self._config.set_config('current_dataset', dataset_type)
					result = self._process_dataset(dataset_type, files)
					
					# Make sure result has dataset set (defensive)
					if 'dataset' not in result:
						result['dataset'] = dataset_type
						
					# Add to results collections
					all_processing_results.append(result)
					
					# Create a summary without the dataframes
					summary = {
						'dataset': dataset_type,
						'status': result['status'],
						'output_path': result.get('output_path')
					}
					processing_summary.append(summary)
					
				except Exception as e:
					error_result = {
						'status': 'error', 
						'dataset': dataset_type, 
						'error': str(e)
					}
					# Add to results collections
					all_processing_results.append(error_result)
					processing_summary.append(error_result)
					self._logger.error(f"Error processing {dataset_type}: {e}")
					traceback.print_exc()
		
		# Reset to System context after processing
		self._config.set_config('current_dataset', '')
		self._logger.info('All datasets processing complete')
		
		# Generate reports for all datasets        
		# Check if report generation is enabled
		if not self._config.get_config('data_quality_report', False):
			self._logger.info('Data quality report generation is disabled in config.')
			return processing_summary
		
		# Generate reports sequentially for each dataset
		self._logger.info('Starting report generation')
		for result in all_processing_results:
			if 'dataset' not in result:
				self._logger.warning("Skipping report generation: Result missing dataset name")
				continue
				
			dataset_name = result['dataset']
			df = result.get('df')
			validation_results = result.get('validation_results')

			# Set the current dataset for proper logging context
			self._config.set_config('current_dataset', dataset_name)
			
			# Skip if we don't have valid data or results
			if (result['status'] != 'success' or 
				df is None or df.empty or 
				validation_results is None):
				self._logger.warning(f"Skipping report generation for {dataset_name}: " +
									f"Invalid data or validation results not available.")
				continue
				
			try:
				# Configure reporter for this dataset
				self._reporter.set_dataset_name(dataset_name)
				
				# Force report generation by resetting flag
				if 'report_generated' in validation_results:
					validation_results['report_generated'] = False
					
				# Add force_regenerate flag to ensure report is generated
				validation_results['force_regenerate'] = True
				
				# Generate report
				self._reporter.generate_report(df, validation_results)
				
			except Exception as e:
				self._logger.error(f"Error generating report for {dataset_name}: {e}")
				traceback.print_exc()
		
		self._logger.info(f"Reports saved to {self._reporter.get_report_dir()}")

		# Reset to System context after all reporting is done
		self._config.set_config('current_dataset', '')
		self._logger.info('Report generation complete')

		return processing_summary

	def cleanup(self) -> None:
		"""Clean up temporary resources after processing."""
		try:
			# Create a cache manager with the same configuration
			cache_manager = CacheManager(self._config)
			
			# Delete the cache directory
			cache_manager.delete_cache_directory()
			
			# Remove any temporary files
			output_dir = Path(self._config.get_config('output_dir'))
			for temp_file in output_dir.glob("*_temp.*"):
				try:
					temp_file.unlink()
					self._logger.debug(f"Deleted temporary file: {temp_file}")
				except Exception as e:
					self._logger.warning(f"Could not delete temp file {temp_file}: {e}")
		except Exception as e:
			self._logger.error(f"Error during cache cleanup: {e}")
			traceback.print_exc()

	def _process_dataset(self, dataset_type: str, files: List[str]) -> Dict[str, Any]:
		"""Process a single dataset."""
		# Ensure proper dataset context for this thread
		self._config.set_config('current_dataset', dataset_type)
		self._logger.info(f"Processing dataset with {len(files)} files")
		
		try:
			if not files:
				return {'status': 'error', 'error': 'No files specified'}
				
			input_dir = Path(self._config.get_config('input_dir'))
			output_dir = Path(self._config.get_config('output_dir'))
			
			# Handle single vs multiple files
			if len(files) == 1:
				input_path = input_dir / files[0]
				output_path = output_dir / f"{dataset_type}_cooked.csv"
				
				# Create and execute appropriate pipeline
				pipeline = self._pipeline_factory.create_pipeline(dataset_type)
				success, final_df, validation_results = pipeline.execute(input_path, output_path)
				
				# Return all necessary information for later reporting
				return {
					'status': 'success' if success else 'error',
					'dataset': dataset_type,
					'file_count': len(files) if isinstance(files, list) else 1,
					'output_path': str(output_path) if success else None,
					'df': final_df if success else None,
					'validation_results': validation_results
				}
			else:
				# Handle multiple files - concatenate first
				concatenated_data = self._concatenate_files(dataset_type, files)
				
				if concatenated_data.empty:
					return {'status': 'error', 'error': 'Failed to concatenate files'}
					
				# Save temp concatenated file
				temp_path = output_dir / f"{dataset_type}_concatenated_temp.csv"
				self._saver.save(concatenated_data, temp_path)
				
				# Process the concatenated file
				output_path = output_dir / f"{dataset_type}_cooked.csv"
				pipeline = self._pipeline_factory.create_pipeline(dataset_type)
				success, final_df, validation_results = pipeline.execute(temp_path, output_path)
				
				# Clean up temp file if needed
				if not self._config.get_config('keep_temp_files', False):
					try:
						temp_path.unlink()
					except:
						pass
				
			return {
				'status': 'success' if success else 'error',
				'dataset': dataset_type,
				'file_count': len(files),
				'output_path': str(output_path) if success else None,
				'df': final_df if success else None,
				'validation_results': validation_results
			}
				
		except Exception as e:
			self._logger.error(f"Error processing {dataset_type}: {e}")
			traceback.print_exc()
			return {
				'status': 'error', 
				'dataset': dataset_type, 
				'error': str(e),
				'df': None,
				'validation_results': None
			}
	
	def _concatenate_files(self, dataset_type: str, files: List[str]) -> pd.DataFrame:
		"""Concatenate multiple files into a single dataframe."""
		input_dir = Path(self._config.get_config('input_dir'))
		dataframes = []
		
		for file in files:
			try:
				file_path = input_dir / file
				df = self._loader.load(file_path)
				
				if not df.empty:
					dataframes.append(df)
					
			except Exception as e:
				self._logger.error(f"Error loading {file}: {e}")
				
		if not dataframes:
			return pd.DataFrame()
			
		# Concatenate files - handle overlapping columns
		return pd.concat(dataframes, axis=1)



#========= 4. Main Entrypoint ==========
def parse_arguments():
	"""Parse command line arguments"""
	parser = argparse.ArgumentParser(description='Financial Data Processing Pipeline')
	parser.add_argument(
		'--config', 
		type=str, 
		default='config.yaml',
		help='Path to configuration file'
	)
	parser.add_argument(
		'--root',
		type=str,
		help='Project root directory (defaults to parent directory of the script)'
	)

	parser.add_argument(
		'--loglevel',
		type=str,
		default='INFO',
		choices=['DEBUG', 'INFO'],
		help='Set the logging level (default: INFO)'
	)

	return parser.parse_args()


def main():
	# Parse command line arguments
	args = parse_arguments()
	
	try:
		# Start engine
		prompt = f"DataFactory Engine Version: {__version__}"
		print('-' * len(prompt), f"\n{prompt}\n", '-' * len(prompt), sep='')

		# Initialize configuration with the specified config path and root dir
		config = Configuration(args.config, args.root, args.loglevel)
		logger = config.get_logger()

		# Log system information
		logger.info('Starting Financial Risk Data Processing Pipeline')
		logger.info(f'Project root: {config.get_config("root_dir")}')
		
		# Get dataset mappings from configuration
		datasets = config.get_dataset_mappings()
		if not datasets:
			logger.error('No datasets found in configuration')
			return 1
			
		logger.info(f"Found {len(datasets)} datasets in configuration: {', '.join(datasets.keys())}")
		
		# Initialize and run the orchestration service
		orchestrator = OrchestrationService(config)
		orchestrator.register_datasets(datasets)
		
		# Process start time
		start_time = pd.Timestamp.now()
		logger.info(f"Processing started at: {start_time}")
		
		# Run the pipeline
		results = orchestrator.process_all()
		
		# Processing end time
		end_time = pd.Timestamp.now()
		duration = (end_time - start_time).total_seconds()
		
		# Report results
		success_count = sum(1 for r in results if r['status'] == 'success')
		logger.info(f"Processed {success_count}/{len(results)} datasets successfully")
		
		# Clean up cache and temporary files
		orchestrator.cleanup()

		# Log final processing time
		logger.info(f"Total processing time: {duration:.2f} seconds")
		
		# Return appropriate exit code
		return 0 if success_count == len(results) else 1
	
	except Exception as e:
		print(f"ERROR: Unhandled exception in pipeline: {e}")
		traceback.print_exc()
		return 1


if __name__ == '__main__':
	exit_code = main()
	sys.exit(exit_code)