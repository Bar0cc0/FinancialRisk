#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Interfaces for the data integration engine components."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import logging



class IConfigProvider(ABC):
	"""Interface for configuration providers."""
	
	@abstractmethod
	def get_logger(self) -> Any:
		"""Get the logger instance."""
		pass

	@abstractmethod
	def get_config(self, key: str, default: Any = None) -> Any:
		"""Get a configuration value."""
		pass

	@abstractmethod
	def set_config(self, key: str, value: Any) -> None:
		"""Set a configuration value."""
		pass

class IDataLoader(ABC):
    """Interface for data loaders."""
    @abstractmethod
    def load(self, data_path: Path) -> pd.DataFrame:
        """Load data from a source."""
        pass

class IDataSaver(ABC):
    """Interface for data savers."""
    @abstractmethod
    def save(self, df: pd.DataFrame, output_path: Path) -> bool:
        """Save data to a destination."""
        pass

class ICacheManager(ABC):
		"""Interface for cache management."""
		@abstractmethod
		def get_from_cache(self, dataset_name: str, step_name: str) -> Optional[pd.DataFrame]:
			"""Retrieve data from cache if available."""
			pass
		@abstractmethod
		def save_to_cache(self, df: pd.DataFrame, dataset_name: str, step_name: str) -> None:
			"""Save data to cache for future use."""
			pass
		@abstractmethod
		def clear_cache(self, dataset_name: Optional[str] = None) -> None:
			"""Clear the cache."""
			pass
		@abstractmethod
		def delete_cache_directory(self) -> None:
			"""Delete the entire cache directory."""
			pass


class IDataPreprocessingStrategy(ABC):
    """Interface for data preprocessing strategies."""
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply preprocessing operations to the dataframe."""
        pass

class IDataFixerStrategy(ABC):
	"""Interface for domain-specific outlier fixing."""
	@abstractmethod
	def fix(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Implement specific fixing logic."""
		pass
     
class IDataTransformationStrategy(ABC):
	"""Interface for data transformation strategies."""
	@abstractmethod
	def transform(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Transform the dataframe and return the result."""
		pass

class IDataValidationStrategy(ABC):
	"""Interface for data validation strategies."""
	@abstractmethod
	def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
		"""Validate the dataframe and return validation results."""
		pass

class IDataQualityReporter(ABC):
	"""Interface for data quality reporting."""
	
	@abstractmethod
	def generate_report(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> None:
		"""Generate quality reports based on validation results."""
		pass


class IPipelineStep(ABC):
	"""Interface for pipeline processing steps."""
	
	@property
	@abstractmethod
	def name(self) -> str:
		"""Get the name of this pipeline step."""
		pass

	@abstractmethod
	def execute(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process the data and return the transformed result."""
		pass
