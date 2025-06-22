"""
RoboLake CLI - Robotics data processing platform

Convert ROSbag files to analytics-ready formats like Parquet, CSV, and JSON.
"""

__version__ = "0.1.0"

from .processor import ROSbagProcessor
from .catalog import DataCatalog

__all__ = ["ROSbagProcessor", "DataCatalog"] 