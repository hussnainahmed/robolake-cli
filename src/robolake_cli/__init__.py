"""
RoboLake CLI - Robotics data processing platform

Convert ROSbag files to analytics-ready formats for data science and analysis.
"""

__version__ = "0.1.0"
__author__ = "Ekai Labs"
__email__ = "team@ekai.ai"

from .processor import ROSbagProcessor
from .catalog import DataCatalog

__all__ = ["ROSbagProcessor", "DataCatalog"] 