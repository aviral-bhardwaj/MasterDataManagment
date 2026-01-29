"""
User De-duplication Pipeline.

A data engineering solution for identifying and removing duplicate user records
from large datasets using the Medallion Architecture (Bronze-Silver-Gold).

Modules:
    config: Configuration settings and constants
    pipelines: Data processing pipelines

Usage:
    This package is designed to run on Databricks Delta Live Tables (DLT).
    Each module contains DLT table definitions that are automatically
    discovered and executed based on data dependencies.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from .config import (
    S3_SOURCE_PATH,
    SOURCE_SYSTEMS,
    BRONZE_QUALITY_RULES,
    CONFIDENCE_THRESHOLDS,
    TABLE_NAMES,
)

__all__ = [
    "S3_SOURCE_PATH",
    "SOURCE_SYSTEMS",
    "BRONZE_QUALITY_RULES",
    "CONFIDENCE_THRESHOLDS",
    "TABLE_NAMES",
]
