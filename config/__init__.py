"""
Configuration module for User De-duplication Pipeline.
"""

from .settings import (
    S3_SOURCE_PATH,
    SOURCE_SYSTEMS,
    BRONZE_QUALITY_RULES,
    CONFIDENCE_THRESHOLDS,
    CONFIDENCE_RANKS,
    TABLE_NAMES,
)

__all__ = [
    "S3_SOURCE_PATH",
    "SOURCE_SYSTEMS",
    "BRONZE_QUALITY_RULES",
    "CONFIDENCE_THRESHOLDS",
    "CONFIDENCE_RANKS",
    "TABLE_NAMES",
]
