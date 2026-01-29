"""
Bronze Layer - Raw Data Ingestion.

This layer handles the ingestion of raw user data from multiple source systems.
"""

from .ingest_raw_data import dd_bronze

__all__ = ["dd_bronze"]
