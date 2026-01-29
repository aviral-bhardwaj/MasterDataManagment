"""
De-duplication Pipeline following the Medallion Architecture.

Layers:
- Bronze: Raw data ingestion
- Silver: Data cleaning and transformation
- Gold: De-duplication and golden record selection
"""

from .bronze import ingest_raw_data
from .silver import clean_and_transform, generate_identifiers
from .gold import count_duplicates, score_confidence, select_golden_records

__all__ = [
    "ingest_raw_data",
    "clean_and_transform",
    "generate_identifiers",
    "count_duplicates",
    "score_confidence",
    "select_golden_records",
]
