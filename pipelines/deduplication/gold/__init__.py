"""
Gold Layer - De-duplication and Golden Record Selection.

This layer handles:
- Counting duplicate occurrences
- Scoring confidence levels
- Selecting the best "golden record" for each user
"""

from .count_duplicates import dd_gold_duplicate_count
from .score_confidence import dd_gold_confidence_scored
from .select_golden_records import dd_gold_final

__all__ = [
    "dd_gold_duplicate_count",
    "dd_gold_confidence_scored",
    "dd_gold_final",
]
