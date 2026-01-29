"""
Gold Layer: Duplicate Counting Module.

This module counts the number of occurrences for each unique identifier
to determine how many times a user appears in the dataset.
"""

import dlt
from pyspark.sql.functions import count


@dlt.table(
    name="dd_gold_duplicate_count",
    comment="Count of occurrences per unique identifier for duplicate detection",
)
def dd_gold_duplicate_count():
    """
    Count duplicate occurrences per unique identifier.

    Groups records by unique_identifier and counts repetitions.
    Higher counts indicate more confident duplicate matches.

    Returns:
        DataFrame: Unique identifiers with their repetition counts.
    """
    df = spark.readStream.table("dd_silver_identified")

    df = df.groupBy("unique_identifier").agg(
        count("*").alias("repetition")
    )

    return df
