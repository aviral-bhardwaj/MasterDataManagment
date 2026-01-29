"""
Gold Layer: Confidence Scoring Module.

This module assigns confidence levels to each user record based on
how many times their unique identifier appears in the dataset.

Confidence Levels:
- High: Appears 3+ times (strong duplicate evidence)
- Moderate: Appears exactly 2 times
- Low: Appears only once (unique record)
"""

import dlt
from pyspark.sql.functions import col, when


@dlt.table(
    name="dd_gold_confidence_scored",
    comment="User records with confidence scores based on duplicate frequency",
)
def dd_gold_confidence_scored():
    """
    Assign confidence levels to user records.

    Joins duplicate counts with user data and assigns confidence:
    - High: repetition > 2
    - Moderate: repetition == 2
    - Low: repetition == 1

    Returns:
        DataFrame: User records with confidence levels assigned.
    """
    # Read duplicate counts (streaming)
    df_counts = spark.readStream.table("dd_gold_duplicate_count")

    # Read user data (batch for join)
    df_users = spark.read.table("dd_silver_identified")

    # Join counts with user records
    df_joined = df_counts.join(
        df_users,
        on="unique_identifier",
        how="inner",
    )

    # Assign confidence levels based on repetition count
    df_scored = df_joined.withColumn(
        "confidence",
        when(col("repetition") > 2, "High")
        .when(col("repetition") == 2, "Moderate")
        .otherwise("Low"),
    )

    # Select final columns
    df_final = df_scored.select(
        col("person_id"),
        col("name"),
        col("email"),
        col("phone"),
        col("age"),
        col("source_system"),
        col("unique_identifier"),
        col("confidence"),
    )

    return df_final
