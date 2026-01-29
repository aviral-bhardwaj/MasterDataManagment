"""
Gold Layer: Golden Record Selection Module.

This module selects the best "golden record" for each user by ranking
records based on confidence level and age, then keeping only the top record.

Selection Criteria (in order of priority):
1. Highest confidence level (High > Moderate > Low)
2. Oldest age (tiebreaker - assumes older records have more complete data)
"""

import dlt
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

# Confidence level rankings for sorting
CONFIDENCE_RANKS = {
    "High": 3,
    "Moderate": 2,
    "Low": 1,
}


@dlt.table(
    name="dd_gold_final",
    comment="Final de-duplicated user records - one golden record per user",
)
def dd_gold_final():
    """
    Select the golden record for each user.

    Uses window functions to rank records within each user group and
    selects the top-ranked record based on:
    1. Confidence rank (descending)
    2. Age (descending - older records preferred)

    Returns:
        DataFrame: De-duplicated user records with one record per unique user.
    """
    df = spark.read.table("dd_gold_confidence_scored")

    # Add numeric confidence rank for sorting
    df = df.withColumn(
        "confidence_rank",
        when(col("confidence") == "High", 3)
        .when(col("confidence") == "Moderate", 2)
        .when(col("confidence") == "Low", 1),
    )

    # Define window for ranking records within each user group
    window_spec = (
        Window.partitionBy(col("name"))
              .orderBy(col("confidence_rank").desc(), col("age").desc())
    )

    # Assign row numbers within each partition
    df_ranked = df.withColumn("row_num", row_number().over(window_spec))

    # Keep only the top record (golden record) for each user
    df_final = df_ranked.filter(col("row_num") == 1)

    return df_final
