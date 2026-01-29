"""
Silver Layer: Unique Identifier Generation Module.

This module generates unique identifiers for each user record to enable
duplicate detection. The identifier is created using a priority-based approach:
1. Email (most reliable)
2. Phone number
3. Name + Age combination (fallback)
"""

import dlt
from pyspark.sql.functions import col, when, concat_ws


@dlt.table(
    name="dd_silver_identified",
    comment="User data with unique identifiers for duplicate detection",
)
def dd_silver_identified():
    """
    Generate unique identifiers for duplicate detection.

    Identifier Priority:
    1. Email - if available (most reliable for matching)
    2. Phone - if email is null
    3. Name_Age - concatenation as fallback

    Returns:
        DataFrame: User data with unique_identifier column added.
    """
    df = spark.readStream.table("dd_silver_clean")

    # Generate unique identifier with priority: email > phone > name_age
    df = df.withColumn(
        "unique_identifier",
        when(col("email").isNotNull(), col("email"))
        .when(col("phone").isNotNull(), col("phone"))
        .otherwise(concat_ws("_", col("name"), col("age"))),
    )

    return df
