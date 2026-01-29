"""
Silver Layer: Data Cleaning and Transformation Module.

This module cleans and standardizes raw bronze data by:
- Identifying source systems from file paths
- Normalizing email, phone, and name fields
- Standardizing data types
"""

import dlt
from pyspark.sql.functions import (
    col,
    when,
    lower,
    trim,
    initcap,
    regexp_replace,
)

# Source system mappings
SOURCE_SYSTEMS = {
    "SAP": "SAP",
    "salesforce": "Salesforce",
    "healthcare": "Healthcare",
    "oddo": "Odoo",
    "zoho": "Zoho",
}


@dlt.table(
    name="dd_silver_clean",
    comment="Cleaned and standardized user data with source system identification",
)
def dd_silver_clean():
    """
    Clean and transform bronze data.

    Transformations applied:
    - Identify source system from file path
    - Normalize email to lowercase
    - Remove non-numeric characters from phone (except +)
    - Apply proper case to names
    - Cast age to integer

    Returns:
        DataFrame: Cleaned user data with standardized fields.
    """
    df = spark.readStream.table("dd_bronze")

    # Identify source system from file path
    df = df.withColumn(
        "source_system",
        when(col("source_file_path").contains("SAP"), "SAP")
        .when(col("source_file_path").contains("salesforce"), "Salesforce")
        .when(col("source_file_path").contains("healthcare"), "Healthcare")
        .when(col("source_file_path").contains("oddo"), "Odoo")
        .when(col("source_file_path").contains("zoho"), "Zoho")
        .otherwise("Unknown"),
    )

    # Select relevant columns
    df = df.select(
        col("person_id"),
        col("name"),
        col("email"),
        col("phone"),
        col("age"),
        col("source_system"),
    )

    # Apply data normalization
    df = (
        df.withColumn("email", lower(trim(col("email"))))
          .withColumn("phone", regexp_replace(col("phone"), "[^0-9+]", ""))
          .withColumn("name", initcap(trim(col("name"))))
          .withColumn("age", col("age").cast("int"))
    )

    return df
