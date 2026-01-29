"""
Bronze Layer: Raw Data Ingestion Module.

This module ingests raw user data from multiple source systems (SAP, Salesforce,
Healthcare, Odoo, Zoho) stored in S3 as CSV files using Databricks Auto Loader.
"""

import dlt
from pyspark.sql.functions import col

# Data quality rules for bronze layer
QUALITY_RULES = {
    "valid_person_id": "person_id IS NOT NULL",
    "valid_age": "age > 0",
}

# S3 source path
S3_SOURCE_PATH = "s3://ankit-bucket-567/de_duplicate_project_root"


@dlt.table(
    name="dd_bronze",
    comment="Raw user data from all 5 source systems: SAP, Salesforce, Healthcare, Odoo, and Zoho",
)
@dlt.expect_all(QUALITY_RULES)
def dd_bronze():
    """
    Ingest raw user data from S3 using Databricks Auto Loader.

    Returns:
        DataFrame: Raw user data with source file metadata columns.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.format", "csv")
        .load(S3_SOURCE_PATH)
    )

    # Add source file metadata
    df = (
        df.withColumn("source_file_name", col("_metadata.file_name"))
          .withColumn("source_file_path", col("_metadata.file_path"))
    )

    return df
