"""
Configuration settings for the User De-duplication Pipeline.
Includes pre-bronze layer configuration for generic data processing.
"""

# Data Source Configuration
S3_SOURCE_PATH = "s3://ankit-bucket-567/de_duplicate_project_root"

# Local sample data path (for development/testing)
LOCAL_SAMPLE_DATA_PATH = "data/sample_datasets"

# Pre-Bronze Configuration
PRE_BRONZE_CONFIG = {
    # Supported data source types
    "data_source_types": [
        "customers",
        "products",
        "orders",
        "employees",
        "inventory"
    ],
    # File formats supported
    "supported_formats": ["csv", "json", "parquet"],
    # Default read options for each format
    "default_read_options": {
        "csv": {
            "header": "true",
            "inferSchema": "true",
            "multiLine": "true",
            "escape": '"',
            "quote": '"'
        },
        "json": {
            "multiLine": "true"
        },
        "parquet": {}
    },
    # Schema inference sample size
    "schema_inference_sample_size": 100
}

# Source Systems
SOURCE_SYSTEMS = {
    "SAP": "SAP",
    "salesforce": "Salesforce",
    "healthcare": "Healthcare",
    "oddo": "Odoo",
    "zoho": "Zoho",
}

# Data Quality Rules
BRONZE_QUALITY_RULES = {
    "valid_person_id": "person_id IS NOT NULL",
    "valid_age": "age > 0",
}

# Confidence Thresholds
CONFIDENCE_THRESHOLDS = {
    "high": 3,      # repetition > 2
    "moderate": 2,  # repetition == 2
    "low": 1,       # repetition == 1
}

# Confidence Rank Mapping
CONFIDENCE_RANKS = {
    "High": 3,
    "Moderate": 2,
    "Low": 1,
}

# Table Names
TABLE_NAMES = {
    # Pre-Bronze tables (generic data ingestion)
    "pre_bronze_customers": "pre_bronze_customers",
    "pre_bronze_products": "pre_bronze_products",
    "pre_bronze_orders": "pre_bronze_orders",
    "pre_bronze_employees": "pre_bronze_employees",
    "pre_bronze_inventory": "pre_bronze_inventory",
    # Bronze layer
    "bronze": "dd_bronze",
    # Silver layer
    "silver_clean": "dd_silver_clean",
    "silver_identified": "dd_silver_identified",
    # Gold layer
    "gold_duplicate_count": "dd_gold_duplicate_count",
    "gold_confidence_scored": "dd_gold_confidence_scored",
    "gold_final": "dd_gold_final",
}

# Pre-Bronze Quality Rules (generic rules applied to all data sources)
PRE_BRONZE_QUALITY_RULES = {
    "non_empty_record": "_record_hash IS NOT NULL",
}
