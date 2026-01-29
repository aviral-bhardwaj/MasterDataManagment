"""
Configuration settings for the User De-duplication Pipeline.
"""

# Data Source Configuration
S3_SOURCE_PATH = "s3://ankit-bucket-567/de_duplicate_project_root"

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
    "bronze": "dd_bronze",
    "silver_clean": "dd_silver_clean",
    "silver_identified": "dd_silver_identified",
    "gold_duplicate_count": "dd_gold_duplicate_count",
    "gold_confidence_scored": "dd_gold_confidence_scored",
    "gold_final": "dd_gold_final",
}
