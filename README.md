# User De-duplication Pipeline

A data engineering solution for identifying and removing duplicate user records from large datasets to ensure data accuracy, consistency, and reliability.

## Overview

This project applies rule-based and key-based matching logic to detect multiple entries representing the same user and consolidates them into a single, clean "golden record."

The solution is designed to work efficiently on high-volume data using **Apache Spark** and **Databricks Delta Live Tables (DLT)**, following the **Medallion Architecture** pattern.

## Project Structure

```
user_deduplication/
│
├── __init__.py                     # Package initialization
│
├── config/
│   ├── __init__.py
│   └── settings.py                 # Configuration constants
│
└── pipelines/
    ├── __init__.py
    └── deduplication/
        ├── __init__.py
        │
        ├── bronze/                 # Raw Data Ingestion
        │   ├── __init__.py
        │   └── ingest_raw_data.py
        │
        ├── silver/                 # Data Cleaning & Transformation
        │   ├── __init__.py
        │   ├── clean_and_transform.py
        │   └── generate_identifiers.py
        │
        └── gold/                   # De-duplication & Golden Record
            ├── __init__.py
            ├── count_duplicates.py
            ├── score_confidence.py
            └── select_golden_records.py
```

## Medallion Architecture

### Bronze Layer (Raw Data)
**File:** `bronze/ingest_raw_data.py`

- Ingests raw user data from 5 source systems (SAP, Salesforce, Healthcare, Odoo, Zoho)
- Uses Databricks Auto Loader for streaming CSV ingestion from S3
- Applies data quality rules (valid person_id, age > 0)
- Adds source file metadata

### Silver Layer (Cleaned Data)
**Files:** `silver/clean_and_transform.py`, `silver/generate_identifiers.py`

**Clean and Transform:**
- Identifies source system from file paths
- Normalizes email (lowercase, trimmed)
- Cleans phone numbers (removes non-numeric characters)
- Standardizes names (proper case)
- Casts age to integer

**Generate Identifiers:**
- Creates unique identifiers for duplicate detection
- Priority: Email > Phone > Name_Age combination

### Gold Layer (De-duplicated Data)
**Files:** `gold/count_duplicates.py`, `gold/score_confidence.py`, `gold/select_golden_records.py`

**Count Duplicates:**
- Groups records by unique identifier
- Counts occurrences to identify duplicates

**Score Confidence:**
- Assigns confidence levels based on repetition:
  - **High:** 3+ occurrences
  - **Moderate:** 2 occurrences
  - **Low:** 1 occurrence

**Select Golden Records:**
- Ranks records by confidence and age
- Selects the best record for each user

## Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     BRONZE      │     │     SILVER      │     │      GOLD       │
│                 │     │                 │     │                 │
│  ingest_raw_    │────▶│  clean_and_     │────▶│ count_          │
│  data.py        │     │  transform.py   │     │ duplicates.py   │
│                 │     │       │         │     │       │         │
│  (S3 → Raw)     │     │       ▼         │     │       ▼         │
│                 │     │  generate_      │────▶│ score_          │
│                 │     │  identifiers.py │     │ confidence.py   │
│                 │     │                 │     │       │         │
│                 │     │  (Normalized)   │     │       ▼         │
│                 │     │                 │     │ select_golden_  │
│                 │     │                 │     │ records.py      │
│                 │     │                 │     │                 │
│                 │     │                 │     │  (Deduplicated) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## DLT Tables

| Table Name                  | Layer  | Description                        |
|-----------------------------|--------|------------------------------------|
| `dd_bronze`                 | Bronze | Raw user data with metadata        |
| `dd_silver_clean`           | Silver | Cleaned and normalized data        |
| `dd_silver_identified`      | Silver | Data with unique identifiers       |
| `dd_gold_duplicate_count`   | Gold   | Duplicate occurrence counts        |
| `dd_gold_confidence_scored` | Gold   | Records with confidence levels     |
| `dd_gold_final`             | Gold   | Final de-duplicated golden records |

## Tech Stack

- **Apache Spark / PySpark** - Distributed data processing
- **Databricks Delta Live Tables (DLT)** - Declarative pipeline framework
- **Delta Lake** - ACID-compliant data storage
- **AWS S3** - Cloud data storage
- **Python** - Programming language

## Key Features

- Detects duplicates using configurable matching keys (email, phone, name + age)
- Handles exact and near-duplicate records
- Retains a single "golden record" per user based on confidence scoring
- Streaming-enabled for near real-time processing
- Scalable for high-volume data processing

## Configuration

Key settings can be modified in `config/settings.py`:

- `S3_SOURCE_PATH` - Source data location
- `BRONZE_QUALITY_RULES` - Data quality expectations
- `CONFIDENCE_THRESHOLDS` - Duplicate confidence levels
- `SOURCE_SYSTEMS` - Supported source system mappings

## Use Cases

- Data quality improvement
- Customer 360 or master data management (MDM)
- Analytics and reporting accuracy
- ETL pipeline validation
- Reducing storage and processing overhead

## Output

- **Cleaned user dataset** with unique records
- **One golden record per user** based on highest confidence and data completeness

---

*"Clean data is the oxygen of analytics, and this project acts like a precision filter, quietly removing noise so insights can breathe freely."*
