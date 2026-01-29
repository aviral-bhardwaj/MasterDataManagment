"""
Custom Data Source Example

This module demonstrates how to add and process any custom dataset type
that isn't pre-configured. Use this as a template for adding new data
sources to the pre-bronze pipeline.

The generic processor can handle ANY CSV dataset - you just need to
define the configuration for your specific data type.
"""

import dlt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name

from pipelines.deduplication.pre_bronze import (
    GenericDataProcessor,
    DataSourceConfig,
    create_custom_source_config
)


# =============================================================================
# EXAMPLE 1: Quick Custom Source Using Factory Function
# =============================================================================

def create_suppliers_source(base_path: str) -> DataSourceConfig:
    """
    Example: Create a configuration for supplier data.

    This shows the minimal configuration needed to process a new dataset.
    """
    return create_custom_source_config(
        name='suppliers',
        source_path=f'{base_path}/suppliers/',
        primary_key_columns=['supplier_id'],
        dedup_columns=['company_name', 'contact_email'],
        quality_rules={
            'valid_supplier_id': 'supplier_id IS NOT NULL',
            'valid_company_name': 'company_name IS NOT NULL'
        }
    )


# =============================================================================
# EXAMPLE 2: Full Custom Configuration with Transformations
# =============================================================================

def normalize_company_name(df: DataFrame) -> DataFrame:
    """Custom transformation for company names."""
    from pyspark.sql.functions import upper, trim, regexp_replace

    if 'company_name' in df.columns:
        return df.withColumn(
            'company_name',
            upper(trim(regexp_replace(col('company_name'), r'\s+', ' ')))
        )
    return df


def create_vendors_source(base_path: str) -> DataSourceConfig:
    """
    Example: Create a fully customized vendor data configuration.

    This shows how to create a complete configuration with custom
    transformations, quality rules, and read options.
    """
    return DataSourceConfig(
        name='vendors',
        source_path=f'{base_path}/vendors/',
        file_format='csv',
        primary_key_columns=['vendor_id'],
        dedup_columns=['vendor_code', 'company_name', 'tax_id'],
        quality_rules={
            'valid_vendor_id': 'vendor_id IS NOT NULL',
            'valid_vendor_code': 'vendor_code IS NOT NULL',
            'valid_tax_id': "tax_id IS NULL OR tax_id RLIKE '^[0-9]{9}$'",
            'valid_payment_terms': 'payment_terms IN ("NET30", "NET60", "NET90", "COD")'
        },
        schema=None,  # Let the processor infer the schema
        read_options={
            'header': 'true',
            'inferSchema': 'true',
            'delimiter': ',',
            'encoding': 'UTF-8'
        },
        transformations=[
            normalize_company_name
        ]
    )


# =============================================================================
# EXAMPLE 3: Processing JSON Data
# =============================================================================

def create_api_logs_source(base_path: str) -> DataSourceConfig:
    """
    Example: Configuration for JSON data (API logs).

    This shows how to configure the processor for JSON format data.
    """
    return DataSourceConfig(
        name='api_logs',
        source_path=f'{base_path}/api_logs/',
        file_format='json',
        primary_key_columns=['request_id'],
        dedup_columns=['request_id', 'timestamp'],
        quality_rules={
            'valid_request_id': 'request_id IS NOT NULL',
            'valid_timestamp': 'timestamp IS NOT NULL',
            'valid_status_code': 'status_code >= 100 AND status_code < 600'
        },
        read_options={
            'multiLine': 'true'
        },
        transformations=[]
    )


# =============================================================================
# EXAMPLE 4: Using the Generic Processor Directly
# =============================================================================

def process_custom_dataset_example(spark: SparkSession, data_path: str):
    """
    Example: Process a completely new dataset using the generic processor.

    This function demonstrates the full workflow for adding and processing
    any custom dataset that you might receive.
    """
    # Step 1: Create the processor
    processor = GenericDataProcessor(spark)

    # Step 2: Define your custom configuration
    custom_config = DataSourceConfig(
        name='custom_dataset',
        source_path=data_path,
        file_format='csv',
        primary_key_columns=['id'],
        quality_rules={
            'has_id': 'id IS NOT NULL'
        }
    )

    # Step 3: Register the data source
    processor.register_data_source(custom_config)

    # Step 4: Read the data
    df = processor.read_source_data('custom_dataset', use_streaming=False)

    # Step 5: Apply automatic schema detection and typing
    df = processor.detect_and_apply_schema(df)

    # Step 6: Apply standard cleaning
    df = processor.apply_standard_cleaning(df)

    # Step 7: Enrich with metadata
    df = processor.enrich_with_metadata(df, 'custom_dataset')

    # Step 8: Generate record hash for deduplication tracking
    df = processor.generate_record_hash(df)

    return df


# =============================================================================
# EXAMPLE 5: DLT Table for Custom Source
# =============================================================================

# Uncomment and modify this to create a DLT table for your custom source

# DATA_PATH = "path/to/your/data"
#
# @dlt.table(
#     name="pre_bronze_custom",
#     comment="Pre-bronze layer: Custom dataset"
# )
# @dlt.expect_all({
#     "valid_id": "id IS NOT NULL"
# })
# def pre_bronze_custom():
#     """
#     Template for creating a DLT table for any custom dataset.
#     """
#     df = (
#         spark.read
#         .format("csv")
#         .option("header", "true")
#         .option("inferSchema", "true")
#         .load(DATA_PATH)
#     )
#
#     # Add your transformations here
#
#     # Add metadata
#     df = df \
#         .withColumn('_source_system', lit('custom')) \
#         .withColumn('_ingestion_timestamp', current_timestamp()) \
#         .withColumn('_source_file', input_file_name())
#
#     return df


# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================

"""
HOW TO ADD A NEW DATA SOURCE TO THE PRE-BRONZE PIPELINE:

1. QUICK METHOD (for simple datasets):
   ----------------------------------
   Use create_custom_source_config() to quickly create a configuration:

   ```python
   from pipelines.deduplication.pre_bronze import create_custom_source_config

   config = create_custom_source_config(
       name='my_dataset',
       source_path='/path/to/data/',
       primary_key_columns=['id']
   )
   ```

2. FULL CONFIGURATION (for complex datasets):
   ------------------------------------------
   Create a DataSourceConfig with all options:

   ```python
   from pipelines.deduplication.pre_bronze import DataSourceConfig

   config = DataSourceConfig(
       name='my_dataset',
       source_path='/path/to/data/',
       file_format='csv',
       primary_key_columns=['id'],
       dedup_columns=['email', 'name'],
       quality_rules={
           'valid_id': 'id IS NOT NULL',
           'valid_email': "email RLIKE '^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'"
       },
       transformations=[my_custom_transform_function]
   )
   ```

3. REGISTER AND PROCESS:
   ---------------------
   ```python
   processor = GenericDataProcessor(spark)
   processor.register_data_source(config)
   df = processor.read_source_data('my_dataset')
   ```

4. ADD TO data_source_configs.py:
   ------------------------------
   For permanent data sources, add a configuration function to
   data_source_configs.py and update get_all_source_configs().

5. CREATE DLT TABLE:
   -----------------
   Add a new @dlt.table decorated function to ingest_all_sources.py
   following the existing patterns.

The generic processor handles:
- Automatic schema detection
- Standard data cleaning
- Metadata enrichment
- Record hashing for deduplication
- Quality expectations via DLT
"""
