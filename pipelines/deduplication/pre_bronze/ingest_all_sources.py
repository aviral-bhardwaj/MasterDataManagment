"""
Pre-Bronze Data Ingestion Pipeline

This module creates Delta Live Tables for all registered data sources
in the pre-bronze layer. It automatically processes any type of dataset
(customers, products, orders, employees, inventory, etc.) using the
generic processor.

Features:
- Automatic schema detection for any CSV dataset
- Standard data cleaning and normalization
- Metadata enrichment (source tracking, timestamps, record hashing)
- Quality expectations for data validation
- Support for streaming (Auto Loader) and batch ingestion

Usage in Databricks DLT Pipeline:
    This file should be referenced in your DLT pipeline configuration.
    The tables will be automatically created when the pipeline runs.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    regexp_extract, md5, concat_ws
)

# Import configurations
import sys
sys.path.append('/Workspace/MasterDataManagment')

from config.settings import (
    LOCAL_SAMPLE_DATA_PATH,
    S3_SOURCE_PATH,
    PRE_BRONZE_CONFIG
)
from pipelines.deduplication.pre_bronze import (
    GenericDataProcessor,
    get_all_source_configs,
    get_source_config_by_name
)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Use local sample data for development, S3 for production
# Change this based on your environment
DATA_BASE_PATH = LOCAL_SAMPLE_DATA_PATH  # or S3_SOURCE_PATH for production


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def add_standard_metadata(df, source_name: str):
    """Add standard metadata columns to DataFrame."""
    return df \
        .withColumn('_source_system', lit(source_name)) \
        .withColumn('_ingestion_timestamp', current_timestamp()) \
        .withColumn('_source_file', input_file_name()) \
        .withColumn(
            '_source_file_name',
            regexp_extract(input_file_name(), r'([^/]+)$', 1)
        )


def add_record_hash(df):
    """Generate MD5 hash for each record."""
    data_columns = [c for c in df.columns if not c.startswith('_')]
    return df.withColumn(
        '_record_hash',
        md5(concat_ws('||', *[col(c).cast('string') for c in data_columns]))
    )


# =============================================================================
# PRE-BRONZE TABLES - CUSTOMERS
# =============================================================================

@dlt.table(
    name="pre_bronze_customers",
    comment="Pre-bronze layer: Customer data from all source systems"
)
@dlt.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email_or_phone": "email IS NOT NULL OR phone IS NOT NULL"
})
def pre_bronze_customers():
    """
    Ingest customer data from multiple sources (SAP, Salesforce, etc.).
    Applies standard cleaning and normalization.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{DATA_BASE_PATH}/customers/")
    )

    # Apply transformations
    from pyspark.sql.functions import lower, trim, regexp_replace, initcap, concat

    df = df \
        .withColumn('email', lower(trim(col('email')))) \
        .withColumn('phone', regexp_replace(col('phone'), r'[^\d\+]', '')) \
        .withColumn('first_name', initcap(trim(col('first_name')))) \
        .withColumn('last_name', initcap(trim(col('last_name')))) \
        .withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))

    df = add_standard_metadata(df, 'customers')
    df = add_record_hash(df)

    return df


# =============================================================================
# PRE-BRONZE TABLES - PRODUCTS
# =============================================================================

@dlt.table(
    name="pre_bronze_products",
    comment="Pre-bronze layer: Product data from ERP and warehouse systems"
)
@dlt.expect_all({
    "valid_product_id": "product_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
    "valid_price": "price IS NULL OR price >= 0"
})
def pre_bronze_products():
    """
    Ingest product data from ERP and warehouse systems.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{DATA_BASE_PATH}/products/")
    )

    # Apply transformations
    from pyspark.sql.functions import initcap, trim, when, lower

    df = df \
        .withColumn('product_name', initcap(trim(col('product_name')))) \
        .withColumn('category', initcap(trim(col('category')))) \
        .withColumn(
            'is_active',
            when(lower(col('is_active')).isin('true', 'yes', '1'), True)
            .otherwise(False)
        )

    df = add_standard_metadata(df, 'products')
    df = add_record_hash(df)

    return df


# =============================================================================
# PRE-BRONZE TABLES - ORDERS
# =============================================================================

@dlt.table(
    name="pre_bronze_orders",
    comment="Pre-bronze layer: Order/transaction data from e-commerce systems"
)
@dlt.expect_all({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_ref": "customer_id IS NOT NULL",
    "valid_quantity": "quantity IS NULL OR quantity > 0"
})
def pre_bronze_orders():
    """
    Ingest order data from e-commerce and POS systems.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{DATA_BASE_PATH}/orders/")
    )

    # Apply transformations
    from pyspark.sql.functions import to_date

    df = df \
        .withColumn('order_date', to_date(col('order_date')))

    df = add_standard_metadata(df, 'orders')
    df = add_record_hash(df)

    return df


# =============================================================================
# PRE-BRONZE TABLES - EMPLOYEES
# =============================================================================

@dlt.table(
    name="pre_bronze_employees",
    comment="Pre-bronze layer: Employee/HR data from HRIS systems"
)
@dlt.expect_all({
    "valid_employee_id": "employee_id IS NOT NULL",
    "valid_email": "email IS NOT NULL",
    "valid_salary": "salary IS NULL OR salary >= 0"
})
def pre_bronze_employees():
    """
    Ingest employee data from HR systems.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{DATA_BASE_PATH}/employees/")
    )

    # Apply transformations
    from pyspark.sql.functions import lower, trim, initcap, concat, to_date

    df = df \
        .withColumn('email', lower(trim(col('email')))) \
        .withColumn('first_name', initcap(trim(col('first_name')))) \
        .withColumn('last_name', initcap(trim(col('last_name')))) \
        .withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name'))) \
        .withColumn('hire_date', to_date(col('hire_date')))

    df = add_standard_metadata(df, 'employees')
    df = add_record_hash(df)

    return df


# =============================================================================
# PRE-BRONZE TABLES - INVENTORY
# =============================================================================

@dlt.table(
    name="pre_bronze_inventory",
    comment="Pre-bronze layer: Inventory data from warehouse management systems"
)
@dlt.expect_all({
    "valid_inventory_id": "inventory_id IS NOT NULL",
    "valid_product_ref": "product_id IS NOT NULL",
    "valid_quantity": "quantity_on_hand IS NULL OR quantity_on_hand >= 0"
})
def pre_bronze_inventory():
    """
    Ingest inventory data from warehouse management systems.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{DATA_BASE_PATH}/inventory/")
    )

    # Apply transformations
    from pyspark.sql.functions import to_date

    df = df \
        .withColumn('last_restock_date', to_date(col('last_restock_date')))

    df = add_standard_metadata(df, 'inventory')
    df = add_record_hash(df)

    return df


# =============================================================================
# UNIFIED PRE-BRONZE VIEW
# =============================================================================

@dlt.table(
    name="pre_bronze_unified_metadata",
    comment="Unified view of all pre-bronze tables metadata for monitoring"
)
def pre_bronze_unified_metadata():
    """
    Create a unified metadata view showing ingestion statistics
    across all pre-bronze tables.
    """
    from pyspark.sql.functions import count, countDistinct, max as spark_max

    # Read from all pre-bronze tables
    customers = dlt.read("pre_bronze_customers")
    products = dlt.read("pre_bronze_products")
    orders = dlt.read("pre_bronze_orders")
    employees = dlt.read("pre_bronze_employees")
    inventory = dlt.read("pre_bronze_inventory")

    # Create metadata summary for each
    def create_summary(df, source_name):
        return df.select(
            lit(source_name).alias('source_type'),
            count('*').alias('total_records'),
            countDistinct('_record_hash').alias('unique_records'),
            countDistinct('_source_file').alias('source_files'),
            spark_max('_ingestion_timestamp').alias('last_ingestion')
        )

    # Union all summaries
    summary = (
        create_summary(customers, 'customers')
        .union(create_summary(products, 'products'))
        .union(create_summary(orders, 'orders'))
        .union(create_summary(employees, 'employees'))
        .union(create_summary(inventory, 'inventory'))
    )

    return summary
