"""
Data Source Configurations for Pre-Bronze Layer

This module defines configurations for various data sources that can be
processed through the generic pre-bronze pipeline. Each configuration
specifies how to read, validate, and transform a specific type of dataset.

To add a new data source:
1. Create a new DataSourceConfig instance
2. Define appropriate quality rules and deduplication columns
3. Add any custom transformations if needed
4. Register it in the get_all_source_configs() function
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, initcap,
    when, coalesce, to_date, concat, lit
)
from typing import List
from .generic_processor import DataSourceConfig


# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================

def normalize_email(df: DataFrame) -> DataFrame:
    """Normalize email addresses to lowercase and trimmed."""
    if 'email' in df.columns:
        return df.withColumn('email', lower(trim(col('email'))))
    return df


def normalize_phone(df: DataFrame) -> DataFrame:
    """Normalize phone numbers by removing non-numeric characters except +."""
    if 'phone' in df.columns:
        return df.withColumn(
            'phone',
            regexp_replace(col('phone'), r'[^\d\+]', '')
        )
    return df


def normalize_names(df: DataFrame) -> DataFrame:
    """Normalize name fields to proper case."""
    name_columns = ['first_name', 'last_name', 'name', 'product_name']
    for col_name in name_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, initcap(trim(col(col_name))))
    return df


def standardize_boolean(df: DataFrame) -> DataFrame:
    """Standardize boolean-like columns to true/false."""
    for col_name in df.columns:
        if 'is_' in col_name.lower() or 'active' in col_name.lower():
            df = df.withColumn(
                col_name,
                when(lower(col(col_name)).isin('true', 'yes', '1', 't', 'y'), True)
                .when(lower(col(col_name)).isin('false', 'no', '0', 'f', 'n'), False)
                .otherwise(col(col_name))
            )
    return df


def create_full_name(df: DataFrame) -> DataFrame:
    """Create full_name column from first_name and last_name if both exist."""
    if 'first_name' in df.columns and 'last_name' in df.columns:
        return df.withColumn(
            'full_name',
            concat(col('first_name'), lit(' '), col('last_name'))
        )
    return df


# =============================================================================
# DATA SOURCE CONFIGURATIONS
# =============================================================================

def get_customers_config(base_path: str) -> DataSourceConfig:
    """
    Configuration for customer data sources.

    Customer data typically comes from CRM systems like SAP, Salesforce, etc.
    Primary deduplication is based on email, with phone and name as fallbacks.
    """
    return DataSourceConfig(
        name='customers',
        source_path=f'{base_path}/customers/',
        file_format='csv',
        primary_key_columns=['customer_id'],
        dedup_columns=['email', 'phone', 'first_name', 'last_name'],
        quality_rules={
            'valid_customer_id': 'customer_id IS NOT NULL',
            'valid_email_format': "email IS NULL OR email RLIKE '^[\\\\w\\\\.\\\\-]+@[\\\\w\\\\.\\\\-]+\\\\.[a-zA-Z]{2,}$'",
            'valid_name': 'first_name IS NOT NULL OR last_name IS NOT NULL'
        },
        read_options={
            'header': 'true',
            'inferSchema': 'true'
        },
        transformations=[
            normalize_email,
            normalize_phone,
            normalize_names,
            create_full_name
        ]
    )


def get_products_config(base_path: str) -> DataSourceConfig:
    """
    Configuration for product data sources.

    Product data typically comes from ERP, inventory systems, etc.
    Primary key is product_id or SKU, with deduplication based on SKU and name.
    """
    return DataSourceConfig(
        name='products',
        source_path=f'{base_path}/products/',
        file_format='csv',
        primary_key_columns=['product_id'],
        dedup_columns=['sku', 'product_name'],
        quality_rules={
            'valid_product_id': 'product_id IS NOT NULL',
            'valid_sku': 'sku IS NOT NULL',
            'valid_price': 'price IS NULL OR price >= 0',
            'valid_stock': 'stock_quantity IS NULL OR stock_quantity >= 0'
        },
        read_options={
            'header': 'true',
            'inferSchema': 'true'
        },
        transformations=[
            normalize_names,
            standardize_boolean
        ]
    )


def get_orders_config(base_path: str) -> DataSourceConfig:
    """
    Configuration for order/transaction data sources.

    Order data comes from e-commerce platforms, POS systems, etc.
    Each order should have unique order_id with references to customers and products.
    """
    return DataSourceConfig(
        name='orders',
        source_path=f'{base_path}/orders/',
        file_format='csv',
        primary_key_columns=['order_id'],
        dedup_columns=['order_id', 'customer_id', 'order_date'],
        quality_rules={
            'valid_order_id': 'order_id IS NOT NULL',
            'valid_customer_ref': 'customer_id IS NOT NULL',
            'valid_product_ref': 'product_id IS NOT NULL',
            'valid_quantity': 'quantity IS NULL OR quantity > 0',
            'valid_amount': 'total_amount IS NULL OR total_amount >= 0'
        },
        read_options={
            'header': 'true',
            'inferSchema': 'true'
        },
        transformations=[]
    )


def get_employees_config(base_path: str) -> DataSourceConfig:
    """
    Configuration for employee/HR data sources.

    Employee data comes from HRIS systems like Workday, BambooHR, etc.
    Primary key is employee_id with deduplication based on email.
    """
    return DataSourceConfig(
        name='employees',
        source_path=f'{base_path}/employees/',
        file_format='csv',
        primary_key_columns=['employee_id'],
        dedup_columns=['email', 'first_name', 'last_name'],
        quality_rules={
            'valid_employee_id': 'employee_id IS NOT NULL',
            'valid_email': 'email IS NOT NULL',
            'valid_department': 'department IS NOT NULL',
            'valid_salary': 'salary IS NULL OR salary >= 0'
        },
        read_options={
            'header': 'true',
            'inferSchema': 'true'
        },
        transformations=[
            normalize_email,
            normalize_names,
            create_full_name
        ]
    )


def get_inventory_config(base_path: str) -> DataSourceConfig:
    """
    Configuration for inventory data sources.

    Inventory data comes from warehouse management systems.
    Tracks stock levels, reorder points, and supplier relationships.
    """
    return DataSourceConfig(
        name='inventory',
        source_path=f'{base_path}/inventory/',
        file_format='csv',
        primary_key_columns=['inventory_id'],
        dedup_columns=['product_id', 'warehouse_location'],
        quality_rules={
            'valid_inventory_id': 'inventory_id IS NOT NULL',
            'valid_product_ref': 'product_id IS NOT NULL',
            'valid_location': 'warehouse_location IS NOT NULL',
            'valid_quantity': 'quantity_on_hand IS NULL OR quantity_on_hand >= 0',
            'valid_reorder_level': 'reorder_level IS NULL OR reorder_level >= 0'
        },
        read_options={
            'header': 'true',
            'inferSchema': 'true'
        },
        transformations=[]
    )


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def get_all_source_configs(base_path: str) -> List[DataSourceConfig]:
    """
    Get all configured data source configurations.

    Args:
        base_path: Base path where all data source folders are located

    Returns:
        List of all DataSourceConfig instances
    """
    return [
        get_customers_config(base_path),
        get_products_config(base_path),
        get_orders_config(base_path),
        get_employees_config(base_path),
        get_inventory_config(base_path)
    ]


def get_source_config_by_name(
    name: str,
    base_path: str
) -> DataSourceConfig:
    """
    Get a specific data source configuration by name.

    Args:
        name: Name of the data source (customers, products, orders, etc.)
        base_path: Base path where data is located

    Returns:
        DataSourceConfig for the specified source

    Raises:
        ValueError: If the source name is not recognized
    """
    config_map = {
        'customers': get_customers_config,
        'products': get_products_config,
        'orders': get_orders_config,
        'employees': get_employees_config,
        'inventory': get_inventory_config
    }

    if name not in config_map:
        available = ', '.join(config_map.keys())
        raise ValueError(
            f"Unknown data source: '{name}'. Available sources: {available}"
        )

    return config_map[name](base_path)


def create_custom_source_config(
    name: str,
    source_path: str,
    primary_key_columns: List[str],
    **kwargs
) -> DataSourceConfig:
    """
    Create a custom data source configuration for new dataset types.

    This is useful when you need to add a new type of dataset that doesn't
    fit the predefined configurations.

    Args:
        name: Unique name for this data source
        source_path: Path to the data files
        primary_key_columns: List of primary key column names
        **kwargs: Additional configuration options

    Returns:
        New DataSourceConfig instance

    Example:
        >>> config = create_custom_source_config(
        ...     name='suppliers',
        ...     source_path='/data/suppliers/',
        ...     primary_key_columns=['supplier_id'],
        ...     dedup_columns=['company_name', 'contact_email'],
        ...     quality_rules={'valid_id': 'supplier_id IS NOT NULL'}
        ... )
    """
    return DataSourceConfig(
        name=name,
        source_path=source_path,
        primary_key_columns=primary_key_columns,
        **kwargs
    )
