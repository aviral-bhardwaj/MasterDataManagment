"""
Pre-Bronze Layer Module

This module provides a generic data processing layer that sits before the
traditional bronze layer. It handles automatic schema detection, data
validation, and preparation for any type of dataset.

Key Components:
- SchemaDetector: Automatic schema inference from CSV/JSON/Parquet files
- GenericDataProcessor: Unified interface for processing any dataset
- DataSourceConfig: Configuration class for defining data sources
- Data Source Configs: Pre-built configurations for common data types

Usage:
    from pipelines.deduplication.pre_bronze import (
        GenericDataProcessor,
        DataSourceConfig,
        SchemaDetector,
        get_all_source_configs,
        create_custom_source_config
    )

    # Create processor
    processor = GenericDataProcessor(spark)

    # Register data sources
    for config in get_all_source_configs('/path/to/data'):
        processor.register_data_source(config)

    # Create DLT tables
    for source in processor.get_registered_sources():
        processor.create_pre_bronze_table(source)
"""

from .schema_detector import SchemaDetector
from .generic_processor import (
    GenericDataProcessor,
    DataSourceConfig,
    create_unified_pre_bronze_processor,
    process_all_sources
)
from .data_source_configs import (
    get_all_source_configs,
    get_source_config_by_name,
    create_custom_source_config,
    get_customers_config,
    get_products_config,
    get_orders_config,
    get_employees_config,
    get_inventory_config
)

__all__ = [
    # Core classes
    'SchemaDetector',
    'GenericDataProcessor',
    'DataSourceConfig',
    # Factory functions
    'create_unified_pre_bronze_processor',
    'process_all_sources',
    # Configuration functions
    'get_all_source_configs',
    'get_source_config_by_name',
    'create_custom_source_config',
    # Individual config getters
    'get_customers_config',
    'get_products_config',
    'get_orders_config',
    'get_employees_config',
    'get_inventory_config'
]
