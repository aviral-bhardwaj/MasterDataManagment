"""
Generic Data Processor for Pre-Bronze Layer

This module provides a unified interface to process any type of dataset
from various sources (CSV, JSON, Parquet) and prepare them for the
bronze layer pipeline. It handles schema detection, data validation,
and metadata enrichment automatically.
"""

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    regexp_extract, trim, lower, when, coalesce,
    to_date, to_timestamp, md5, concat_ws
)
from pyspark.sql.types import StructType, StringType
from typing import Dict, List, Optional, Any, Callable
from .schema_detector import SchemaDetector


class DataSourceConfig:
    """
    Configuration class for a data source.
    Defines how to read and process a specific type of dataset.
    """

    def __init__(
        self,
        name: str,
        source_path: str,
        file_format: str = 'csv',
        primary_key_columns: Optional[List[str]] = None,
        dedup_columns: Optional[List[str]] = None,
        quality_rules: Optional[Dict[str, str]] = None,
        schema: Optional[StructType] = None,
        read_options: Optional[Dict[str, str]] = None,
        transformations: Optional[List[Callable[[DataFrame], DataFrame]]] = None
    ):
        """
        Initialize data source configuration.

        Args:
            name: Unique name for this data source (e.g., 'customers', 'products')
            source_path: Path to the data files (local or cloud storage)
            file_format: Format of source files ('csv', 'json', 'parquet')
            primary_key_columns: Columns that form the primary key
            dedup_columns: Columns to use for deduplication
            quality_rules: Dict of rule_name -> SQL expression for DLT expectations
            schema: Optional explicit schema (if None, will be inferred)
            read_options: Additional options for Spark reader
            transformations: List of transformation functions to apply
        """
        self.name = name
        self.source_path = source_path
        self.file_format = file_format.lower()
        self.primary_key_columns = primary_key_columns or []
        self.dedup_columns = dedup_columns or []
        self.quality_rules = quality_rules or {}
        self.schema = schema
        self.read_options = read_options or {}
        self.transformations = transformations or []

        # Set default read options based on format
        self._set_default_options()

    def _set_default_options(self):
        """Set sensible default options based on file format."""
        if self.file_format == 'csv':
            defaults = {
                'header': 'true',
                'inferSchema': 'true',
                'multiLine': 'true',
                'escape': '"',
                'quote': '"'
            }
        elif self.file_format == 'json':
            defaults = {
                'multiLine': 'true'
            }
        else:
            defaults = {}

        # Merge defaults with provided options (provided options take precedence)
        self.read_options = {**defaults, **self.read_options}


class GenericDataProcessor:
    """
    A generic data processor that can handle any type of dataset.
    Provides automatic schema detection, validation, and preparation
    for the bronze layer pipeline.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the generic data processor.

        Args:
            spark: Active SparkSession instance
        """
        self.spark = spark
        self.schema_detector = SchemaDetector(spark)
        self._registered_sources: Dict[str, DataSourceConfig] = {}

    def register_data_source(self, config: DataSourceConfig) -> None:
        """
        Register a new data source configuration.

        Args:
            config: DataSourceConfig instance
        """
        self._registered_sources[config.name] = config

    def get_registered_sources(self) -> List[str]:
        """Get list of registered data source names."""
        return list(self._registered_sources.keys())

    def read_source_data(
        self,
        source_name: str,
        use_streaming: bool = False
    ) -> DataFrame:
        """
        Read data from a registered source.

        Args:
            source_name: Name of the registered data source
            use_streaming: Whether to use streaming read (for Auto Loader)

        Returns:
            DataFrame with the source data
        """
        if source_name not in self._registered_sources:
            raise ValueError(f"Data source '{source_name}' not registered")

        config = self._registered_sources[source_name]

        if use_streaming:
            return self._read_streaming(config)
        else:
            return self._read_batch(config)

    def _read_batch(self, config: DataSourceConfig) -> DataFrame:
        """Read data in batch mode."""
        reader = self.spark.read.format(config.file_format)

        # Apply read options
        for key, value in config.read_options.items():
            reader = reader.option(key, value)

        # Apply schema if provided
        if config.schema:
            reader = reader.schema(config.schema)

        return reader.load(config.source_path)

    def _read_streaming(self, config: DataSourceConfig) -> DataFrame:
        """Read data in streaming mode using Auto Loader."""
        reader = self.spark.readStream.format('cloudFiles')

        # Set cloud files format
        reader = reader.option('cloudFiles.format', config.file_format)

        # Apply read options
        for key, value in config.read_options.items():
            reader = reader.option(key, value)

        # Apply schema if provided
        if config.schema:
            reader = reader.schema(config.schema)
        else:
            reader = reader.option('cloudFiles.inferColumnTypes', 'true')

        return reader.load(config.source_path)

    def enrich_with_metadata(
        self,
        df: DataFrame,
        source_name: str,
        include_file_info: bool = True
    ) -> DataFrame:
        """
        Enrich DataFrame with standard metadata columns.

        Args:
            df: Input DataFrame
            source_name: Name of the data source
            include_file_info: Whether to include file path/name metadata

        Returns:
            DataFrame with added metadata columns
        """
        enriched = df.withColumn('_source_system', lit(source_name)) \
                     .withColumn('_ingestion_timestamp', current_timestamp())

        if include_file_info:
            enriched = enriched.withColumn(
                '_source_file',
                input_file_name()
            ).withColumn(
                '_source_file_name',
                regexp_extract(input_file_name(), r'([^/]+)$', 1)
            )

        return enriched

    def generate_record_hash(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Generate a hash for each record based on specified columns.

        Args:
            df: Input DataFrame
            columns: Columns to include in hash (None = all columns)

        Returns:
            DataFrame with _record_hash column
        """
        if columns is None:
            # Exclude metadata columns
            columns = [c for c in df.columns if not c.startswith('_')]

        return df.withColumn(
            '_record_hash',
            md5(concat_ws('||', *[col(c).cast('string') for c in columns]))
        )

    def apply_standard_cleaning(self, df: DataFrame) -> DataFrame:
        """
        Apply standard data cleaning operations.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        cleaned = df

        for col_name in df.columns:
            if col_name.startswith('_'):
                continue

            dtype = df.schema[col_name].dataType

            # String cleaning
            if isinstance(dtype, StringType):
                cleaned = cleaned.withColumn(
                    col_name,
                    trim(col(col_name))
                )

        return cleaned

    def detect_and_apply_schema(
        self,
        df: DataFrame,
        sample_size: int = 100
    ) -> DataFrame:
        """
        Detect schema from data and cast columns appropriately.

        Args:
            df: Input DataFrame
            sample_size: Number of rows to sample for inference

        Returns:
            DataFrame with proper data types
        """
        schema, metadata = self.schema_detector.detect_schema_from_dataframe(
            df, sample_size
        )

        # Cast columns to inferred types
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name,
                    col(field.name).cast(field.dataType)
                )

        return df

    def create_pre_bronze_table(
        self,
        source_name: str,
        table_name: Optional[str] = None,
        use_streaming: bool = True,
        apply_cleaning: bool = True,
        include_metadata: bool = True
    ) -> Callable:
        """
        Create a DLT table definition for the pre-bronze layer.

        Args:
            source_name: Name of the registered data source
            table_name: Name for the DLT table (default: pre_bronze_{source_name})
            use_streaming: Whether to use streaming ingestion
            apply_cleaning: Whether to apply standard cleaning
            include_metadata: Whether to include metadata columns

        Returns:
            DLT table function
        """
        if source_name not in self._registered_sources:
            raise ValueError(f"Data source '{source_name}' not registered")

        config = self._registered_sources[source_name]
        final_table_name = table_name or f"pre_bronze_{source_name}"

        # Build quality expectations
        expectations = config.quality_rules.copy()

        # Add default expectation if primary key is defined
        if config.primary_key_columns:
            for pk_col in config.primary_key_columns:
                expectations[f'valid_{pk_col}'] = f'{pk_col} IS NOT NULL'

        @dlt.table(
            name=final_table_name,
            comment=f"Pre-bronze table for {source_name} data"
        )
        @dlt.expect_all(expectations) if expectations else lambda x: x
        def create_table():
            # Read data
            df = self.read_source_data(source_name, use_streaming)

            # Apply cleaning if requested
            if apply_cleaning:
                df = self.apply_standard_cleaning(df)

            # Add metadata if requested
            if include_metadata:
                df = self.enrich_with_metadata(df, source_name)

            # Apply custom transformations
            for transform in config.transformations:
                df = transform(df)

            # Generate record hash
            df = self.generate_record_hash(df)

            return df

        return create_table


def create_unified_pre_bronze_processor(
    spark: SparkSession,
    source_configs: List[DataSourceConfig]
) -> GenericDataProcessor:
    """
    Factory function to create a pre-configured processor with multiple sources.

    Args:
        spark: Active SparkSession
        source_configs: List of DataSourceConfig for different data sources

    Returns:
        Configured GenericDataProcessor instance
    """
    processor = GenericDataProcessor(spark)

    for config in source_configs:
        processor.register_data_source(config)

    return processor


# Example usage function for DLT pipeline
def process_all_sources(
    spark: SparkSession,
    source_configs: List[DataSourceConfig]
) -> Dict[str, Callable]:
    """
    Process all configured data sources and return DLT table functions.

    Args:
        spark: Active SparkSession
        source_configs: List of data source configurations

    Returns:
        Dict mapping table names to DLT table functions
    """
    processor = create_unified_pre_bronze_processor(spark, source_configs)
    tables = {}

    for source_name in processor.get_registered_sources():
        table_func = processor.create_pre_bronze_table(source_name)
        tables[f"pre_bronze_{source_name}"] = table_func

    return tables
