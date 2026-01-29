"""
Schema Detection Utility for Generic Data Processing

This module provides utilities to dynamically detect and infer schemas
from various data sources (CSV, JSON, Parquet, etc.) enabling the
pre-bronze layer to process any type of dataset without hardcoding schemas.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, DateType, TimestampType, LongType
)
from pyspark.sql.functions import col, trim, lower
from typing import Dict, List, Optional, Tuple
import re


class SchemaDetector:
    """
    A generic schema detector that can infer data types and create
    appropriate schemas for any CSV dataset.
    """

    # Common patterns for data type inference
    TYPE_PATTERNS = {
        'integer': re.compile(r'^-?\d+$'),
        'double': re.compile(r'^-?\d+\.\d+$'),
        'boolean': re.compile(r'^(true|false|yes|no|1|0)$', re.IGNORECASE),
        'date': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        'timestamp': re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'),
        'email': re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$'),
        'phone': re.compile(r'^[\+]?[\d\s\-\(\)]+$'),
        'currency': re.compile(r'^[\$€£¥]?\d+\.?\d*$'),
    }

    # Mapping of inferred types to Spark types
    SPARK_TYPE_MAPPING = {
        'integer': IntegerType(),
        'long': LongType(),
        'double': DoubleType(),
        'boolean': BooleanType(),
        'date': DateType(),
        'timestamp': TimestampType(),
        'string': StringType(),
    }

    def __init__(self, spark: SparkSession):
        """
        Initialize the schema detector with a Spark session.

        Args:
            spark: Active SparkSession instance
        """
        self.spark = spark

    def infer_column_type(self, sample_values: List[str]) -> str:
        """
        Infer the data type of a column based on sample values.

        Args:
            sample_values: List of sample string values from the column

        Returns:
            String representing the inferred data type
        """
        if not sample_values:
            return 'string'

        # Filter out None and empty values
        valid_values = [v for v in sample_values if v and str(v).strip()]

        if not valid_values:
            return 'string'

        # Check patterns in order of specificity
        type_matches = {
            'integer': 0,
            'double': 0,
            'boolean': 0,
            'date': 0,
            'timestamp': 0,
            'string': 0
        }

        for value in valid_values:
            value_str = str(value).strip()

            if self.TYPE_PATTERNS['timestamp'].match(value_str):
                type_matches['timestamp'] += 1
            elif self.TYPE_PATTERNS['date'].match(value_str):
                type_matches['date'] += 1
            elif self.TYPE_PATTERNS['boolean'].match(value_str):
                type_matches['boolean'] += 1
            elif self.TYPE_PATTERNS['integer'].match(value_str):
                # Check if it might be a large number (use long instead)
                try:
                    int_val = int(value_str)
                    if abs(int_val) > 2147483647:
                        type_matches['double'] += 1
                    else:
                        type_matches['integer'] += 1
                except ValueError:
                    type_matches['string'] += 1
            elif self.TYPE_PATTERNS['double'].match(value_str):
                type_matches['double'] += 1
            else:
                type_matches['string'] += 1

        # Determine the dominant type (need at least 80% match for non-string)
        total = len(valid_values)
        threshold = 0.8

        for dtype, count in type_matches.items():
            if dtype != 'string' and count / total >= threshold:
                return dtype

        return 'string'

    def detect_schema_from_dataframe(
        self,
        df: DataFrame,
        sample_size: int = 100
    ) -> Tuple[StructType, Dict[str, str]]:
        """
        Detect schema from an existing DataFrame by analyzing sample data.

        Args:
            df: Input DataFrame to analyze
            sample_size: Number of rows to sample for type inference

        Returns:
            Tuple of (StructType schema, Dict of column metadata)
        """
        # Get sample data
        sample_rows = df.limit(sample_size).collect()

        if not sample_rows:
            # Return original schema if no data
            return df.schema, {}

        column_types = {}
        column_metadata = {}

        for col_name in df.columns:
            # Extract values for this column
            values = [row[col_name] for row in sample_rows]

            # Infer type
            inferred_type = self.infer_column_type(values)
            column_types[col_name] = inferred_type

            # Build metadata
            non_null_count = sum(1 for v in values if v is not None)
            column_metadata[col_name] = {
                'inferred_type': inferred_type,
                'null_percentage': (len(values) - non_null_count) / len(values) * 100,
                'sample_values': values[:3]
            }

        # Build schema
        fields = [
            StructField(col_name, self.SPARK_TYPE_MAPPING.get(dtype, StringType()), True)
            for col_name, dtype in column_types.items()
        ]

        return StructType(fields), column_metadata

    def detect_schema_from_csv(
        self,
        file_path: str,
        delimiter: str = ',',
        header: bool = True,
        sample_size: int = 100
    ) -> Tuple[StructType, Dict[str, str]]:
        """
        Detect schema directly from a CSV file.

        Args:
            file_path: Path to the CSV file
            delimiter: CSV delimiter character
            header: Whether the CSV has a header row
            sample_size: Number of rows to sample

        Returns:
            Tuple of (StructType schema, Dict of column metadata)
        """
        # First read with all strings to analyze
        df = self.spark.read.csv(
            file_path,
            header=header,
            sep=delimiter,
            inferSchema=False  # Read as strings first
        )

        return self.detect_schema_from_dataframe(df, sample_size)

    def get_column_categories(self, df: DataFrame) -> Dict[str, str]:
        """
        Categorize columns by their semantic meaning (identifier, date, metric, etc.)

        Args:
            df: Input DataFrame

        Returns:
            Dict mapping column names to their categories
        """
        categories = {}

        identifier_patterns = ['id', 'key', 'code', 'sku', 'number']
        date_patterns = ['date', 'time', 'created', 'updated', 'modified']
        name_patterns = ['name', 'title', 'description', 'label']
        contact_patterns = ['email', 'phone', 'address', 'city', 'country', 'zip']
        metric_patterns = ['amount', 'price', 'cost', 'quantity', 'count', 'total', 'sum']

        for col_name in df.columns:
            col_lower = col_name.lower()

            if any(p in col_lower for p in identifier_patterns):
                categories[col_name] = 'identifier'
            elif any(p in col_lower for p in date_patterns):
                categories[col_name] = 'temporal'
            elif any(p in col_lower for p in name_patterns):
                categories[col_name] = 'descriptive'
            elif any(p in col_lower for p in contact_patterns):
                categories[col_name] = 'contact'
            elif any(p in col_lower for p in metric_patterns):
                categories[col_name] = 'metric'
            else:
                categories[col_name] = 'general'

        return categories

    def suggest_primary_key(self, df: DataFrame) -> Optional[str]:
        """
        Suggest a potential primary key column based on column analysis.

        Args:
            df: Input DataFrame

        Returns:
            Column name that could serve as primary key, or None
        """
        categories = self.get_column_categories(df)

        # Look for identifier columns
        identifier_cols = [
            col for col, cat in categories.items()
            if cat == 'identifier'
        ]

        for col_name in identifier_cols:
            # Check uniqueness
            total_count = df.count()
            distinct_count = df.select(col_name).distinct().count()

            if total_count == distinct_count:
                return col_name

        return identifier_cols[0] if identifier_cols else None

    def suggest_dedup_columns(self, df: DataFrame) -> List[str]:
        """
        Suggest columns that could be used for deduplication.

        Args:
            df: Input DataFrame

        Returns:
            List of column names suitable for deduplication
        """
        categories = self.get_column_categories(df)

        # Priority: contact info > identifiers > descriptive
        dedup_cols = []

        # Email is usually the best dedup key
        contact_cols = [col for col, cat in categories.items() if cat == 'contact']
        for col in contact_cols:
            if 'email' in col.lower():
                dedup_cols.insert(0, col)
            else:
                dedup_cols.append(col)

        # Add identifier columns
        identifier_cols = [col for col, cat in categories.items() if cat == 'identifier']
        dedup_cols.extend(identifier_cols)

        # Add name columns
        name_cols = [col for col, cat in categories.items() if cat == 'descriptive']
        dedup_cols.extend([c for c in name_cols if 'name' in c.lower()])

        return dedup_cols[:5]  # Return top 5 suggestions
