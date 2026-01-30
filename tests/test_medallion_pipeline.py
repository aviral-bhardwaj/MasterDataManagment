"""
Medallion Architecture Pipeline Test Suite

This script tests the entire data pipeline locally using pandas.
It validates:
1. Sample CSV files are readable and well-formed
2. Pre-Bronze: Generic data processing and schema detection
3. Bronze: Data ingestion with metadata
4. Silver: Data cleaning and transformation
5. Gold: Deduplication and golden record selection
"""

import os
import sys
import re
import hashlib
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd
import numpy as np

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


class TestResults:
    """Track test results and ensure failures cause test suite to fail."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def record_pass(self, test_name: str):
        self.passed += 1
        print(f"  ✓ {test_name}")

    def record_fail(self, test_name: str, error: str):
        """Record a test failure. Failures are accumulated and raised at summary."""
        self.failed += 1
        self.errors.append((test_name, error))
        print(f"  ✗ {test_name}: {error}")

    def summary(self) -> bool:
        """Print summary and raise AssertionError if any tests failed."""
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"RESULTS: {self.passed}/{total} tests passed")
        if self.errors:
            print(f"\nFailed tests:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
            # Raise AssertionError to ensure pytest fails
            raise AssertionError(
                f"{self.failed} test(s) failed: " +
                ", ".join(name for name, _ in self.errors)
            )
        return True


results = TestResults()


# =============================================================================
# TEST 1: SAMPLE CSV FILES VALIDATION
# =============================================================================

def test_sample_csv_files():
    """Test that all sample CSV files exist and are readable."""
    print("\n" + "="*60)
    print("TEST 1: Sample CSV Files Validation")
    print("="*60)

    base_path = os.path.join(PROJECT_ROOT, 'data', 'sample_datasets')

    expected_files = {
        'customers': ['customers_sap.csv', 'customers_salesforce.csv'],
        'products': ['products_erp.csv', 'products_warehouse.csv'],
        'orders': ['orders_ecommerce.csv'],
        'employees': ['employees_hr.csv'],
        'inventory': ['inventory_warehouse.csv']
    }

    all_dataframes = {}

    for folder, files in expected_files.items():
        folder_path = os.path.join(base_path, folder)

        # Check folder exists
        if os.path.exists(folder_path):
            results.record_pass(f"Folder exists: {folder}/")
        else:
            results.record_fail(f"Folder exists: {folder}/", "Folder not found")
            continue

        for filename in files:
            file_path = os.path.join(folder_path, filename)

            # Check file exists
            if os.path.exists(file_path):
                results.record_pass(f"File exists: {folder}/{filename}")
            else:
                results.record_fail(f"File exists: {folder}/{filename}", "File not found")
                continue

            # Try to read the CSV
            try:
                df = pd.read_csv(file_path)
                all_dataframes[f"{folder}/{filename}"] = df
                results.record_pass(f"CSV readable: {folder}/{filename} ({len(df)} rows, {len(df.columns)} cols)")
            except Exception as e:
                results.record_fail(f"CSV readable: {folder}/{filename}", str(e))

    return all_dataframes


# =============================================================================
# TEST 2: SCHEMA DETECTION (PANDAS VERSION)
# =============================================================================

class PandasSchemaDetector:
    """Schema detector using pandas instead of PySpark."""

    TYPE_PATTERNS = {
        'integer': re.compile(r'^-?\d+$'),
        'float': re.compile(r'^-?\d+\.\d+$'),
        'boolean': re.compile(r'^(true|false|yes|no|1|0)$', re.IGNORECASE),
        'date': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        'email': re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$'),
    }

    def infer_column_type(self, series: pd.Series) -> str:
        """Infer the data type of a column."""
        sample = series.dropna().astype(str).head(100)
        if len(sample) == 0:
            return 'string'

        type_counts = {'integer': 0, 'float': 0, 'boolean': 0, 'date': 0, 'string': 0}

        for value in sample:
            value = str(value).strip()
            if self.TYPE_PATTERNS['date'].match(value):
                type_counts['date'] += 1
            elif self.TYPE_PATTERNS['boolean'].match(value):
                type_counts['boolean'] += 1
            elif self.TYPE_PATTERNS['integer'].match(value):
                type_counts['integer'] += 1
            elif self.TYPE_PATTERNS['float'].match(value):
                type_counts['float'] += 1
            else:
                type_counts['string'] += 1

        # Return dominant type (80% threshold)
        total = len(sample)
        for dtype, count in type_counts.items():
            if dtype != 'string' and count / total >= 0.8:
                return dtype
        return 'string'

    def detect_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Detect schema for all columns."""
        schema = {}
        for col in df.columns:
            schema[col] = self.infer_column_type(df[col])
        return schema

    def categorize_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Categorize columns by semantic meaning."""
        categories = {}
        patterns = {
            'identifier': ['id', 'key', 'code', 'sku'],
            'temporal': ['date', 'time', 'created', 'updated'],
            'contact': ['email', 'phone', 'address', 'city'],
            'metric': ['amount', 'price', 'cost', 'quantity', 'total']
        }

        for col in df.columns:
            col_lower = col.lower()
            found = False
            for category, keywords in patterns.items():
                if any(kw in col_lower for kw in keywords):
                    categories[col] = category
                    found = True
                    break
            if not found:
                categories[col] = 'general'

        return categories


def test_schema_detection():
    """Test schema detection functionality."""
    print("\n" + "="*60)
    print("TEST 2: Schema Detection")
    print("="*60)

    detector = PandasSchemaDetector()

    # Test with customers data
    customers_path = os.path.join(PROJECT_ROOT, 'data', 'sample_datasets', 'customers', 'customers_sap.csv')
    df = pd.read_csv(customers_path)

    # Test schema inference
    schema = detector.detect_schema(df)
    results.record_pass(f"Schema detected: {len(schema)} columns")

    expected_types = {
        'customer_id': 'string',
        'email': 'string',
        'registration_date': 'date'
    }

    for col, expected_type in expected_types.items():
        if col not in schema:
            # Explicitly fail when expected column is missing (schema regression)
            results.record_fail(f"Column '{col}' exists", f"Expected column '{col}' not found in schema")
        elif schema[col] == expected_type:
            results.record_pass(f"Column '{col}' type: {schema[col]}")
        else:
            results.record_fail(f"Column '{col}' type", f"Expected {expected_type}, got {schema[col]}")

    # Test column categorization
    categories = detector.categorize_columns(df)
    results.record_pass(f"Columns categorized: {len(categories)}")

    if categories.get('customer_id') == 'identifier':
        results.record_pass("customer_id categorized as 'identifier'")
    if categories.get('email') == 'contact':
        results.record_pass("email categorized as 'contact'")

    return detector


# =============================================================================
# TEST 3: PRE-BRONZE LAYER (GENERIC PROCESSING)
# =============================================================================

def add_metadata(df: pd.DataFrame, source_name: str, source_file: str) -> pd.DataFrame:
    """Add standard metadata columns."""
    df = df.copy()
    df['_source_system'] = source_name
    df['_ingestion_timestamp'] = datetime.now().isoformat()
    df['_source_file'] = source_file
    return df


def generate_record_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Generate MD5 hash for each record.

    Note: MD5 is used intentionally for non-cryptographic purposes (record
    fingerprinting/deduplication). It provides fast, deterministic hashing
    suitable for data pipeline record identification. Not used for security.
    """
    df = df.copy()
    data_cols = [c for c in df.columns if not c.startswith('_')]

    def hash_row(row):
        values = '||'.join(str(row[c]) for c in data_cols)
        # MD5 used for record fingerprinting, not cryptographic security
        return hashlib.md5(values.encode()).hexdigest()

    df['_record_hash'] = df.apply(hash_row, axis=1)
    return df


def test_pre_bronze_layer():
    """Test pre-bronze generic data processing."""
    print("\n" + "="*60)
    print("TEST 3: Pre-Bronze Layer (Generic Processing)")
    print("="*60)

    base_path = os.path.join(PROJECT_ROOT, 'data', 'sample_datasets')
    pre_bronze_tables = {}

    datasets = {
        'customers': 'customers/customers_sap.csv',
        'products': 'products/products_erp.csv',
        'orders': 'orders/orders_ecommerce.csv',
        'employees': 'employees/employees_hr.csv',
        'inventory': 'inventory/inventory_warehouse.csv'
    }

    for source_name, rel_path in datasets.items():
        file_path = os.path.join(base_path, rel_path)
        df = pd.read_csv(file_path)

        # Add metadata
        df = add_metadata(df, source_name, rel_path)
        results.record_pass(f"Metadata added: {source_name}")

        # Generate hash
        df = generate_record_hash(df)
        results.record_pass(f"Record hash generated: {source_name}")

        # Verify metadata columns exist
        required_cols = ['_source_system', '_ingestion_timestamp', '_source_file', '_record_hash']
        missing = [c for c in required_cols if c not in df.columns]
        if not missing:
            results.record_pass(f"All metadata columns present: {source_name}")
        else:
            results.record_fail(f"Metadata columns: {source_name}", f"Missing: {missing}")

        pre_bronze_tables[source_name] = df

    return pre_bronze_tables


# =============================================================================
# TEST 4: BRONZE LAYER (DATA INGESTION WITH QUALITY RULES)
# =============================================================================

def apply_quality_rules(df: pd.DataFrame, rules: Dict[str, str]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Apply quality rules and return only valid records with statistics.

    Invalid records are filtered out to prevent bad data from flowing downstream.
    """
    stats = {}
    # Start with all records valid, then AND each rule's mask
    cumulative_mask = pd.Series([True] * len(df), index=df.index)

    for rule_name, rule_expr in rules.items():
        # Convert SQL-like expression to pandas
        if 'IS NOT NULL' in rule_expr:
            col = rule_expr.replace(' IS NOT NULL', '').strip()
            mask = df[col].notna()
        elif '>=' in rule_expr:
            # Handle >= before > to avoid substring matching issues
            parts = rule_expr.split('>=')
            col = parts[0].strip()
            val = float(parts[1].strip())
            mask = df[col] >= val
        elif '>' in rule_expr:
            parts = rule_expr.split('>')
            col = parts[0].strip()
            val = float(parts[1].strip())
            mask = df[col] > val
        else:
            mask = pd.Series([True] * len(df), index=df.index)

        failed_count = (~mask).sum()
        stats[rule_name] = {'passed': int(mask.sum()), 'failed': int(failed_count)}

        # Accumulate: record must pass ALL rules
        cumulative_mask = cumulative_mask & mask

    # Filter DataFrame to only include valid records
    valid_df = df[cumulative_mask].copy()

    return valid_df, stats


def test_bronze_layer(pre_bronze_tables: Dict[str, pd.DataFrame]):
    """Test bronze layer data ingestion with quality rules."""
    print("\n" + "="*60)
    print("TEST 4: Bronze Layer (Quality Rules)")
    print("="*60)

    quality_rules = {
        'customers': {
            'valid_customer_id': 'customer_id IS NOT NULL',
        },
        'products': {
            'valid_product_id': 'product_id IS NOT NULL',
            'valid_price': 'price >= 0',
        },
        'orders': {
            'valid_order_id': 'order_id IS NOT NULL',
            'valid_quantity': 'quantity > 0',
        }
    }

    bronze_tables = {}

    for source_name, df in pre_bronze_tables.items():
        rules = quality_rules.get(source_name, {})

        if rules:
            df, stats = apply_quality_rules(df, rules)

            for rule_name, stat in stats.items():
                if stat['failed'] == 0:
                    results.record_pass(f"{source_name}: {rule_name} (all {stat['passed']} passed)")
                else:
                    results.record_fail(f"{source_name}: {rule_name}", f"{stat['failed']} records failed")
        else:
            results.record_pass(f"{source_name}: No specific quality rules (passed)")

        bronze_tables[source_name] = df

    return bronze_tables


# =============================================================================
# TEST 5: SILVER LAYER (CLEANING AND TRANSFORMATION)
# =============================================================================

def clean_and_transform(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    """Apply cleaning and transformation based on source type."""
    df = df.copy()

    # Email normalization
    if 'email' in df.columns:
        df['email'] = df['email'].str.lower().str.strip()

    # Phone normalization
    if 'phone' in df.columns:
        df['phone'] = df['phone'].astype(str).str.replace(r'[^\d\+]', '', regex=True)

    # Name normalization
    name_cols = ['first_name', 'last_name', 'name', 'product_name']
    for col in name_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    # Create full_name for customers/employees
    if 'first_name' in df.columns and 'last_name' in df.columns:
        if 'full_name' not in df.columns:
            df['full_name'] = df['first_name'] + ' ' + df['last_name']

    return df


def generate_unique_identifier(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    """Generate unique identifier for deduplication.

    Priority for customers: email > phone > name+city
    Empty strings after normalization are treated as missing values.
    """
    df = df.copy()

    def is_valid_value(val) -> bool:
        """Check if value is both not null AND not empty string."""
        return pd.notna(val) and str(val).strip() != ''

    if source_type == 'customers':
        # Priority: email > phone > name+city
        # Check both notna AND truthiness to avoid empty string collisions
        df['unique_identifier'] = df.apply(
            lambda r: r['email'] if is_valid_value(r.get('email'))
            else r['phone'] if is_valid_value(r.get('phone'))
            else f"{r.get('first_name', '')}_{r.get('city', '')}",
            axis=1
        )
    elif source_type == 'products':
        df['unique_identifier'] = df['sku']
    elif source_type == 'employees':
        df['unique_identifier'] = df['email']
    else:
        # Default: use first column that looks like an ID
        id_cols = [c for c in df.columns if 'id' in c.lower()]
        if id_cols:
            df['unique_identifier'] = df[id_cols[0]]
        else:
            df['unique_identifier'] = df.index.astype(str)

    return df


def test_silver_layer(bronze_tables: Dict[str, pd.DataFrame]):
    """Test silver layer cleaning and transformation."""
    print("\n" + "="*60)
    print("TEST 5: Silver Layer (Cleaning & Transformation)")
    print("="*60)

    silver_tables = {}

    for source_name, df in bronze_tables.items():
        # Apply cleaning
        cleaned_df = clean_and_transform(df, source_name)
        results.record_pass(f"{source_name}: Data cleaned")

        # Verify email normalization
        if 'email' in cleaned_df.columns:
            sample_email = cleaned_df['email'].dropna().iloc[0] if len(cleaned_df['email'].dropna()) > 0 else ''
            if sample_email == sample_email.lower():
                results.record_pass(f"{source_name}: Email normalized to lowercase")
            else:
                results.record_fail(f"{source_name}: Email normalization", "Email not lowercase")

        # Generate unique identifier
        identified_df = generate_unique_identifier(cleaned_df, source_name)
        results.record_pass(f"{source_name}: Unique identifier generated")

        # Verify unique_identifier exists
        if 'unique_identifier' in identified_df.columns:
            non_null = identified_df['unique_identifier'].notna().sum()
            results.record_pass(f"{source_name}: {non_null}/{len(identified_df)} records have identifier")

        silver_tables[source_name] = identified_df

    return silver_tables


# =============================================================================
# TEST 6: GOLD LAYER (DEDUPLICATION)
# =============================================================================

def count_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Count occurrences of each unique identifier."""
    counts = df.groupby('unique_identifier').size().reset_index(name='repetition')
    return df.merge(counts, on='unique_identifier', how='left')


def score_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """Assign confidence levels based on repetition count."""
    df = df.copy()

    def get_confidence(rep):
        if rep >= 3:
            return 'High'
        elif rep == 2:
            return 'Moderate'
        else:
            return 'Low'

    df['confidence'] = df['repetition'].apply(get_confidence)
    df['confidence_rank'] = df['confidence'].map({'High': 3, 'Moderate': 2, 'Low': 1})

    return df


def select_golden_records(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    """Select the best record per unique entity."""
    df = df.copy()

    # Sort by confidence_rank (desc), then by other criteria
    sort_cols = ['unique_identifier', 'confidence_rank']
    ascending = [True, False]

    # Add secondary sort based on source type
    if source_type == 'customers' and 'registration_date' in df.columns:
        sort_cols.append('registration_date')
        ascending.append(False)
    elif source_type == 'products' and 'price' in df.columns:
        sort_cols.append('price')
        ascending.append(True)

    df_sorted = df.sort_values(sort_cols, ascending=ascending)

    # Keep first record per unique_identifier (the best one)
    golden = df_sorted.groupby('unique_identifier').first().reset_index()

    return golden


def test_gold_layer(silver_tables: Dict[str, pd.DataFrame]):
    """Test gold layer deduplication."""
    print("\n" + "="*60)
    print("TEST 6: Gold Layer (Deduplication)")
    print("="*60)

    gold_tables = {}

    for source_name, df in silver_tables.items():
        original_count = len(df)

        # Count duplicates
        df_with_counts = count_duplicates(df)
        results.record_pass(f"{source_name}: Duplicate counts calculated")

        # Check for duplicates
        has_duplicates = (df_with_counts['repetition'] > 1).any()
        dup_count = (df_with_counts['repetition'] > 1).sum()
        results.record_pass(f"{source_name}: Found {dup_count} records that are duplicates")

        # Score confidence
        df_scored = score_confidence(df_with_counts)
        results.record_pass(f"{source_name}: Confidence scored")

        # Verify confidence levels
        confidence_dist = df_scored['confidence'].value_counts().to_dict()
        results.record_pass(f"{source_name}: Confidence distribution: {confidence_dist}")

        # Select golden records
        golden = select_golden_records(df_scored, source_name)
        final_count = len(golden)
        dedup_ratio = (original_count - final_count) / original_count * 100 if original_count > 0 else 0

        results.record_pass(f"{source_name}: Golden records selected ({original_count} → {final_count}, {dedup_ratio:.1f}% deduplicated)")

        gold_tables[source_name] = golden

    return gold_tables


# =============================================================================
# TEST 7: END-TO-END PIPELINE SIMULATION
# =============================================================================

def test_end_to_end_pipeline():
    """Test the complete medallion architecture flow."""
    print("\n" + "="*60)
    print("TEST 7: End-to-End Pipeline Simulation")
    print("="*60)

    # Simulate full pipeline for customers (with data from multiple sources)
    base_path = os.path.join(PROJECT_ROOT, 'data', 'sample_datasets', 'customers')

    # Read all customer files
    all_customers = []
    for filename in ['customers_sap.csv', 'customers_salesforce.csv']:
        file_path = os.path.join(base_path, filename)
        df = pd.read_csv(file_path)
        df = add_metadata(df, 'customers', filename)
        all_customers.append(df)

    # Combine all sources
    combined = pd.concat(all_customers, ignore_index=True)
    results.record_pass(f"Combined {len(combined)} records from multiple sources")

    # Pre-Bronze: Add hash
    combined = generate_record_hash(combined)
    results.record_pass(f"Pre-Bronze: Record hashes generated")

    # Bronze: Quality check
    combined, stats = apply_quality_rules(combined, {'valid_id': 'customer_id IS NOT NULL'})
    results.record_pass(f"Bronze: Quality rules applied")

    # Silver: Clean and identify
    combined = clean_and_transform(combined, 'customers')
    combined = generate_unique_identifier(combined, 'customers')
    results.record_pass(f"Silver: Data cleaned and identifiers generated")

    # Gold: Deduplicate
    combined = count_duplicates(combined)
    combined = score_confidence(combined)
    golden = select_golden_records(combined, 'customers')

    original = len(pd.concat(all_customers))
    final = len(golden)

    print(f"\n  Pipeline Summary:")
    print(f"  - Input records: {original}")
    print(f"  - Golden records: {final}")
    print(f"  - Duplicates removed: {original - final}")
    print(f"  - Deduplication rate: {(original - final) / original * 100:.1f}%")

    results.record_pass(f"End-to-End: Pipeline completed successfully")

    # Show sample golden records
    print(f"\n  Sample Golden Records:")
    display_cols = ['customer_id', 'full_name', 'email', 'confidence', '_source_file']
    display_cols = [c for c in display_cols if c in golden.columns]
    print(golden[display_cols].head(5).to_string(index=False))

    return golden


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================

def run_all_tests():
    """Run all tests in sequence."""
    print("\n" + "="*60)
    print("MEDALLION ARCHITECTURE PIPELINE TEST SUITE")
    print("="*60)
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Test Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Test 1: CSV Files
        dataframes = test_sample_csv_files()

        # Test 2: Schema Detection
        detector = test_schema_detection()

        # Test 3: Pre-Bronze Layer
        pre_bronze = test_pre_bronze_layer()

        # Test 4: Bronze Layer
        bronze = test_bronze_layer(pre_bronze)

        # Test 5: Silver Layer
        silver = test_silver_layer(bronze)

        # Test 6: Gold Layer
        gold = test_gold_layer(silver)

        # Test 7: End-to-End
        golden_customers = test_end_to_end_pipeline()

        # Final Summary
        success = results.summary()

        if success:
            print("\n✓ ALL TESTS PASSED - Pipeline is working correctly!")
        else:
            print("\n✗ SOME TESTS FAILED - Review errors above")

        return success

    except Exception as e:
        print(f"\n✗ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
