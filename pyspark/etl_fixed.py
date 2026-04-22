"""
ETL Job — FIXED version (PySpark equivalent of SchemaETL.scala).

Fixes:
1. mergeSchema=true: reads all Parquet files and unions their schemas
2. Canonical schema: adds missing columns with default values
3. Null filling: existing columns with nulls get defaults
4. Consistent column order: output always has the same columns in the same order
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

spark = SparkSession.builder.appName("ETL_Fixed").master("local[*]").getOrCreate()

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output", "fixed")

# Canonical schema: columns downstream consumers ALWAYS expect.
# If missing from input, we add them with these defaults.
CANONICAL_SCHEMA = {
    "name":        ("string", "unknown"),
    "age":         ("int",    0),
    "city":        ("string", "unknown"),
    "email":       ("string", "none"),
    "signup_date": ("string", "1970-01-01"),
}


def ensure_canonical_schema(df):
    """Add missing columns with defaults, fill nulls, enforce column order."""
    for col_name, (col_type, default) in CANONICAL_SCHEMA.items():
        if col_name not in df.columns:
            # Column doesn't exist in this file — add it with default
            df = df.withColumn(col_name, lit(default))
        else:
            # Column exists but may have nulls — fill nulls with default
            df = df.withColumn(col_name, coalesce(col(col_name), lit(default)))

    # Select only canonical columns in consistent order
    return df.select(sorted(CANONICAL_SCHEMA.keys()))


# FIX 1: mergeSchema=true reads files with different schemas and unions them
df = spark.read.option("mergeSchema", "true").parquet(
    os.path.join(BASE, "v1"),
    os.path.join(BASE, "v2"),
    os.path.join(BASE, "v3"),
)

print(f"Merged schema columns: {df.columns}")
print(f"Total records: {df.count()}")
df.show(truncate=False)

# FIX 2: Ensure canonical schema — add missing columns, fill nulls
normalized = ensure_canonical_schema(df)

print(f"\nNormalized schema columns: {normalized.columns}")
normalized.show(truncate=False)

# FIX 3: Process and write — downstream always sees the same columns
processed = normalized.filter(col("age") >= 18)
processed.write.mode("overwrite").parquet(OUTPUT)

print(f"\nWrote {processed.count()} records with canonical schema")
print(f"Output columns: {processed.columns}")

spark.stop()
